"""The entity scan every table is built on: scroll pages of DataHub search, or fetch URNs.

A scan has a *plan* resolved once in ``initial_state`` from the pushed-down filters:

* ``=`` / ``IN`` on the table's id column (``id``, ``dataset_id``, ...) becomes a URN list,
  fetched ``page_size`` at a time with ``entities(urns: ...)``; no search at all.
* ``=`` / ``IN`` on other mapped columns (platform, environment, domain, container,
  owner, entity type) becomes ``scrollAcrossEntities`` ``types`` / ``orFilters``.
* Everything else is a full ``scrollAcrossEntities`` over the table's entity types.

The plan and the scroll cursor (``nextScrollId``) live in table-function state, so a long
scan resumes on any worker process over HTTP. The SDK re-applies every predicate to the
rows afterwards, so pushdown only ever narrows what is fetched.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.table_function import OutputCollector, ProcessParams, TableFunctionGenerator
from vgi_rpc import ArrowSerializableDataclass

from .client import Endpoint, endpoint, graphql
from .common import as_urn, eq_values, rows_to_batch

MAX_EMPTY_PAGES = 50
SEARCH_FLAGS = {"skipHighlighting": True, "skipAggregates": True}

# GraphQL selections shared by many scans.
OWNERSHIP = (
    "ownership { owners { owner { __typename "
    "... on CorpUser { urn username properties { displayName email } } "
    "... on CorpGroup { urn name properties { displayName email } } } "
    "type ownershipType { urn info { name } } } }"
)
TAGS = "tags { tags { tag { urn name properties { name } } } }"
TERMS = "glossaryTerms { terms { term { urn name properties { name } } } }"
DOMAIN = "domain { domain { urn properties { name } } }"
# Enough of any entity to name it (lineage endpoints, search hits).
ENTITY_NAME = (
    "... on Dataset { name platform { name } dataPlatformInstance { instanceId } properties { name } } "
    "... on Dashboard { platform { name } properties { name } } "
    "... on Chart { platform { name } properties { name } } "
    "... on DataFlow { platform { name } properties { name } } "
    "... on DataJob { jobPlatform: platform { name } properties { name } } "
    "... on Container { platform { name } properties { name } } "
    "... on MLModel { platform { name } name } "
    "... on MLFeatureTable { platform { name } name } "
    "... on Notebook { platform { name } } "
    "... on CorpUser { username } "
    "... on CorpGroup { name } "
    "... on Domain { properties { name } } "
    "... on DataProduct { properties { name } } "
    "... on GlossaryTerm { name } "
    "... on Tag { name }"
)


@dataclass(kw_only=True)
class ScanCursor(ArrowSerializableDataclass):
    """The resolved plan (JSON) and the position in it."""

    plan_json: str = "{}"
    scroll_id: str = ""
    offset: int = 0
    done: bool = False


@dataclass(frozen=True, slots=True)
class FilterMap:
    """How ``=`` / ``IN`` on one column becomes a search ``FacetFilterInput``."""

    field: str
    to_value: Callable[[Any], str] = str


def platform_filter() -> FilterMap:
    """``platform = 'snowflake'`` -> ``platform`` facet ``urn:li:dataPlatform:snowflake``."""
    return FilterMap("platform", lambda v: as_urn(str(v), "urn:li:dataPlatform:"))


def environment_filter() -> FilterMap:
    """``environment = 'PROD'`` -> ``origin`` facet ``PROD``."""
    return FilterMap("origin", lambda v: str(v).upper())


def owner_names(ownership: Any) -> list[str]:
    """Owner URNs of an ``ownership`` selection."""
    out = []
    for o in (ownership or {}).get("owners") or []:
        urn = (o.get("owner") or {}).get("urn") if isinstance(o, dict) else None
        if urn:
            out.append(str(urn))
    return out


def tag_names(tags: Any) -> list[str]:
    """Tag names of a ``GlobalTags`` selection."""
    out = []
    for t in (tags or {}).get("tags") or []:
        tag = t.get("tag") if isinstance(t, dict) else None
        if isinstance(tag, dict):
            name = (tag.get("properties") or {}).get("name") or tag.get("name")
            if name:
                out.append(str(name))
    return out


def term_names(terms: Any) -> list[str]:
    """Glossary term names of a ``GlossaryTerms`` selection."""
    out = []
    for t in (terms or {}).get("terms") or []:
        term = t.get("term") if isinstance(t, dict) else None
        if isinstance(term, dict):
            name = (term.get("properties") or {}).get("name") or term.get("name")
            if name:
                out.append(str(name))
    return out


def domain_of(entity: dict[str, Any]) -> tuple[str | None, str | None]:
    """``(domain_id, domain_name)`` of an entity's ``domain`` selection."""
    dom = (entity.get("domain") or {}).get("domain") or {}
    return dom.get("urn"), (dom.get("properties") or {}).get("name")


def entity_platform(entity: dict[str, Any]) -> str | None:
    """Platform name of an entity (DataJob's nullable ``platform`` is selected as ``jobPlatform``)."""
    plat = entity.get("platform") or entity.get("jobPlatform") or {}
    name = plat.get("name") if isinstance(plat, dict) else None
    return str(name).removeprefix("urn:li:dataPlatform:") if name else None


def description_of(entity: dict[str, Any]) -> str | None:
    """Return the UI-edited description if any, else the ingested one."""
    edited = (entity.get("editableProperties") or {}).get("description")
    return edited or (entity.get("properties") or {}).get("description")


class EntityScan[A](TableFunctionGenerator[A, ScanCursor]):
    """Scroll DataHub search (or fetch pushed-down URNs) and turn entities into rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema]
    #: GraphQL operation-name stem, e.g. ``Datasets`` -> ``vgiDatasetsScroll`` / ``vgiDatasetsGet``.
    OPERATION: ClassVar[str]
    #: Search ``types`` scanned.
    ENTITY_TYPES: ClassVar[tuple[str, ...]]
    #: Inline-fragment selection applied to each entity.
    SELECTION: ClassVar[str]
    #: Column whose ``=`` / ``IN`` values are URNs to fetch directly (None: no URN mode).
    ID_COLUMN: ClassVar[str | None] = "id"
    #: Column whose ``=`` / ``IN`` values narrow ``ENTITY_TYPES`` (lower-case EntityType names).
    TYPE_COLUMN: ClassVar[str | None] = None
    #: Columns pushed into ``orFilters``.
    FILTERS: ClassVar[dict[str, FilterMap]] = {}
    #: Extra GraphQL variable declarations used by ``SELECTION`` (e.g. ``$start: Long``).
    EXTRA_VARS: ClassVar[str] = ""
    #: Extra selection on each search hit (e.g. ``matchedFields { name }``), kept as ``_<field>``.
    HIT_SELECTION: ClassVar[str] = ""

    @classmethod
    def extra_variables(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Values for ``EXTRA_VARS`` (resolved once, from pushdown)."""
        return {}

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Rows for one entity."""
        raise NotImplementedError

    @classmethod
    def search_query(cls, params: ProcessParams[Any]) -> str:
        """Return the search text (``*`` = everything)."""
        return "*"

    @classmethod
    def extra_filters(cls, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """Facet filters that do not come from column pushdown (e.g. named arguments)."""
        return []

    # ----------------------------------------------------------------- plan

    @classmethod
    def plan(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Resolve pushdown into ``{mode, urns | types + orFilters, variables}``."""
        filters = params.current_pushdown_filters
        plan: dict[str, Any] = {"vars": cls.extra_variables(params)}
        types = list(cls.ENTITY_TYPES)
        if cls.TYPE_COLUMN:
            wanted = eq_values(filters, cls.TYPE_COLUMN)
            if wanted is not None:
                types = [t for t in types if t.lower() in {str(w).lower() for w in wanted}]
        if not types:
            return {**plan, "mode": "empty"}
        if cls.ID_COLUMN:
            urns = eq_values(filters, cls.ID_COLUMN)
            if urns is not None:
                return {**plan, "mode": "urns", "urns": [str(u) for u in urns]}
        facets = list(cls.extra_filters(params))
        for column_name, fmap in cls.FILTERS.items():
            values = eq_values(filters, column_name)
            if values is None:
                continue
            if not values:
                return {**plan, "mode": "empty"}
            facets.append({"field": fmap.field, "values": [fmap.to_value(v) for v in values], "condition": "EQUAL"})
        return {**plan, "mode": "scroll", "types": types, "query": cls.search_query(params), "facets": facets}

    # ----------------------------------------------------------------- GraphQL documents

    @classmethod
    def scroll_document(cls) -> str:
        """Build the ``scrollAcrossEntities`` query document."""
        return (
            f"query vgi{cls.OPERATION}Scroll($input: ScrollAcrossEntitiesInput!{cls.EXTRA_VARS}) {{ "
            "scrollAcrossEntities(input: $input) { nextScrollId count total "
            f"searchResults {{ {cls.HIT_SELECTION} entity {{ urn type {cls.SELECTION} }} }} }} }}"
        )

    @classmethod
    def get_document(cls) -> str:
        """Build the ``entities(urns:)`` query document."""
        return (
            f"query vgi{cls.OPERATION}Get($urns: [String!]!{cls.EXTRA_VARS}) {{ "
            f"entities(urns: $urns) {{ urn type {cls.SELECTION} }} }}"
        )

    @classmethod
    def fetch_page(cls, ep: Endpoint, plan: dict[str, Any], state: ScanCursor) -> list[dict[str, Any]]:
        """Fetch the next page of entities and advance ``state``."""
        if plan["mode"] == "urns":
            urns = plan["urns"][state.offset : state.offset + ep.page_size]
            state.offset += ep.page_size
            state.done = state.offset >= len(plan["urns"])
            if not urns:
                return []
            data = graphql(ep, f"vgi{cls.OPERATION}Get", cls.get_document(), {"urns": urns, **plan["vars"]})
            wanted = set(cls.ENTITY_TYPES)
            return [e for e in data.get("entities") or [] if isinstance(e, dict) and e.get("type") in wanted]
        search_input: dict[str, Any] = {
            "types": plan["types"],
            "query": plan["query"],
            "count": ep.page_size,
            "searchFlags": SEARCH_FLAGS,
        }
        if state.scroll_id:
            search_input["scrollId"] = state.scroll_id
        if plan["facets"]:
            search_input["orFilters"] = [{"and": plan["facets"]}]
        data = graphql(ep, f"vgi{cls.OPERATION}Scroll", cls.scroll_document(), {"input": search_input, **plan["vars"]})
        result = data.get("scrollAcrossEntities") or {}
        hits = []
        for h in result.get("searchResults") or []:
            entity = h.get("entity") if isinstance(h, dict) else None
            if isinstance(entity, dict):
                hits.append({**entity, **{f"_{k}": v for k, v in h.items() if k != "entity"}})
        next_id = result.get("nextScrollId")
        state.scroll_id = str(next_id) if next_id else ""
        state.done = not next_id or not hits
        return hits

    # ----------------------------------------------------------------- table-function protocol

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> ScanCursor:
        """Resolve the plan from the pushed-down filters."""
        plan = cls.plan(params)
        return ScanCursor(plan_json=json.dumps(plan, separators=(",", ":")), done=plan["mode"] == "empty")

    @classmethod
    def process(cls, params: ProcessParams[Any], state: ScanCursor, out: OutputCollector) -> None:
        """Fetch pages until one yields rows (or the scan ends) and emit them."""
        plan = json.loads(state.plan_json)
        ep = endpoint(params.secrets)
        for _ in range(MAX_EMPTY_PAGES):
            if state.done:
                out.finish()
                return
            rows: list[dict[str, Any]] = []
            for entity in cls.fetch_page(ep, plan, state):
                rows.extend(cls.build_rows(ep, entity, plan))
            if rows:
                out.emit(rows_to_batch(rows, params.output_schema))
                return
        out.emit(rows_to_batch([], params.output_schema))
