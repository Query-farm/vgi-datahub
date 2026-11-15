"""Lineage and search: the ``lineage_edges`` table and the ``lineage`` / ``search`` functions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from .assets import DATASET_IDENTITY, catalog_tool_column, dataset_identity, scan_tags
from .client import SECRET_REQUIREMENT, Endpoint, endpoint, graphql
from .common import (
    CATALOG_TOOL,
    STR_LIST,
    TSTZ,
    NoArgs,
    as_urn,
    dataset_name_from_urn,
    entity_type_name,
    eq_values,
    get,
    rows_to_batch,
    simple_field_path,
    ts,
)
from .governance import entity_display_name
from .meta import column
from .scan import (
    DOMAIN,
    ENTITY_NAME,
    OWNERSHIP,
    SEARCH_FLAGS,
    TAGS,
    TERMS,
    EntityScan,
    FilterMap,
    ScanCursor,
    description_of,
    domain_of,
    entity_platform,
    owner_names,
    platform_filter,
    tag_names,
    term_names,
)


def entity_summary(entity: dict[str, Any] | None) -> dict[str, Any]:
    """``{id, entity_type, name, platform, qualified_name}`` of an entity selected with ``ENTITY_NAME``."""
    entity = entity or {}
    etype = entity.get("type")
    urn = entity.get("urn")
    if etype == "DATASET":
        ident = dataset_identity(entity) if entity.get("name") else None
        parsed = dataset_name_from_urn(urn, get(entity, "dataPlatformInstance", "instanceId"))
        return {
            "id": urn,
            "entity_type": "dataset",
            "name": get(entity, "properties", "name") or (ident or {}).get("table_name") or parsed.table,
            "platform": (ident or {}).get("platform") or parsed.platform,
            "qualified_name": (ident or {}).get("qualified_name") or parsed.qualified_name,
        }
    return {
        "id": urn,
        "entity_type": entity_type_name(etype),
        "name": entity_display_name(entity),
        "platform": entity_platform(entity),
        "qualified_name": None,
    }


# ----------------------------------------------------------------------------- lineage_edges

LINEAGE_EDGES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("level", pa.string(), "'dataset' (entity-to-entity) or 'column' (column-to-column).", nullable=False),
        column("upstream_id", pa.string(), "URN of the upstream (source) entity.", nullable=False),
        column("upstream_type", pa.string(), "Upstream entity type: dataset, data_job, chart, ..."),
        column("upstream_platform", pa.string(), "Platform the upstream entity lives on, e.g. snowflake, airflow."),
        column("upstream_name", pa.string(), "Upstream display name."),
        column("upstream_qualified_name", pa.string(), "Lower-cased database.schema.table when upstream is a dataset."),
        column("upstream_column", pa.string(), "Upstream column (column level only)."),
        column("downstream_id", pa.string(), "URN of the downstream (derived) entity.", nullable=False),
        column("downstream_type", pa.string(), "Downstream entity type: dataset, data_job, chart, dashboard."),
        column("downstream_platform", pa.string(), "Platform the downstream entity lives on, e.g. snowflake, looker."),
        column("downstream_name", pa.string(), "Downstream display name."),
        column(
            "downstream_qualified_name", pa.string(), "Lower-cased database.schema.table when downstream is a dataset."
        ),
        column("downstream_column", pa.string(), "Downstream column (column level only)."),
        column(
            "relationship",
            pa.string(),
            "DataHub relationship for dataset-level edges: DownstreamOf, Consumes, Produces, ...",
        ),
        column("transformation", pa.string(), "Column-level transform operation (e.g. 'IDENTITY', 'SQL')."),
        column("query_id", pa.string(), "URN of the query that created a column-level edge, if known."),
        column("created_at", TSTZ, "When the edge was first recorded (dataset level)."),
    ]
)

FINE_GRAINED = "fineGrainedLineages { upstreams { urn path } downstreams { urn path } transformOperation query }"
UPSTREAM_LINEAGE = (
    "lineage(input: { direction: UPSTREAM, start: 0, count: 1000 }) { relationships { type createdOn "
    f"entity {{ urn type {ENTITY_NAME} }} }} }}"
)


@init_single_worker
@bind_fixed_schema
class LineageEdgesScan(EntityScan[NoArgs]):
    """Rows of ``lineage_edges``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LINEAGE_EDGES_SCHEMA
    OPERATION: ClassVar[str] = "LineageEdges"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATASET", "DATA_JOB", "CHART", "DASHBOARD")
    ID_COLUMN: ClassVar[str | None] = "downstream_id"
    TYPE_COLUMN: ClassVar[str | None] = "downstream_type"
    FILTERS: ClassVar[dict[str, FilterMap]] = {"downstream_platform": platform_filter()}
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} properties {{ name }} {UPSTREAM_LINEAGE} {FINE_GRAINED} }} "
        f"... on DataJob {{ jobPlatform: platform {{ name }} properties {{ name }} {UPSTREAM_LINEAGE} "
        f"inputOutput {{ {FINE_GRAINED} }} }} "
        f"... on Chart {{ platform {{ name }} properties {{ name }} {UPSTREAM_LINEAGE} }} "
        f"... on Dashboard {{ platform {{ name }} properties {{ name }} {UPSTREAM_LINEAGE} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "lineage_edges_scan"
        description = "Every lineage edge, upstream to downstream, at dataset and column level"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Direct Lineage Edges",
            category="lineage",
            keywords="lineage, upstream, downstream, column lineage, fine grained lineage, impact analysis, edges",
            doc_llm=(
                "One row per direct lineage edge. level = 'dataset': entity-to-entity edges into datasets, data "
                "jobs (tasks), charts and dashboards (upstream_id -> downstream_id, with relationship and "
                "created_at). level = 'column': DataHub fine-grained lineage (upstream_column -> "
                "downstream_column with transformation and query_id). Both ends carry type, platform, name and "
                "qualified_name (datasets), so join on qualified_name to other workers. Pushdown: "
                "downstream_id = / IN (fetches those entities' upstreams only), downstream_type, "
                "downstream_platform. For multi-hop traversal use lineage(id, direction, max_hops)."
            ),
            doc_md="Direct lineage edges (dataset and column level).",
            schema=LINEAGE_EDGES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT upstream_qualified_name, upstream_column, downstream_column "
                    "FROM datahub.main.lineage_edges WHERE level = 'column' AND downstream_id = "
                    "'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.marts.fct_orders,PROD)' "
                    "ORDER BY downstream_column"
                ),
                description="Where each column of one table comes from (downstream_id pushes down)",
            ),
            FunctionExample(
                sql=(
                    "SELECT downstream_type, count(*) AS edges FROM datahub.main.lineage_edges "
                    "WHERE level = 'dataset' GROUP BY downstream_type ORDER BY edges DESC"
                ),
                description="Lineage edges by downstream entity type",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Dataset-level edges from ``lineage(UPSTREAM)``, column-level from fine-grained lineage."""
        down = entity_summary(entity)
        rows = []
        for rel in get(entity, "lineage", "relationships") or []:
            up = entity_summary(rel.get("entity"))
            if not up["id"]:
                continue
            rows.append(
                {
                    **_ends(up, down),
                    "level": "dataset",
                    "relationship": rel.get("type"),
                    "created_at": ts(rel.get("createdOn")),
                }
            )
        fgls = entity.get("fineGrainedLineages") or get(entity, "inputOutput", "fineGrainedLineages") or []
        for fgl in fgls:
            for u in fgl.get("upstreams") or []:
                for d in fgl.get("downstreams") or []:
                    if not u.get("urn") or not d.get("urn"):
                        continue
                    rows.append(
                        {
                            **_ends(_ref_summary(u["urn"]), _ref_summary(d["urn"])),
                            "level": "column",
                            "upstream_column": simple_field_path(u.get("path")),
                            "downstream_column": simple_field_path(d.get("path")),
                            "transformation": fgl.get("transformOperation"),
                            "query_id": fgl.get("query"),
                        }
                    )
        return rows


def _ref_summary(urn: str) -> dict[str, Any]:
    parsed = dataset_name_from_urn(urn)
    return {
        "id": urn,
        "entity_type": "dataset" if parsed.platform else None,
        "name": parsed.table or parsed.full_name,
        "platform": parsed.platform,
        "qualified_name": parsed.qualified_name,
    }


def _ends(up: dict[str, Any], down: dict[str, Any]) -> dict[str, Any]:
    return {
        "catalog_tool": CATALOG_TOOL,
        "upstream_id": up["id"],
        "upstream_type": up["entity_type"],
        "upstream_platform": up["platform"],
        "upstream_name": up["name"],
        "upstream_qualified_name": up["qualified_name"],
        "downstream_id": down["id"],
        "downstream_type": down["entity_type"],
        "downstream_platform": down["platform"],
        "downstream_name": down["name"],
        "downstream_qualified_name": down["qualified_name"],
    }


# ----------------------------------------------------------------------------- lineage()


@dataclass(slots=True, frozen=True, kw_only=True)
class LineageArgs:
    """``lineage(id, direction := 'upstream', max_hops := 3)``."""

    id: Annotated[
        str, Arg(0, doc="URN of the start entity (a dataset, chart, dashboard or data job id).", pattern=r"^urn:li:")
    ]
    direction: Annotated[
        str,
        Arg(
            "direction",
            default="upstream",
            doc="'upstream' (what it depends on) or 'downstream' (impact).",
            choices=["upstream", "downstream"],
        ),
    ]
    max_hops: Annotated[
        int, Arg("max_hops", default=3, doc="Maximum lineage distance to follow; 1 to 20.", ge=1, le=20)
    ]


LINEAGE_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("start_id", pa.string(), "The start entity URN (the id argument).", nullable=False),
        column("direction", pa.string(), "'upstream' or 'downstream'.", nullable=False),
        column("id", pa.string(), "URN of the reached entity.", nullable=False),
        column("entity_type", pa.string(), "Reached entity type: dataset, data_job, chart, dashboard, ..."),
        column("name", pa.string(), "Display name."),
        column("platform", pa.string(), "Platform."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table for datasets."),
        column("degree", pa.int32(), "Hops from the start entity (1 = direct).", nullable=False),
        column("paths", pa.list_(STR_LIST), "Paths of URNs from the start entity to this one."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)

LINEAGE_TYPES = ("DATASET", "DATA_JOB", "DATA_FLOW", "CHART", "DASHBOARD", "MLMODEL", "MLFEATURE_TABLE", "NOTEBOOK")


def degree_filter(max_hops: int) -> list[str]:
    """DataHub's ``degree`` facet values covering ``max_hops`` hops ('1', '2', '3+')."""
    return ["1", "2", "3+"][: min(max_hops, 3)]


@init_single_worker
@bind_fixed_schema
class Lineage(TableFunctionGenerator[LineageArgs, ScanCursor]):
    """``lineage(id, direction, max_hops)`` via ``scrollAcrossLineage``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LINEAGE_SCHEMA
    DOCUMENT: ClassVar[str] = (
        "query vgiLineage($input: ScrollAcrossLineageInput!) { scrollAcrossLineage(input: $input) { "
        f"nextScrollId count total searchResults {{ degree paths {{ path {{ urn }} }} entity {{ urn type "
        f"{ENTITY_NAME} }} }} }} }}"
    )

    class Meta:
        """Function metadata."""

        name = "lineage"
        description = "Multi-hop upstream or downstream lineage of one entity (impact analysis)"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Lineage Traversal",
            category="lineage",
            keywords="lineage, impact analysis, upstream, downstream, dependencies, blast radius, root cause",
            doc_llm=(
                "Everything upstream (dependencies) or downstream (impact) of one entity, up to max_hops away, "
                "via DataHub's searchAcrossLineage. Arguments: id (the start URN, e.g. a datasets.id), "
                "direction := 'upstream' | 'downstream' (default 'upstream'), max_hops := 1..20 (default 3). "
                "Returns start_id, direction, id, entity_type, name, platform, qualified_name, degree (hops), "
                "paths (URN paths from the start) and url. entity_type = / IN and platform = / IN push down. "
                "For direct edges with column detail use the lineage_edges table."
            ),
            doc_md="Multi-hop lineage traversal from one entity.",
            schema=LINEAGE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, degree FROM datahub.main.lineage("
                    "'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.marts.fct_orders,PROD)') "
                    "WHERE entity_type = 'dataset' ORDER BY degree, qualified_name"
                ),
                description="Every dataset a table depends on, nearest first",
            ),
            FunctionExample(
                sql=(
                    "SELECT entity_type, name FROM datahub.main.lineage("
                    "'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.raw.orders,PROD)', "
                    "direction := 'downstream', max_hops := 5) WHERE entity_type IN ('chart', 'dashboard') "
                    "ORDER BY name"
                ),
                description="Charts and dashboards affected by a change to a raw table",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[LineageArgs]) -> ScanCursor:
        """Resolve pushed-down entity types and platforms."""
        filters = params.current_pushdown_filters
        types = list(LINEAGE_TYPES)
        wanted = eq_values(filters, "entity_type")
        if wanted is not None:
            types = [t for t in types if t.lower() in {str(w).lower() for w in wanted}]
        facets: list[dict[str, Any]] = [{"field": "degree", "values": degree_filter(params.args.max_hops)}]
        platforms = eq_values(filters, "platform")
        if platforms is not None:
            facets.append({"field": "platform", "values": [as_urn(str(p), "urn:li:dataPlatform:") for p in platforms]})
        empty = not types or (platforms is not None and not platforms)
        plan = {"types": types, "facets": facets}
        return ScanCursor(plan_json=json.dumps(plan, separators=(",", ":")), done=empty)

    @classmethod
    def process(cls, params: ProcessParams[LineageArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Fetch one ``scrollAcrossLineage`` page and emit it."""
        if state.done:
            out.finish()
            return
        plan = json.loads(state.plan_json)
        ep = endpoint(params.secrets)
        args = params.args
        search_input: dict[str, Any] = {
            "urn": args.id,
            "direction": args.direction.upper(),
            "types": plan["types"],
            "query": "*",
            "count": ep.page_size,
            "orFilters": [{"and": plan["facets"]}],
            "searchFlags": SEARCH_FLAGS,
        }
        if state.scroll_id:
            search_input["scrollId"] = state.scroll_id
        data = graphql(ep, "vgiLineage", cls.DOCUMENT, {"input": search_input})
        result = data.get("scrollAcrossLineage") or {}
        hits = [h for h in result.get("searchResults") or [] if isinstance(h, dict)]
        next_id = result.get("nextScrollId")
        state.scroll_id = str(next_id) if next_id else ""
        state.done = not next_id or not hits
        rows = []
        for h in hits:
            degree = h.get("degree")
            if degree is None or int(degree) > args.max_hops:
                continue
            summary = entity_summary(h.get("entity"))
            if not summary["id"]:
                continue
            paths = [
                [str(e.get("urn")) for e in p.get("path") or [] if isinstance(e, dict) and e.get("urn")]
                for p in h.get("paths") or []
                if isinstance(p, dict)
            ]
            rows.append(
                {
                    "catalog_tool": CATALOG_TOOL,
                    "start_id": args.id,
                    "direction": args.direction,
                    **summary,
                    "degree": int(degree),
                    "paths": paths,
                    "url": ep.entity_url(summary["id"], get(h, "entity", "type")),
                }
            )
        out.emit(rows_to_batch(rows, params.output_schema))


# ----------------------------------------------------------------------------- search()


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """``search(query, tag := NULL, glossary_term := NULL, owner := NULL)``."""

    query: Annotated[str, Arg(0, doc="Search text (DataHub full-text syntax); '*' matches everything.")]
    tag: Annotated[
        str | None, Arg("tag", default=None, doc="Only entities with this tag (name or urn:li:tag:... URN).")
    ]
    glossary_term: Annotated[
        str | None,
        Arg("glossary_term", default=None, doc="Only entities with this glossary term (name or URN)."),
    ]
    owner: Annotated[
        str | None,
        Arg("owner", default=None, doc="Only entities owned by this user (username or URN) or group (URN)."),
    ]


SEARCH_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "URN of the matching entity.", nullable=False),
        column(
            "entity_type",
            pa.string(),
            "dataset, dashboard, chart, data_flow, data_job, container, domain, "
            "data_product, glossary_term, glossary_node, tag, corp_user or corp_group.",
            nullable=False,
        ),
        column("name", pa.string(), "Display name."),
        column("platform", pa.string(), "Platform (assets only)."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table for datasets."),
        column("description", pa.string(), "Description."),
        column("owners", STR_LIST, "Owner URNs."),
        column("tags", STR_LIST, "Tag names."),
        column("glossary_terms", STR_LIST, "Glossary term names."),
        column("domain_id", pa.string(), "Domain URN."),
        column("domain", pa.string(), "Domain name."),
        column("matched_fields", STR_LIST, "Index fields the query matched (e.g. 'name', 'description')."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)

SEARCH_TYPES = (
    "DATASET",
    "DASHBOARD",
    "CHART",
    "DATA_FLOW",
    "DATA_JOB",
    "CONTAINER",
    "DOMAIN",
    "DATA_PRODUCT",
    "GLOSSARY_TERM",
    "GLOSSARY_NODE",
    "TAG",
    "CORP_USER",
    "CORP_GROUP",
)
_GOV = f"{OWNERSHIP} {TAGS} {TERMS} {DOMAIN}"


@init_single_worker
@bind_fixed_schema
class Search(EntityScan[SearchArgs]):
    """``search(query, ...)`` via ``scrollAcrossEntities``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SEARCH_SCHEMA
    OPERATION: ClassVar[str] = "Search"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = SEARCH_TYPES
    ID_COLUMN: ClassVar[str | None] = None
    TYPE_COLUMN: ClassVar[str | None] = "entity_type"
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter(), "domain_id": FilterMap("domains")}
    HIT_SELECTION: ClassVar[str] = "matchedFields { name }"
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} properties {{ name description }} editableProperties {{ description }} "
        f"{_GOV} }} "
        f"... on Dashboard {{ platform {{ name }} properties {{ name description }} editableProperties "
        f"{{ description }} {_GOV} }} "
        f"... on Chart {{ platform {{ name }} properties {{ name description }} editableProperties {{ description }} "
        f"{_GOV} }} "
        f"... on DataFlow {{ platform {{ name }} properties {{ name description }} editableProperties "
        f"{{ description }} {_GOV} }} "
        f"... on DataJob {{ properties {{ name description }} editableProperties {{ description }} {_GOV} }} "
        f"... on Container {{ platform {{ name }} properties {{ name description }} editableProperties "
        f"{{ description }} {_GOV} }} "
        f"... on Domain {{ properties {{ name description }} {OWNERSHIP} }} "
        f"... on DataProduct {{ properties {{ name description }} {_GOV} }} "
        f"... on GlossaryTerm {{ name properties {{ name definition }} {OWNERSHIP} {DOMAIN} }} "
        "... on GlossaryNode { properties { name description } } "
        "... on Tag { name properties { name description } } "
        "... on CorpUser { username properties { displayName } } "
        "... on CorpGroup { name properties { displayName description } }"
    )

    class Meta:
        """Function metadata."""

        name = "search"
        description = "Full-text search across DataHub entities with tag, glossary term and owner filters"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Search",
            category="search",
            keywords="search, find, discovery, full text, keyword, tag filter, owner filter, glossary filter",
            doc_llm=(
                "DataHub's search across entity types (datasets, dashboards, charts, pipelines, tasks, "
                "containers, domains, data products, glossary, tags, users, groups). Arguments: query "
                "(full-text; '*' = all), tag := name or URN, glossary_term := name or URN, owner := username or "
                "URN; each becomes a DataHub search filter. entity_type = / IN, platform = / IN and domain_id "
                "push down as well. Returns id, entity_type, name, platform, qualified_name, description, "
                "owners, tags, glossary_terms, domain_id, domain, matched_fields, url."
            ),
            doc_md="Search DataHub like the search bar, with facet filters.",
            schema=SEARCH_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT entity_type, name, qualified_name FROM datahub.main.search('orders') "
                    "WHERE entity_type IN ('dataset', 'dashboard') ORDER BY entity_type, name"
                ),
                description="Datasets and dashboards about orders",
            ),
            FunctionExample(
                sql=(
                    "SELECT qualified_name FROM datahub.main.search('*', tag := 'pii') "
                    "WHERE entity_type = 'dataset' AND platform = 'snowflake' ORDER BY qualified_name"
                ),
                description="Snowflake datasets tagged PII (tag, type and platform all push down)",
            ),
        ]

    @classmethod
    def search_query(cls, params: ProcessParams[Any]) -> str:
        """Return the ``query`` argument (blank -> ``*``)."""
        return str(params.args.query or "").strip() or "*"

    @classmethod
    def extra_filters(cls, params: ProcessParams[Any]) -> list[dict[str, Any]]:
        """``tag`` / ``glossary_term`` / ``owner`` arguments as facet filters."""
        args = params.args
        facets = []
        if args.tag:
            facets.append({"field": "tags", "values": [as_urn(args.tag, "urn:li:tag:")], "condition": "EQUAL"})
        if args.glossary_term:
            facets.append(
                {
                    "field": "glossaryTerms",
                    "values": [as_urn(args.glossary_term, "urn:li:glossaryTerm:")],
                    "condition": "EQUAL",
                }
            )
        if args.owner:
            facets.append({"field": "owners", "values": [as_urn(args.owner, "urn:li:corpuser:")], "condition": "EQUAL"})
        return facets

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per search hit."""
        summary = entity_summary(entity)
        domain_id, domain = domain_of(entity)
        description = description_of(entity) or get(entity, "properties", "definition")
        matched = [str(m.get("name")) for m in entity.get("_matchedFields") or [] if isinstance(m, dict)]
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                **summary,
                "entity_type": summary["entity_type"] or "unknown",
                "description": description,
                "owners": owner_names(entity.get("ownership")),
                "tags": tag_names(entity.get("tags")),
                "glossary_terms": term_names(entity.get("glossaryTerms")),
                "domain_id": domain_id,
                "domain": domain,
                "matched_fields": list(dict.fromkeys(matched)),
                "url": ep.entity_url(entity.get("urn"), entity.get("type")),
            }
        ]


LINEAGE_TABLES: list[tuple[str, type[Any], list[str] | None]] = [("lineage_edges", LineageEdgesScan, None)]
FUNCTIONS: list[type[Any]] = [Search, Lineage]
