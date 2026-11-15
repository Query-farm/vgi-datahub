"""Governance tables: owners, tags, glossary_terms, glossary_nodes, domains, data_products, users, groups."""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import bind_fixed_schema, init_single_worker

from .assets import catalog_tool_column, dataset_identity, scan_tags
from .client import SECRET_REQUIREMENT, Endpoint
from .common import CATALOG_TOOL, STR_LIST, NoArgs, entity_type_name, get
from .meta import column
from .scan import DOMAIN, OWNERSHIP, TAGS, TERMS, EntityScan, FilterMap, domain_of, owner_names, tag_names, term_names

# ----------------------------------------------------------------------------- owners

OWNED_TYPES = (
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
)

OWNERS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("entity_id", pa.string(), "URN of the owned entity.", nullable=False),
        column(
            "entity_type",
            pa.string(),
            "Owned entity type: dataset, dashboard, chart, data_flow, data_job, container, domain, "
            "data_product, glossary_term, glossary_node or tag.",
            nullable=False,
        ),
        column("entity_name", pa.string(), "Name of the owned entity."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table when the entity is a dataset."),
        column("owner_id", pa.string(), "Owner URN (urn:li:corpuser:... or urn:li:corpGroup:...).", nullable=False),
        column("owner_type", pa.string(), "'user' or 'group'.", nullable=False),
        column("owner_name", pa.string(), "Username (users) or group name (groups)."),
        column("owner_display_name", pa.string(), "Display name."),
        column("owner_email", pa.string(), "Email address, when known."),
        column(
            "ownership_type",
            pa.string(),
            "Ownership type: the custom type's name (e.g. 'Technical Owner') or the legacy enum "
            "(TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, ...).",
        ),
        column("ownership_type_id", pa.string(), "URN of the ownership type entity."),
    ]
)

_OWNED_SELECTION = " ".join(
    f"... on {t} {{ {OWNERSHIP} }}"
    for t in (
        "Dataset",
        "Dashboard",
        "Chart",
        "DataFlow",
        "DataJob",
        "Container",
        "Domain",
        "DataProduct",
        "GlossaryTerm",
        "GlossaryNode",
        "Tag",
    )
)


def entity_display_name(entity: dict[str, Any]) -> str | None:
    """Return a readable name for any entity selected with ``ENTITY_NAME`` (or similar)."""
    name = (
        get(entity, "properties", "name")
        or entity.get("name")
        or entity.get("username")
        or get(entity, "properties", "displayName")
    )
    return str(name) if name else None


@init_single_worker
@bind_fixed_schema
class OwnersScan(EntityScan[NoArgs]):
    """Rows of ``owners``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OWNERS_SCHEMA
    OPERATION: ClassVar[str] = "Owners"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = OWNED_TYPES
    ID_COLUMN: ClassVar[str | None] = "entity_id"
    TYPE_COLUMN: ClassVar[str | None] = "entity_type"
    FILTERS: ClassVar[dict[str, FilterMap]] = {"owner_id": FilterMap("owners")}
    SELECTION: ClassVar[str] = (
        "... on Dataset { name origin platform { name } dataPlatformInstance { instanceId } properties { name } } "
        "... on Dashboard { properties { name } } ... on Chart { properties { name } } "
        "... on DataFlow { properties { name } } ... on DataJob { properties { name } } "
        "... on Container { properties { name } } ... on Domain { properties { name } } "
        "... on DataProduct { properties { name } } ... on GlossaryTerm { name properties { name } } "
        "... on GlossaryNode { properties { name } } ... on Tag { name properties { name } } " + _OWNED_SELECTION
    )

    class Meta:
        """Scan metadata."""

        name = "owners_scan"
        description = "Who owns what: one row per (entity, owner) with the ownership type"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Asset Ownership",
            category="governance",
            keywords="owners, ownership, stewards, technical owner, business owner, accountability",
            doc_llm=(
                "One row per (entity, owner) across datasets, dashboards, charts, data_flows, data_jobs, "
                "containers, domains, data products, glossary terms / nodes and tags: entity_id, entity_type, "
                "entity_name, qualified_name (datasets), owner_id (user or group URN), owner_type ('user' / "
                "'group'), owner_name, owner_display_name, owner_email, ownership_type, ownership_type_id. "
                "Pushdown: entity_type = / IN narrows the searched types, entity_id fetches those URNs, "
                "owner_id = '<urn>' becomes DataHub's owners search filter."
            ),
            doc_md="Ownership of every entity, one row per owner.",
            schema=OWNERS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT owner_name, count(*) AS datasets FROM datahub.main.owners "
                    "WHERE entity_type = 'dataset' GROUP BY owner_name ORDER BY datasets DESC"
                ),
                description="Datasets owned per person or group",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per owner of the entity."""
        etype = entity.get("type")
        qualified = dataset_identity(entity)["qualified_name"] if etype == "DATASET" else None
        rows = []
        for o in get(entity, "ownership", "owners") or []:
            owner = o.get("owner") or {}
            if not owner.get("urn"):
                continue
            is_group = owner.get("__typename") == "CorpGroup" or str(owner["urn"]).startswith("urn:li:corpGroup:")
            props = owner.get("properties") or {}
            rows.append(
                {
                    "catalog_tool": CATALOG_TOOL,
                    "entity_id": entity.get("urn"),
                    "entity_type": entity_type_name(etype),
                    "entity_name": entity_display_name(entity),
                    "qualified_name": qualified,
                    "owner_id": owner["urn"],
                    "owner_type": "group" if is_group else "user",
                    "owner_name": owner.get("name") if is_group else owner.get("username"),
                    "owner_display_name": props.get("displayName"),
                    "owner_email": props.get("email"),
                    "ownership_type": get(o, "ownershipType", "info", "name") or o.get("type"),
                    "ownership_type_id": get(o, "ownershipType", "urn"),
                }
            )
        return rows


# ----------------------------------------------------------------------------- tags

TAGS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Tag URN, e.g. 'urn:li:tag:pii'.", nullable=False),
        column("name", pa.string(), "Tag name (the display name when set).", nullable=False),
        column("description", pa.string(), "What the tag means and when to apply it."),
        column("color", pa.string(), "Hex colour, e.g. '#FF0000'."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class TagsScan(EntityScan[NoArgs]):
    """Rows of ``tags``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TAGS_SCHEMA
    OPERATION: ClassVar[str] = "Tags"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("TAG",)
    SELECTION: ClassVar[str] = "... on Tag { name description properties { name description colorHex } }"

    class Meta:
        """Scan metadata."""

        name = "tags_scan"
        description = "Tag definitions (the tags lists on assets and columns hold their names)"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Tag Definitions",
            category="governance",
            keywords="tags, labels, classification, pii, sensitivity",
            doc_llm=(
                "One row per tag definition: id (URN), name, description, color, url. Tags applied to assets "
                "appear by name in the `tags` list columns of datasets, columns, dashboards, ...; count usage "
                "with UNNEST. Pushdown: id."
            ),
            doc_md="Tag definitions.",
            schema=TAGS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT t.name, count(c.column_name) AS tagged_columns FROM datahub.main.tags t "
                    "LEFT JOIN datahub.main.columns c ON list_contains(c.tags, t.name) "
                    "GROUP BY t.name ORDER BY tagged_columns DESC"
                ),
                description="How many columns carry each tag",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per tag."""
        props = entity.get("properties") or {}
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "name": props.get("name") or entity.get("name") or entity.get("urn"),
                "description": props.get("description") or entity.get("description"),
                "color": props.get("colorHex"),
                "url": ep.entity_url(entity.get("urn"), "TAG"),
            }
        ]


# ----------------------------------------------------------------------------- glossary

GLOSSARY_TERMS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Glossary term URN.", nullable=False),
        column("name", pa.string(), "Term name.", nullable=False),
        column("hierarchical_name", pa.string(), "Dotted name including parent nodes."),
        column("description", pa.string(), "The term's definition."),
        column("term_source", pa.string(), "'INTERNAL' or 'EXTERNAL'."),
        column("source_ref", pa.string(), "External source reference (e.g. a FIBO id)."),
        column("source_url", pa.string(), "External source URL."),
        column("parent_node_id", pa.string(), "URN of the immediate parent glossary node."),
        column("parent_path", STR_LIST, "Parent node names from the root down."),
        column("owners", STR_LIST, "Owner URNs."),
        column("domain_id", pa.string(), "Domain URN."),
        column("domain", pa.string(), "Domain name."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)

PARENT_NODES = "parentNodes { nodes { urn properties { name } } }"


def _parents(entity: dict[str, Any]) -> tuple[str | None, list[str]]:
    nodes = [n for n in get(entity, "parentNodes", "nodes") or [] if isinstance(n, dict)]
    # parentNodes lists the nearest parent first.
    return (nodes[0].get("urn") if nodes else None), [
        str(get(n, "properties", "name") or n.get("urn")) for n in reversed(nodes)
    ]


@init_single_worker
@bind_fixed_schema
class GlossaryTermsScan(EntityScan[NoArgs]):
    """Rows of ``glossary_terms``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GLOSSARY_TERMS_SCHEMA
    OPERATION: ClassVar[str] = "GlossaryTerms"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("GLOSSARY_TERM",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {"domain_id": FilterMap("domains")}
    SELECTION: ClassVar[str] = (
        "... on GlossaryTerm { name hierarchicalName properties { name definition termSource sourceRef sourceUrl } "
        f"{PARENT_NODES} {OWNERSHIP} {DOMAIN} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "glossary_terms_scan"
        description = "Business glossary terms with definitions and their place in the glossary hierarchy"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Business Glossary Terms",
            category="governance",
            keywords="glossary, business glossary, terms, definitions, semantics, classification",
            doc_llm=(
                "One row per business glossary term: id, name, hierarchical_name, description (definition), "
                "term_source, source_ref, source_url, parent_node_id, parent_path (root-first node names), "
                "owners, domain_id, domain, url. Terms applied to assets appear by name in the glossary_terms "
                "list columns. Pushdown: id, domain_id."
            ),
            doc_md="Business glossary terms.",
            schema=GLOSSARY_TERMS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT parent_path[1] AS top_node, count(*) AS terms FROM datahub.main.glossary_terms "
                    "GROUP BY top_node ORDER BY terms DESC"
                ),
                description="Terms per top-level glossary node",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per term."""
        props = entity.get("properties") or {}
        parent, path = _parents(entity)
        domain_id, domain = domain_of(entity)
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "name": props.get("name") or entity.get("name") or entity.get("urn"),
                "hierarchical_name": entity.get("hierarchicalName"),
                "description": props.get("definition"),
                "term_source": props.get("termSource"),
                "source_ref": props.get("sourceRef"),
                "source_url": props.get("sourceUrl"),
                "parent_node_id": parent,
                "parent_path": path,
                "owners": owner_names(entity.get("ownership")),
                "domain_id": domain_id,
                "domain": domain,
                "url": ep.entity_url(entity.get("urn"), "GLOSSARY_TERM"),
            }
        ]


GLOSSARY_NODES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Glossary node URN.", nullable=False),
        column("name", pa.string(), "Node name.", nullable=False),
        column("description", pa.string(), "What the node groups."),
        column("parent_node_id", pa.string(), "URN of the parent node (NULL at the root)."),
        column("parent_path", STR_LIST, "Parent node names from the root down."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class GlossaryNodesScan(EntityScan[NoArgs]):
    """Rows of ``glossary_nodes``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GLOSSARY_NODES_SCHEMA
    OPERATION: ClassVar[str] = "GlossaryNodes"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("GLOSSARY_NODE",)
    SELECTION: ClassVar[str] = f"... on GlossaryNode {{ properties {{ name description }} {PARENT_NODES} }}"

    class Meta:
        """Scan metadata."""

        name = "glossary_nodes_scan"
        description = "Glossary nodes (term groups) and their hierarchy"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Business Glossary Nodes",
            category="governance",
            keywords="glossary nodes, term groups, glossary hierarchy, folders",
            doc_llm=(
                "One row per glossary node (a folder of terms): id, name, description, parent_node_id, "
                "parent_path, url. glossary_terms.parent_node_id points here. Pushdown: id."
            ),
            doc_md="Glossary nodes (term groups).",
            schema=GLOSSARY_NODES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT n.name, count(t.id) AS terms FROM datahub.main.glossary_nodes n "
                    "LEFT JOIN datahub.main.glossary_terms t ON t.parent_node_id = n.id GROUP BY n.name ORDER BY n.name"
                ),
                description="Terms directly under each glossary node",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per node."""
        props = entity.get("properties") or {}
        parent, path = _parents(entity)
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "name": props.get("name") or entity.get("urn"),
                "description": props.get("description"),
                "parent_node_id": parent,
                "parent_path": path,
                "url": ep.entity_url(entity.get("urn"), "GLOSSARY_NODE"),
            }
        ]


# ----------------------------------------------------------------------------- domains / data products

DOMAINS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Domain URN.", nullable=False),
        column("name", pa.string(), "Domain name.", nullable=False),
        column("description", pa.string(), "What the domain covers."),
        column("parent_domain_id", pa.string(), "URN of the parent domain (NULL for a top-level domain)."),
        column("asset_count", pa.int64(), "Number of assets assigned to the domain."),
        column("owners", STR_LIST, "Owner URNs."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DomainsScan(EntityScan[NoArgs]):
    """Rows of ``domains``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DOMAINS_SCHEMA
    OPERATION: ClassVar[str] = "Domains"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DOMAIN",)
    SELECTION: ClassVar[str] = (
        "... on Domain { properties { name description } parentDomains { domains { urn } } "
        f"entities(input: {{ start: 0, count: 0 }}) {{ total }} {OWNERSHIP} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "domains_scan"
        description = "Domains (business areas) with their parent domain and asset counts"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Business Domains",
            category="governance",
            keywords="domains, business domains, data mesh, ownership areas",
            doc_llm=(
                "One row per domain: id, name, description, parent_domain_id (nested domains), asset_count, "
                "owners, url. Assets carry domain_id / domain columns. Pushdown: id."
            ),
            doc_md="Domains and how many assets each holds.",
            schema=DOMAINS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql="SELECT name, asset_count FROM datahub.main.domains ORDER BY asset_count DESC",
                description="Domains by asset count",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per domain."""
        props = entity.get("properties") or {}
        parents = [d for d in get(entity, "parentDomains", "domains") or [] if isinstance(d, dict)]
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "name": props.get("name") or entity.get("urn"),
                "description": props.get("description"),
                "parent_domain_id": parents[0].get("urn") if parents else None,
                "asset_count": get(entity, "entities", "total"),
                "owners": owner_names(entity.get("ownership")),
                "url": ep.entity_url(entity.get("urn"), "DOMAIN"),
            }
        ]


DATA_PRODUCTS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Data product URN.", nullable=False),
        column("name", pa.string(), "Data product name.", nullable=False),
        column("description", pa.string(), "What the data product offers its consumers."),
        column("domain_id", pa.string(), "Domain URN."),
        column("domain", pa.string(), "Domain name."),
        column("asset_count", pa.int64(), "Number of assets in the data product."),
        column("owners", STR_LIST, "Owner URNs."),
        column("tags", STR_LIST, "Tag names."),
        column("glossary_terms", STR_LIST, "Glossary term names."),
        column("external_url", pa.string(), "External link."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DataProductsScan(EntityScan[NoArgs]):
    """Rows of ``data_products``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_PRODUCTS_SCHEMA
    OPERATION: ClassVar[str] = "DataProducts"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATA_PRODUCT",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {"domain_id": FilterMap("domains")}
    SELECTION: ClassVar[str] = (
        "... on DataProduct { properties { name description externalUrl numAssets } "
        f"{DOMAIN} {OWNERSHIP} {TAGS} {TERMS} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "data_products_scan"
        description = "Data products: curated groups of assets owned by a domain"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="DataHub Data Products",
            category="governance",
            keywords="data products, data mesh, curated assets, domains",
            doc_llm=(
                "One row per data product: id, name, description, domain_id, domain, asset_count, owners, tags, "
                "glossary_terms, external_url, url. Pushdown: id, domain_id."
            ),
            doc_md="Data products and their domains.",
            schema=DATA_PRODUCTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT domain, count(*) AS products, sum(asset_count) AS assets "
                    "FROM datahub.main.data_products GROUP BY domain ORDER BY products DESC"
                ),
                description="Data products and assets per domain",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per data product."""
        props = entity.get("properties") or {}
        domain_id, domain = domain_of(entity)
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "name": props.get("name") or entity.get("urn"),
                "description": props.get("description"),
                "domain_id": domain_id,
                "domain": domain,
                "asset_count": props.get("numAssets"),
                "owners": owner_names(entity.get("ownership")),
                "tags": tag_names(entity.get("tags")),
                "glossary_terms": term_names(entity.get("glossaryTerms")),
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "DATA_PRODUCT"),
            }
        ]


# ----------------------------------------------------------------------------- users / groups

USERS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "User URN, e.g. 'urn:li:corpuser:jdoe'.", nullable=False),
        column("username", pa.string(), "Login name, the part after urn:li:corpuser:.", nullable=False),
        column("display_name", pa.string(), "Display name (UI-edited first)."),
        column("email", pa.string(), "Email address."),
        column("title", pa.string(), "Job title."),
        column("department", pa.string(), "Department name."),
        column("active", pa.bool_(), "True unless the user is suspended or inactive in the source directory."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class UsersScan(EntityScan[NoArgs]):
    """Rows of ``users``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = USERS_SCHEMA
    OPERATION: ClassVar[str] = "Users"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("CORP_USER",)
    SELECTION: ClassVar[str] = (
        "... on CorpUser { username status properties { displayName email title departmentName active } "
        "editableProperties { displayName email title } }"
    )

    class Meta:
        """Scan metadata."""

        name = "users_scan"
        description = "DataHub users (corp users) with name, email, title and department"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="DataHub Users",
            category="governance",
            keywords="users, people, corp users, directory, owners, email",
            doc_llm=(
                "One row per DataHub user: id (urn:li:corpuser:...), username, display_name, email, title, "
                "department, active, url. owners.owner_id joins to id. Pushdown: id."
            ),
            doc_md="Users known to DataHub.",
            schema=USERS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT u.display_name, count(o.entity_id) AS owned FROM datahub.main.users u "
                    "LEFT JOIN datahub.main.owners o ON o.owner_id = u.id GROUP BY u.display_name ORDER BY owned DESC"
                ),
                description="Assets owned per user",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per user."""
        props = entity.get("properties") or {}
        edits = entity.get("editableProperties") or {}
        status = entity.get("status")
        active = props.get("active") if props else None
        if status == "SUSPENDED":
            active = False
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "username": entity.get("username") or str(entity.get("urn")).removeprefix("urn:li:corpuser:"),
                "display_name": edits.get("displayName") or props.get("displayName"),
                "email": edits.get("email") or props.get("email"),
                "title": edits.get("title") or props.get("title"),
                "department": props.get("departmentName"),
                "active": active,
                "url": ep.entity_url(entity.get("urn"), "CORP_USER"),
            }
        ]


GROUPS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Group URN, e.g. 'urn:li:corpGroup:data-eng'.", nullable=False),
        column("name", pa.string(), "Group name.", nullable=False),
        column("display_name", pa.string(), "Human-friendly group name shown in the UI."),
        column("description", pa.string(), "What the team does."),
        column("email", pa.string(), "Group email."),
        column("slack", pa.string(), "Slack channel."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class GroupsScan(EntityScan[NoArgs]):
    """Rows of ``groups``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GROUPS_SCHEMA
    OPERATION: ClassVar[str] = "Groups"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("CORP_GROUP",)
    SELECTION: ClassVar[str] = "... on CorpGroup { name properties { displayName description email slack } }"

    class Meta:
        """Scan metadata."""

        name = "groups_scan"
        description = "DataHub groups (teams) that can own assets"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="DataHub Groups",
            category="governance",
            keywords="groups, teams, corp groups, owners",
            doc_llm=(
                "One row per DataHub group: id (urn:li:corpGroup:...), name, display_name, description, email, "
                "slack, url. owners rows with owner_type = 'group' join on owner_id. Pushdown: id."
            ),
            doc_md="Groups (teams) known to DataHub.",
            schema=GROUPS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT g.name, count(o.entity_id) AS owned FROM datahub.main.groups g "
                    "LEFT JOIN datahub.main.owners o ON o.owner_id = g.id GROUP BY g.name ORDER BY owned DESC"
                ),
                description="Assets owned per team",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per group."""
        props = entity.get("properties") or {}
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "name": entity.get("name") or str(entity.get("urn")).removeprefix("urn:li:corpGroup:"),
                "display_name": props.get("displayName"),
                "description": props.get("description"),
                "email": props.get("email"),
                "slack": props.get("slack"),
                "url": ep.entity_url(entity.get("urn"), "CORP_GROUP"),
            }
        ]


GOVERNANCE_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("owners", OwnersScan, None),
    ("tags", TagsScan, ["id"]),
    ("glossary_terms", GlossaryTermsScan, ["id"]),
    ("glossary_nodes", GlossaryNodesScan, ["id"]),
    ("domains", DomainsScan, ["id"]),
    ("data_products", DataProductsScan, ["id"]),
    ("users", UsersScan, ["id"]),
    ("groups", GroupsScan, ["id"]),
]
