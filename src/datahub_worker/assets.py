"""Asset tables: datasets, columns, containers, dashboards, charts, data_flows, data_jobs."""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import bind_fixed_schema, init_single_worker

from .client import SECRET_REQUIREMENT, Endpoint
from .common import (
    CATALOG_TOOL,
    STR_LIST,
    STR_MAP,
    TSTZ,
    NoArgs,
    custom_properties,
    get,
    platform_name,
    result_columns,
    simple_field_path,
    split_dataset_name,
    ts,
)
from .meta import column, object_tags
from .scan import (
    DOMAIN,
    OWNERSHIP,
    TAGS,
    TERMS,
    EntityScan,
    FilterMap,
    description_of,
    domain_of,
    environment_filter,
    owner_names,
    platform_filter,
    tag_names,
    term_names,
)


def catalog_tool_column() -> pa.Field[Any]:
    """Return the shared ``catalog_tool`` column (always 'datahub')."""
    return column(
        "catalog_tool", pa.string(), "Catalog that produced the row; always 'datahub' (UNION key).", nullable=False
    )


def scan_tags(
    *, title: str, category: str, keywords: str, doc_llm: str, doc_md: str, schema: pa.Schema
) -> dict[str, str]:
    """Discovery tags plus ``vgi.result_columns_schema`` for a scan."""
    return {
        **object_tags(title=title, category=category, keywords=keywords, doc_llm=doc_llm, doc_md=doc_md),
        "vgi.result_columns_schema": result_columns(schema),
    }


def dataset_identity(entity: dict[str, Any]) -> dict[str, Any]:
    """Platform, environment, database / schema / table and qualified_name of a Dataset selection."""
    platform = platform_name(get(entity, "platform", "name"))
    instance = get(entity, "dataPlatformInstance", "instanceId")
    parsed = split_dataset_name(platform, entity.get("name"), entity.get("origin"), instance)
    return {
        "platform": platform,
        "platform_instance": instance,
        "environment": entity.get("origin"),
        "database": parsed.database,
        "schema": parsed.schema,
        "table_name": parsed.table or parsed.full_name,
        "qualified_name": parsed.qualified_name,
    }


DATASET_IDENTITY = "name origin platform { name } dataPlatformInstance { instanceId }"

# ----------------------------------------------------------------------------- datasets

DATASETS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column(
            "id",
            pa.string(),
            "Dataset URN, e.g. 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.t,PROD)'.",
            nullable=False,
        ),
        column("platform", pa.string(), "Data platform, e.g. 'snowflake', 'bigquery', 'kafka', 'dbt'."),
        column("platform_instance", pa.string(), "Platform instance id, when the platform has several."),
        column("environment", pa.string(), "Environment (DataHub FabricType): PROD, DEV, QA, ..."),
        column("database", pa.string(), "Database / project / catalog part of the name (relational platforms)."),
        column("schema", pa.string(), "Schema / dataset part of the name (relational platforms)."),
        column(
            "name", pa.string(), "Table name (relational platforms) or the full dataset name otherwise.", nullable=False
        ),
        column(
            "qualified_name",
            pa.string(),
            "Lower-cased 'database.schema.table' (fewer parts when the platform has fewer); the join key to "
            "dbt / OpenLineage / Looker and the other catalog workers.",
        ),
        column("full_name", pa.string(), "DataHub's dataset name as in the URN (without the platform instance)."),
        column("subtype", pa.string(), "First DataHub subtype: 'Table', 'View', 'Topic', 'Model', ..."),
        column("description", pa.string(), "Description (UI-edited one first, else the ingested one)."),
        column("owners", STR_LIST, "Owner URNs (users and groups); see the owners table for types."),
        column("tags", STR_LIST, "Tag names on the dataset."),
        column("glossary_terms", STR_LIST, "Glossary term names on the dataset."),
        column("domain_id", pa.string(), "Domain URN."),
        column("domain", pa.string(), "Domain name."),
        column("container_id", pa.string(), "URN of the immediate parent container (schema / database / folder)."),
        column("deprecated", pa.bool_(), "True if the dataset is marked deprecated.", nullable=False),
        column("custom_properties", STR_MAP, "Ingested custom properties."),
        column("created_at", TSTZ, "Creation time reported by the source platform."),
        column("updated_at", TSTZ, "Last modification reported by the source platform."),
        column("last_ingested_at", TSTZ, "When DataHub last ingested metadata for the dataset."),
        column("external_url", pa.string(), "Link to the dataset in its source platform."),
        column("url", pa.string(), "Link to the dataset in the DataHub UI (needs FRONTEND_URL or a Cloud GMS_URL)."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DatasetsScan(EntityScan[NoArgs]):
    """Rows of ``datasets``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASETS_SCHEMA
    OPERATION: ClassVar[str] = "Datasets"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATASET",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {
        "platform": platform_filter(),
        "environment": environment_filter(),
        "domain_id": FilterMap("domains"),
        "container_id": FilterMap("container"),
    }
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} lastIngested "
        "properties { name qualifiedName description externalUrl created lastModified { time } "
        "customProperties { key value } } editableProperties { description } subTypes { typeNames } "
        f"container {{ urn }} deprecation {{ deprecated }} {OWNERSHIP} {TAGS} {TERMS} {DOMAIN} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "datasets_scan"
        description = "Every DataHub dataset with platform, qualified name, owners, tags, terms and domain"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="DataHub Datasets",
            category="assets",
            keywords="datasets, tables, views, topics, data catalog, qualified name, platform, owners, tags",
            doc_llm=(
                "One row per DataHub dataset (table, view, topic, dbt model, ...): id (URN), platform, "
                "platform_instance, environment, database, schema, name, qualified_name (lower-cased "
                "database.schema.table, the cross-worker join key), subtype, description, owners / tags / "
                "glossary_terms (lists), domain_id, domain, container_id, deprecated, custom_properties, "
                "created_at, updated_at, last_ingested_at, external_url, url. custom_properties is a "
                "`MAP(VARCHAR, VARCHAR)`. Pushdown: id = / IN fetches those "
                "URNs directly; platform, environment, domain_id and container_id become search filters."
            ),
            doc_md="Datasets with their platform, qualified name and governance metadata.",
            schema=DATASETS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT schema, count(*) AS tables FROM datahub.main.datasets "
                    "WHERE platform = 'snowflake' GROUP BY schema ORDER BY tables DESC"
                ),
                description="Snowflake datasets per schema (platform pushes down)",
            ),
            FunctionExample(
                sql=(
                    "SELECT qualified_name, domain FROM datahub.main.datasets "
                    "WHERE len(owners) = 0 AND NOT deprecated ORDER BY qualified_name"
                ),
                description="Datasets nobody owns",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per dataset."""
        ident = dataset_identity(entity)
        props = entity.get("properties") or {}
        domain_id, domain = domain_of(entity)
        subtypes = get(entity, "subTypes", "typeNames") or []
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "platform": ident["platform"],
                "platform_instance": ident["platform_instance"],
                "environment": ident["environment"],
                "database": ident["database"],
                "schema": ident["schema"],
                "name": ident["table_name"] or entity.get("name") or entity.get("urn"),
                "qualified_name": ident["qualified_name"],
                "full_name": entity.get("name"),
                "subtype": subtypes[0] if subtypes else None,
                "description": description_of(entity),
                "owners": owner_names(entity.get("ownership")),
                "tags": tag_names(entity.get("tags")),
                "glossary_terms": term_names(entity.get("glossaryTerms")),
                "domain_id": domain_id,
                "domain": domain,
                "container_id": get(entity, "container", "urn"),
                "deprecated": bool(get(entity, "deprecation", "deprecated")),
                "custom_properties": custom_properties(props.get("customProperties")),
                "created_at": ts(props.get("created")),
                "updated_at": ts(get(props, "lastModified", "time")),
                "last_ingested_at": ts(entity.get("lastIngested")),
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "DATASET"),
            }
        ]


# ----------------------------------------------------------------------------- columns

COLUMNS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("dataset_id", pa.string(), "URN of the dataset (datasets.id).", nullable=False),
        column("platform", pa.string(), "Data platform of the dataset."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the dataset."),
        column(
            "column_name", pa.string(), "Column name; nested fields as dotted paths ('address.city').", nullable=False
        ),
        column(
            "field_path",
            pa.string(),
            "DataHub's raw fieldPath (v2 paths keep their [type=...] tokens).",
            nullable=False,
        ),
        column("ordinal_position", pa.int32(), "1-based position in the dataset's schema.", nullable=False),
        column("data_type", pa.string(), "DataHub type: STRING, NUMBER, BOOLEAN, DATE, TIME, STRUCT, ARRAY, ..."),
        column("native_data_type", pa.string(), "The source platform's type, e.g. 'VARCHAR(16777216)'."),
        column("nullable", pa.bool_(), "True if the column accepts NULL.", nullable=False),
        column("is_primary_key", pa.bool_(), "True if the column is in the schema's primary key.", nullable=False),
        column("description", pa.string(), "Column description (UI-edited one first, else the ingested one)."),
        column("tags", STR_LIST, "Tag names on the column (ingested and UI-edited)."),
        column("glossary_terms", STR_LIST, "Glossary term names on the column (ingested and UI-edited)."),
    ]
)


@init_single_worker
@bind_fixed_schema
class ColumnsScan(EntityScan[NoArgs]):
    """Rows of ``columns``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COLUMNS_SCHEMA
    OPERATION: ClassVar[str] = "Columns"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATASET",)
    ID_COLUMN: ClassVar[str | None] = "dataset_id"
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter()}
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} schemaMetadata {{ primaryKeys fields {{ fieldPath nullable "
        f"description type nativeDataType globalTags {{ tags {{ tag {{ urn name properties {{ name }} }} }} }} "
        f"{TERMS} }} }} editableSchemaMetadata {{ editableSchemaFieldInfo {{ fieldPath description "
        f"globalTags {{ tags {{ tag {{ urn name properties {{ name }} }} }} }} {TERMS} }} }} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "columns_scan"
        description = "Every column of every DataHub dataset with types, nullability, descriptions, tags and terms"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Dataset Columns",
            category="assets",
            keywords="columns, schema fields, data types, nullable, primary key, column tags, pii, glossary",
            doc_llm=(
                "One row per schema field of each dataset: dataset_id, platform, qualified_name, column_name "
                "(nested fields dotted), field_path (raw), ordinal_position, data_type (DataHub type), "
                "native_data_type, nullable, is_primary_key, description, tags and glossary_terms (both lists; "
                "ingested and UI-edited merged). Filter `dataset_id = '<urn>'` to fetch one dataset's schema "
                "without scrolling the catalog; `platform` pushes down too. Find PII with "
                "`list_contains(tags, 'pii')`."
            ),
            doc_md="Columns of every dataset, with types and column-level tags and terms.",
            schema=COLUMNS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, column_name FROM datahub.main.columns "
                    "WHERE list_contains(tags, 'pii') ORDER BY qualified_name, column_name"
                ),
                description="Columns tagged PII",
            ),
            FunctionExample(
                sql=(
                    "SELECT column_name, native_data_type, nullable FROM datahub.main.columns "
                    "WHERE dataset_id = "
                    "'urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.marts.fct_orders,PROD)' "
                    "ORDER BY ordinal_position"
                ),
                description="One dataset's schema (dataset_id pushes down)",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per schema field."""
        ident = dataset_identity(entity)
        schema = entity.get("schemaMetadata") or {}
        pks = {str(p) for p in schema.get("primaryKeys") or []}
        edits = {
            e.get("fieldPath"): e
            for e in get(entity, "editableSchemaMetadata", "editableSchemaFieldInfo") or []
            if isinstance(e, dict)
        }
        rows = []
        for pos, f in enumerate(schema.get("fields") or [], start=1):
            path = f.get("fieldPath")
            edit = edits.get(path) or {}
            name = simple_field_path(path)
            rows.append(
                {
                    "catalog_tool": CATALOG_TOOL,
                    "dataset_id": entity.get("urn"),
                    "platform": ident["platform"],
                    "qualified_name": ident["qualified_name"],
                    "column_name": name,
                    "field_path": path,
                    "ordinal_position": pos,
                    "data_type": f.get("type"),
                    "native_data_type": f.get("nativeDataType"),
                    "nullable": bool(f.get("nullable")),
                    "is_primary_key": path in pks or name in pks,
                    "description": edit.get("description") or f.get("description"),
                    "tags": _merge(tag_names(f.get("globalTags")), tag_names(edit.get("globalTags"))),
                    "glossary_terms": _merge(term_names(f.get("glossaryTerms")), term_names(edit.get("glossaryTerms"))),
                }
            )
        return rows


def _merge(a: list[str], b: list[str]) -> list[str]:
    return list(dict.fromkeys([*a, *b]))


# ----------------------------------------------------------------------------- containers

CONTAINERS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Container URN.", nullable=False),
        column("platform", pa.string(), "Data platform."),
        column("name", pa.string(), "Container name.", nullable=False),
        column("subtype", pa.string(), "First subtype: 'Database', 'Schema', 'Project', 'Dataset', 'Folder', ..."),
        column("qualified_name", pa.string(), "Lower-cased dotted path of the container (e.g. 'analytics.marts')."),
        column("parent_container_id", pa.string(), "URN of the parent container."),
        column("container_path", STR_LIST, "Container names from the root down to this one."),
        column("description", pa.string(), "Container description (UI-edited one first)."),
        column("owners", STR_LIST, "Owner URNs."),
        column("tags", STR_LIST, "Tag names."),
        column("domain_id", pa.string(), "Domain URN."),
        column("domain", pa.string(), "Domain name."),
        column("external_url", pa.string(), "Link in the source platform."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class ContainersScan(EntityScan[NoArgs]):
    """Rows of ``containers``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CONTAINERS_SCHEMA
    OPERATION: ClassVar[str] = "Containers"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("CONTAINER",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {
        "platform": platform_filter(),
        "parent_container_id": FilterMap("container"),
        "domain_id": FilterMap("domains"),
    }
    SELECTION: ClassVar[str] = (
        "... on Container { platform { name } properties { name description externalUrl qualifiedName } "
        "editableProperties { description } subTypes { typeNames } container { urn } "
        f"parentContainers {{ containers {{ urn properties {{ name }} }} }} {OWNERSHIP} {TAGS} {DOMAIN} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "containers_scan"
        description = "DataHub containers: the database / schema / project / folder hierarchy datasets live in"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Database And Schema Containers",
            category="assets",
            keywords="containers, databases, schemas, projects, folders, hierarchy, browse path",
            doc_llm=(
                "One row per container (database, schema, BigQuery project / dataset, folder, ...): id, platform, "
                "name, subtype, qualified_name, parent_container_id, container_path (root-first names), "
                "description, owners, tags, domain_id, domain, external_url, url. datasets.container_id points "
                "here. Pushdown: id, platform, parent_container_id, domain_id."
            ),
            doc_md="The container hierarchy (databases, schemas, folders).",
            schema=CONTAINERS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT c.qualified_name, count(d.id) AS datasets FROM datahub.main.containers c "
                    "LEFT JOIN datahub.main.datasets d ON d.container_id = c.id "
                    "WHERE c.subtype = 'Schema' GROUP BY ALL ORDER BY datasets DESC"
                ),
                description="Datasets per schema container",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per container."""
        props = entity.get("properties") or {}
        parents = [c for c in get(entity, "parentContainers", "containers") or [] if isinstance(c, dict)]
        # parentContainers lists the nearest parent first.
        path = [str(get(c, "properties", "name") or c.get("urn")) for c in reversed(parents)]
        name = props.get("name") or entity.get("urn")
        qualified = props.get("qualifiedName") or ".".join([*path, str(name)])
        domain_id, domain = domain_of(entity)
        subtypes = get(entity, "subTypes", "typeNames") or []
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "platform": platform_name(get(entity, "platform", "name")),
                "name": name,
                "subtype": subtypes[0] if subtypes else None,
                "qualified_name": str(qualified).lower(),
                "parent_container_id": get(entity, "container", "urn"),
                "container_path": [*path, str(name)],
                "description": description_of(entity),
                "owners": owner_names(entity.get("ownership")),
                "tags": tag_names(entity.get("tags")),
                "domain_id": domain_id,
                "domain": domain,
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "CONTAINER"),
            }
        ]


# ----------------------------------------------------------------------------- dashboards / charts


def _asset_columns(id_doc: str) -> list[pa.Field[Any]]:
    return [
        catalog_tool_column(),
        column("id", pa.string(), id_doc, nullable=False),
        column("platform", pa.string(), "Tool / platform, e.g. 'looker', 'tableau', 'airflow'."),
        column("name", pa.string(), "Display name."),
        column("description", pa.string(), "Description (UI-edited one first)."),
    ]


def _governance_columns() -> list[pa.Field[Any]]:
    return [
        column("owners", STR_LIST, "Owner URNs."),
        column("tags", STR_LIST, "Tag names."),
        column("glossary_terms", STR_LIST, "Glossary term names."),
        column("domain_id", pa.string(), "Domain URN."),
        column("domain", pa.string(), "Domain name."),
        column("container_id", pa.string(), "Parent container URN (workspace / folder / project)."),
    ]


def _governance_values(entity: dict[str, Any]) -> dict[str, Any]:
    domain_id, domain = domain_of(entity)
    return {
        "owners": owner_names(entity.get("ownership")),
        "tags": tag_names(entity.get("tags")),
        "glossary_terms": term_names(entity.get("glossaryTerms")),
        "domain_id": domain_id,
        "domain": domain,
        "container_id": get(entity, "container", "urn"),
    }


GOVERNANCE_SELECTION = f"container {{ urn }} {OWNERSHIP} {TAGS} {TERMS} {DOMAIN}"

DASHBOARDS_SCHEMA = pa.schema(
    [
        *_asset_columns("Dashboard URN."),
        column("dashboard_key", pa.string(), "The tool's own dashboard id."),
        *_governance_columns(),
        column("created_at", TSTZ, "Creation time in the tool."),
        column("updated_at", TSTZ, "Last modification in the tool."),
        column("last_refreshed_at", TSTZ, "Last data refresh reported by the tool."),
        column("view_count", pa.int64(), "View count from DataHub's dashboard stats summary."),
        column("external_url", pa.string(), "Link to the dashboard in the BI tool."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DashboardsScan(EntityScan[NoArgs]):
    """Rows of ``dashboards``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DASHBOARDS_SCHEMA
    OPERATION: ClassVar[str] = "Dashboards"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DASHBOARD",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter(), "domain_id": FilterMap("domains")}
    SELECTION: ClassVar[str] = (
        "... on Dashboard { dashboardId platform { name } properties { name description externalUrl "
        "lastRefreshed created { time } lastModified { time } } editableProperties { description } "
        f"statsSummary {{ viewCount }} {GOVERNANCE_SELECTION} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "dashboards_scan"
        description = "BI dashboards indexed by DataHub, with owners, tags, domain and view counts"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="BI Dashboards",
            category="assets",
            keywords="dashboards, bi, looker, tableau, superset, power bi, reports, views",
            doc_llm=(
                "One row per dashboard: id, platform (BI tool), name, description, dashboard_key (the tool's id), "
                "owners, tags, glossary_terms, domain_id, domain, container_id, created_at, updated_at, "
                "last_refreshed_at, view_count, external_url, url. Which datasets feed a dashboard: "
                "lineage_edges WHERE downstream_type = 'dashboard' (via charts) or lineage(id). "
                "Pushdown: id, platform, domain_id."
            ),
            doc_md="Dashboards from every BI tool DataHub ingests.",
            schema=DASHBOARDS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT platform, count(*) AS dashboards, count(*) FILTER (WHERE len(owners) = 0) AS unowned "
                    "FROM datahub.main.dashboards GROUP BY platform ORDER BY dashboards DESC"
                ),
                description="Dashboards per BI tool and how many lack an owner",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per dashboard."""
        props = entity.get("properties") or {}
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "platform": platform_name(get(entity, "platform", "name")),
                "name": props.get("name"),
                "description": description_of(entity),
                "dashboard_key": entity.get("dashboardId"),
                **_governance_values(entity),
                "created_at": ts(get(props, "created", "time")),
                "updated_at": ts(get(props, "lastModified", "time")),
                "last_refreshed_at": ts(props.get("lastRefreshed")),
                "view_count": get(entity, "statsSummary", "viewCount"),
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "DASHBOARD"),
            }
        ]


CHARTS_SCHEMA = pa.schema(
    [
        *_asset_columns("Chart URN."),
        column("chart_key", pa.string(), "The tool's own chart id."),
        column("chart_type", pa.string(), "BAR, LINE, TABLE, PIE, ... when the tool reports it."),
        *_governance_columns(),
        column("created_at", TSTZ, "Creation time in the tool."),
        column("updated_at", TSTZ, "Last modification in the tool."),
        column("external_url", pa.string(), "Link to the chart in the BI tool."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class ChartsScan(EntityScan[NoArgs]):
    """Rows of ``charts``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CHARTS_SCHEMA
    OPERATION: ClassVar[str] = "Charts"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("CHART",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter(), "domain_id": FilterMap("domains")}
    SELECTION: ClassVar[str] = (
        "... on Chart { chartId platform { name } properties { name description externalUrl type "
        f"created {{ time }} lastModified {{ time }} }} editableProperties {{ description }} {GOVERNANCE_SELECTION} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "charts_scan"
        description = "BI charts / tiles / looks indexed by DataHub"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="BI Charts",
            category="assets",
            keywords="charts, tiles, looks, visualizations, bi, reports",
            doc_llm=(
                "One row per chart (tile / look / visualization): id, platform, name, description, chart_key, "
                "chart_type, owners, tags, glossary_terms, domain_id, domain, container_id, created_at, "
                "updated_at, external_url, url. The datasets a chart reads are its upstream lineage_edges. "
                "Pushdown: id, platform, domain_id."
            ),
            doc_md="Charts from every BI tool DataHub ingests.",
            schema=CHARTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT c.name, e.upstream_qualified_name FROM datahub.main.charts c "
                    "JOIN datahub.main.lineage_edges e ON e.downstream_id = c.id "
                    "WHERE e.level = 'dataset' ORDER BY c.name"
                ),
                description="The datasets each chart reads",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per chart."""
        props = entity.get("properties") or {}
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "platform": platform_name(get(entity, "platform", "name")),
                "name": props.get("name"),
                "description": description_of(entity),
                "chart_key": entity.get("chartId"),
                "chart_type": props.get("type"),
                **_governance_values(entity),
                "created_at": ts(get(props, "created", "time")),
                "updated_at": ts(get(props, "lastModified", "time")),
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "CHART"),
            }
        ]


# ----------------------------------------------------------------------------- pipelines

DATA_FLOWS_SCHEMA = pa.schema(
    [
        *_asset_columns("Data flow (pipeline) URN."),
        column("flow_key", pa.string(), "The orchestrator's pipeline id (DAG id, job name, ...)."),
        column("cluster", pa.string(), "Cluster / environment the flow runs in."),
        column("project", pa.string(), "Project the flow belongs to, when reported."),
        *_governance_columns(),
        column("external_url", pa.string(), "Link to the pipeline in the orchestrator."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DataFlowsScan(EntityScan[NoArgs]):
    """Rows of ``data_flows``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_FLOWS_SCHEMA
    OPERATION: ClassVar[str] = "DataFlows"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATA_FLOW",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter(), "domain_id": FilterMap("domains")}
    SELECTION: ClassVar[str] = (
        "... on DataFlow { flowId cluster platform { name } properties { name description project externalUrl } "
        f"editableProperties {{ description }} {GOVERNANCE_SELECTION} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "data_flows_scan"
        description = "Pipelines (DataHub data flows): Airflow DAGs, dbt projects, Spark apps, Fivetran connectors"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Pipelines",
            category="assets",
            keywords="pipelines, data flows, dags, airflow, orchestration, etl",
            doc_llm=(
                "One row per pipeline (DataHub DataFlow: an Airflow DAG, Spark application, Fivetran "
                "connector, ...): id, platform (orchestrator), name, description, flow_key, cluster, project, "
                "owners, tags, glossary_terms, domain_id, domain, container_id, external_url, url. Its tasks are "
                "data_jobs.flow_id = id. Pushdown: id, platform, domain_id."
            ),
            doc_md="Pipelines (DataHub data flows).",
            schema=DATA_FLOWS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT f.name, count(j.id) AS tasks FROM datahub.main.data_flows f "
                    "LEFT JOIN datahub.main.data_jobs j ON j.flow_id = f.id GROUP BY f.name ORDER BY tasks DESC"
                ),
                description="Tasks per pipeline",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per data flow."""
        props = entity.get("properties") or {}
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "platform": platform_name(get(entity, "platform", "name")),
                "name": props.get("name") or entity.get("flowId"),
                "description": description_of(entity),
                "flow_key": entity.get("flowId"),
                "cluster": entity.get("cluster"),
                "project": props.get("project"),
                **_governance_values(entity),
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "DATA_FLOW"),
            }
        ]


DATA_JOBS_SCHEMA = pa.schema(
    [
        *_asset_columns("Data job (task) URN."),
        column("flow_id", pa.string(), "URN of the pipeline this task belongs to (data_flows.id)."),
        column("job_key", pa.string(), "The orchestrator's task id."),
        column("input_dataset_ids", STR_LIST, "URNs of the datasets the task reads."),
        column("output_dataset_ids", STR_LIST, "URNs of the datasets the task writes."),
        *_governance_columns(),
        column("external_url", pa.string(), "Link to the task in the orchestrator."),
        column("url", pa.string(), "Link in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DataJobsScan(EntityScan[NoArgs]):
    """Rows of ``data_jobs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATA_JOBS_SCHEMA
    OPERATION: ClassVar[str] = "DataJobs"
    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATA_JOB",)
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter(), "domain_id": FilterMap("domains")}
    SELECTION: ClassVar[str] = (
        "... on DataJob { jobId jobPlatform: platform { name } dataFlow { urn } "
        "properties { name description externalUrl } "
        "editableProperties { description } inputOutput { inputDatasets { urn } outputDatasets { urn } } "
        f"{GOVERNANCE_SELECTION} }}"
    )

    class Meta:
        """Scan metadata."""

        name = "data_jobs_scan"
        description = "Pipeline tasks (DataHub data jobs) with their input and output datasets"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Pipeline Tasks",
            category="assets",
            keywords="tasks, data jobs, airflow tasks, etl steps, inputs, outputs",
            doc_llm=(
                "One row per task (DataHub DataJob): id, platform, name, description, flow_id (its pipeline, "
                "data_flows.id), job_key, input_dataset_ids and output_dataset_ids (dataset URN lists), owners, "
                "tags, glossary_terms, domain_id, domain, container_id, external_url, url. "
                "Pushdown: id, platform, domain_id."
            ),
            doc_md="Pipeline tasks with their inputs and outputs.",
            schema=DATA_JOBS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, unnest(output_dataset_ids) AS writes FROM datahub.main.data_jobs "
                    "WHERE platform = 'airflow' ORDER BY name"
                ),
                description="Which datasets each Airflow task writes",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per data job."""
        props = entity.get("properties") or {}
        io = entity.get("inputOutput") or {}
        return [
            {
                "catalog_tool": CATALOG_TOOL,
                "id": entity.get("urn"),
                "platform": platform_name(get(entity, "jobPlatform", "name")),
                "name": props.get("name") or entity.get("jobId"),
                "description": description_of(entity),
                "flow_id": get(entity, "dataFlow", "urn"),
                "job_key": entity.get("jobId"),
                "input_dataset_ids": [d["urn"] for d in io.get("inputDatasets") or [] if d and d.get("urn")],
                "output_dataset_ids": [d["urn"] for d in io.get("outputDatasets") or [] if d and d.get("urn")],
                **_governance_values(entity),
                "external_url": props.get("externalUrl"),
                "url": ep.entity_url(entity.get("urn"), "DATA_JOB"),
            }
        ]


ASSET_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("datasets", DatasetsScan, ["id"]),
    ("columns", ColumnsScan, ["dataset_id", "field_path"]),
    ("containers", ContainersScan, ["id"]),
    ("dashboards", DashboardsScan, ["id"]),
    ("charts", ChartsScan, ["id"]),
    ("data_flows", DataFlowsScan, ["id"]),
    ("data_jobs", DataJobsScan, ["id"]),
]
