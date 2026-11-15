"""Discovery metadata for the ``datahub`` catalog: categories, docs and helpers.

DuckDB, coding agents and the ``vgi-lint`` metadata checker read these tags.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "datahub"

SOURCE_URL = "https://github.com/Query-farm/vgi-datahub"
DATAHUB_GRAPHQL_URL = "https://docs.datahub.com/docs/api/graphql/overview"

# Every object's ``vgi.category`` names one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "assets",
        "title": "Data Assets",
        "description": "Datasets and their columns, containers, dashboards, charts, pipelines and tasks.",
    },
    {
        "name": "governance",
        "title": "Governance",
        "description": "Owners, tags, glossary terms and nodes, domains, data products, users and groups.",
    },
    {
        "name": "lineage",
        "title": "Lineage",
        "description": "Dataset- and column-level lineage edges and multi-hop lineage traversal.",
    },
    {
        "name": "quality",
        "title": "Quality And Usage",
        "description": "Assertions and their run results, profiles, operations, usage statistics and incidents.",
    },
    {
        "name": "search",
        "title": "Search",
        "description": "Full-text search across every DataHub entity type with facet filters.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CATALOG_COMMENT = (
    "DataHub (open-source and DataHub Cloud) metadata catalog as SQL: datasets, columns, owners, tags, "
    "glossary, domains, lineage, assertions, profiles and usage, read through the DataHub GraphQL API."
)

CATALOG_DOC_LLM = (
    "Query a DataHub metadata catalog with SQL. DataHub (open source, and the hosted DataHub Cloud / Acryl) "
    "indexes the assets of a data platform: datasets (tables, views, topics) with schemas, dashboards, charts, "
    "pipelines and their tasks, plus governance metadata (owners, tags, glossary terms, domains, data products) "
    "and observability (lineage, assertions, profiles, operations, usage, incidents). Everything is read-only "
    "through DataHub's GraphQL API and needs one DuckDB secret of type `datahub` with GMS_URL (or FRONTEND_URL) "
    "and a personal access TOKEN; DataHub Cloud's GMS_URL is the tenant URL ending in /gms.\n\n"
    "Ids are DataHub URNs (column `id`, or `dataset_id` / `entity_id` on child tables). `qualified_name` is the "
    "lower-cased database.schema.table of a dataset and is the join key to other catalog, lineage and BI workers "
    "(dbt, OpenLineage, Looker). Equality filters on ids, platform, environment, domain, container, owner and "
    "entity type are pushed into DataHub's search, so filter on them when you can. Column names shared with the "
    "other catalog workers (catalog_tool, id, platform, qualified_name, description, created_at, updated_at, url) "
    "let you UNION catalogs."
)

CATALOG_DOC_MD = (
    "# DataHub metadata catalog\n\n"
    "Read a [DataHub](https://datahub.com) metadata graph from DuckDB: datasets and their columns, "
    "dashboards, charts and pipelines; owners, tags, glossary terms, domains and data products; dataset- "
    "and column-level lineage; assertions with their pass/fail history; profiles, operations, usage and "
    "incidents.\n\n"
    "It works with open-source DataHub and DataHub Cloud through the GraphQL API, authenticated with a "
    "personal access token kept in a `datahub` DuckDB secret (GMS_URL plus TOKEN).\n\n"
    "Columns shared with the other data-catalog workers (`catalog_tool`, `id`, `platform`, `qualified_name`, "
    "`description`, `created_at`, `updated_at`, `url`) let you UNION catalogs and join them to lineage and BI "
    "workers on `qualified_name`."
)

SCHEMA_DOC_LLM = (
    "DataHub entities as tables plus traversal and search functions. Entity tables have one row per entity with "
    "`id` = its URN; child tables hang off a dataset (`dataset_id`) or any entity (`entity_id`). Lineage is "
    "available both as direct edges (dataset and column level) and as a multi-hop traversal function. Filtering "
    "a child table on `dataset_id = '<urn>'` (or IN) fetches just those datasets instead of scrolling the whole "
    "catalog; platform, environment, domain and container equality filters become DataHub search filters; time "
    "ranges on run, profile and operation timestamps become DataHub time windows. Offline scalars build and "
    "parse dataset URNs and derive the qualified_name join key without calling DataHub."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "DataHub entities as tables: assets, governance metadata, lineage, quality and usage.\n\n"
    "Entity tables are keyed by URN (`id`); child tables reference their dataset through `dataset_id`. "
    "Filter on ids, platform, domain or environment and the filter runs inside DataHub's search.\n\n"
    "Use the traversal function for multi-hop impact analysis and the search function for keyword "
    "discovery across entity types."
)

AGENT_TEST_TASKS = [
    {
        "name": "snowflake_tables_per_schema",
        "prompt": "Using the DataHub catalog, how many Snowflake datasets are there in each schema?",
    },
    {
        "name": "pii_columns",
        "prompt": "List the qualified name and column name of every dataset column tagged 'pii' in DataHub.",
    },
    {"name": "failing_assertions", "prompt": "Which datasets in DataHub have an assertion whose latest run failed?"},
    {
        "name": "fct_orders_upstreams",
        "prompt": (
            "Using DataHub lineage, list every dataset upstream of the Snowflake table analytics.marts.fct_orders "
            "(any number of hops), by qualified name."
        ),
    },
    {"name": "schema_containers", "prompt": "List the names of the schema containers in DataHub."},
    {"name": "looker_dashboard_views", "prompt": "How many views does each Looker dashboard in DataHub have?"},
    {"name": "line_charts", "prompt": "Which charts in DataHub are line charts?"},
    {"name": "airflow_pipelines", "prompt": "List the Airflow pipelines (data flows) known to DataHub."},
    {"name": "task_outputs", "prompt": "Which datasets does the load_customers task write, according to DataHub?"},
    {
        "name": "group_owned_assets",
        "prompt": "How many assets does each DataHub group own? Show the group's display name.",
    },
    {"name": "tag_colors", "prompt": "What colour is each DataHub tag?"},
    {"name": "revenue_terms", "prompt": "Which DataHub glossary terms sit under the Revenue glossary node?"},
    {"name": "top_level_nodes", "prompt": "List the top-level glossary nodes in DataHub."},
    {"name": "domain_sizes", "prompt": "How many assets does each DataHub domain contain?"},
    {"name": "product_domains", "prompt": "Which domain does each DataHub data product belong to?"},
    {"name": "suspended_users", "prompt": "Which DataHub users are not active?"},
    {
        "name": "column_lineage_fct",
        "prompt": (
            "In DataHub column-level lineage, which upstream column feeds analytics.marts.fct_orders.gross_revenue?"
        ),
    },
    {"name": "assertion_failures", "prompt": "How many failed assertion runs has each dataset had in DataHub?"},
    {"name": "latest_row_counts", "prompt": "What is the latest profiled row count of each DataHub dataset?"},
    {"name": "last_write", "prompt": "When was each DataHub dataset last written to, according to its operations?"},
    {"name": "most_queried", "prompt": "Which dataset was queried most this month according to DataHub usage stats?"},
    {"name": "open_incidents", "prompt": "List the titles of open (active) incidents in DataHub."},
    {"name": "search_orders", "prompt": "Search DataHub for datasets matching 'orders' and list their names."},
    {
        "name": "urn_join_key",
        "prompt": (
            "Without calling DataHub, what qualified name does the URN "
            "'urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS.Marts.FCT_ORDERS,PROD)' map to?"
        ),
    },
    {
        "name": "urn_platform",
        "prompt": (
            "Using the datahub worker, what platform is in the URN "
            "'urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.ds.events,DEV)'?"
        ),
    },
    {
        "name": "build_urn",
        "prompt": ("Using the datahub worker, build the DataHub URN for the DEV BigQuery table proj.ds.events."),
    },
]

CATALOG_TAGS = {
    "vgi.title": "DataHub Metadata Catalog",
    "vgi.subtitle": "Datasets, columns, owners, glossary, lineage, assertions and usage from DataHub as SQL.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "datahub, acryl, data catalog, metadata, lineage, data governance, glossary, data quality, graphql"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
