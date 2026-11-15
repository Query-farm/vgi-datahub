<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query DataHub Datasets, Lineage, Glossary and Data Quality in DuckDB

> **vgi-datahub** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-datahub/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-datahub/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[DataHub](https://datahub.com) (open source, and the hosted DataHub Cloud / Acryl) is a
metadata catalog: it indexes your warehouse tables and their schemas, BI dashboards and
charts, pipelines and tasks, who owns them, how they are tagged and classified, how data
flows between them, and whether their quality checks pass. This worker reads that metadata
graph through DataHub's GraphQL API and exposes it as typed DuckDB tables, read-only:

- **Assets**: datasets with platform, `database.schema.table` and governance metadata;
  their columns with types, nullability, keys and column-level tags / glossary terms;
  containers, dashboards, charts, pipelines (data flows) and tasks (data jobs).
- **Governance**: owners (users and groups, by ownership type), tags, glossary terms and
  nodes, domains, data products, users and groups.
- **Lineage**: direct upstream → downstream edges at dataset and column level, and a
  multi-hop `lineage()` traversal for impact analysis.
- **Quality and usage**: assertions with their latest status, assertion run history,
  dataset profiles, write operations, usage statistics and incidents.
- **Search**: DataHub's search bar as a table function, with tag / glossary term / owner
  filters.

## Quick start

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'datahub' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-datahub@v0.1.0 vgi-datahub-worker');

CREATE SECRET datahub (TYPE datahub, GMS_URL 'http://localhost:8080', TOKEN '<personal access token>');

-- Snowflake tables nobody owns.
SELECT qualified_name FROM datahub.main.datasets
WHERE platform = 'snowflake' AND len(owners) = 0 ORDER BY qualified_name;

-- Columns tagged PII.
SELECT qualified_name, column_name FROM datahub.main.columns WHERE list_contains(tags, 'pii');

-- What breaks if this raw table changes?
SELECT entity_type, name, degree
FROM datahub.main.lineage(datahub.main.dataset_urn('snowflake', 'analytics.raw.orders'),
                          direction := 'downstream', max_hops := 5)
ORDER BY degree;

-- Failing quality checks.
SELECT qualified_name, assertion_type, description, last_run_at
FROM datahub.main.assertions WHERE last_status = 'failed';
```

With DuckDB 2.0 instead of Haybarn, load the extension from the Query.Farm repository:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
```

From a source checkout: `make build`, then
`LOCATION '/abs/path/to/vgi-datahub/.venv/bin/vgi-datahub-worker'`.

## Connect to DataHub

Every table and the `search` / `lineage` functions read one secret of type `datahub`:

| Field | Meaning |
|---|---|
| `GMS_URL` | DataHub's metadata service (GMS) root. Self-hosted: `http://localhost:8080`. DataHub Cloud: `https://<tenant>.acryl.io/gms`. GraphQL goes to `<GMS_URL>/api/graphql`. |
| `FRONTEND_URL` | The DataHub UI root (e.g. `http://localhost:9002`). Used to build each row's `url`; if `GMS_URL` is unset, GraphQL goes to `<FRONTEND_URL>/api/v2/graphql`. For a DataHub Cloud `.../gms` URL the frontend is derived automatically. |
| `TOKEN` | A personal access token (DataHub UI → Settings → Access Tokens), sent as `Authorization: Bearer`. Redacted in `duckdb_secrets()`; never logged or stored in scan state. |
| `PAGE_SIZE` | Entities per scroll page (1–1000, default 100). |

```sql
CREATE SECRET datahub (TYPE datahub, GMS_URL 'https://acme.acryl.io/gms', TOKEN '...');
```

Keep one `datahub` secret per session (`CREATE OR REPLACE SECRET` to switch servers).
`429 Too Many Requests` and 5xx responses are retried with exponential backoff, honouring
`Retry-After`.

## Tables and functions

All objects are in `datahub.main`.

| Object | One row per | Pushdown |
|---|---|---|
| `datasets` | dataset: platform, instance, environment, database / schema / name, `qualified_name`, subtype, description, owners / tags / glossary_terms lists, domain, container, deprecated, custom properties, created / updated / ingested times, links | `id` (= / IN, fetched by URN), `platform`, `environment`, `domain_id`, `container_id` |
| `columns` | schema field: name (nested fields dotted), raw field path, position, DataHub and native type, nullable, primary key, description, tags, glossary terms (ingested and UI-edited merged) | `dataset_id`, `platform` |
| `containers` | database / schema / project / folder, with its root-first path | `id`, `platform`, `parent_container_id`, `domain_id` |
| `dashboards`, `charts` | BI dashboard / chart with owners, tags, domain, refresh and view counts, links | `id`, `platform`, `domain_id` |
| `data_flows`, `data_jobs` | pipeline / task (Airflow DAG and task, ...); tasks list their input and output dataset URNs | `id`, `platform`, `domain_id` |
| `owners` | (entity, owner) across datasets, dashboards, charts, pipelines, tasks, containers, domains, data products, glossary and tags; owner type (user / group) and ownership type | `entity_id`, `entity_type`, `owner_id` |
| `tags`, `glossary_terms`, `glossary_nodes` | tag / term / node definition (terms with definition, source and parent path) | `id` (terms also `domain_id`) |
| `domains`, `data_products` | domain (with parent and asset count) / data product (with domain and asset count) | `id` (products also `domain_id`) |
| `users`, `groups` | DataHub user / group | `id` |
| `lineage_edges` | direct edge: `level` `'dataset'` (entity → entity, with relationship) or `'column'` (column → column, with transformation and query) | `downstream_id`, `downstream_type`, `downstream_platform` |
| `assertions` | assertion (native monitors, dbt / Great Expectations tests) with its latest status | `dataset_id`, `platform` |
| `assertion_results` | assertion run: status, observed value, row counts, native results | `run_at` range → DataHub time window, `dataset_id`, `platform` |
| `dataset_profiles` | profile: row / column counts, size, per-column statistics | `profiled_at` range → time window, `dataset_id`, `platform` |
| `operations` | reported write (INSERT, UPDATE, DDL, ...) with actor and rows affected | `reported_at` range → time window, `dataset_id`, `platform` |
| `usage` | dataset with usage stats: query count, distinct users, top users, top columns, 30-day summary | `time_range` (one value; default `MONTH`), `dataset_id`, `platform` |
| `incidents` | incident on a dataset: type, title, state, stage, priority, source, times | `state`, `dataset_id`, `platform` |
| `search(query, tag :=, glossary_term :=, owner :=)` | search hit across datasets, dashboards, charts, pipelines, tasks, containers, domains, data products, glossary, tags, users, groups | the named arguments, `entity_type`, `platform`, `domain_id` |
| `lineage(id, direction := 'upstream', max_hops := 3)` | entity reachable from `id`, with `degree` and URN `paths` | `entity_type`, `platform`; `max_hops` → DataHub's `degree` filter |
| `dataset_qualified_name(urn)` | scalar, offline: the lower-cased `database.schema.table` join key of a dataset URN | — |
| `parse_dataset_urn(urn)` | scalar, offline: `STRUCT(platform, name, environment, database, schema, table_name, qualified_name)` | — |
| `dataset_urn(platform, name[, environment])` | scalar, offline: build a dataset URN (environment must be a constant) | — |

**How pushdown works.** Equality / `IN` filters on the columns above become DataHub search
`orFilters` (`platform`, `origin`, `domains`, `container`, `owners`, `tags`,
`glossaryTerms`) or the search `types`; an `=` / `IN` on the table's id column skips search
entirely and fetches those URNs with `entities(urns:)`. Child tables (`columns`,
`assertions`, `assertion_results`, `dataset_profiles`, `operations`, `usage`, `incidents`)
are read through their dataset, so `WHERE dataset_id = '<urn>'` turns a catalog-wide scroll
into one request. Time ranges become the `startTimeMillis` / `endTimeMillis` of DataHub's
timeseries queries. DuckDB re-applies every predicate, so pushdown only narrows what is
fetched. Without a time filter the timeseries tables read the newest 100 events per dataset
(per assertion for `assertion_results`); with one, up to 1000.

**Pagination.** Scans use `scrollAcrossEntities` / `scrollAcrossLineage`; the scroll id and
the resolved plan live in the scan's state, so a long scan resumes on any worker process
over the HTTP transport.

## Shared data-catalog vocabulary

`vgi-datahub` is the first of the Query.Farm data-catalog workers; `vgi-openmetadata` and
`vgi-unity-catalog` use the same table names and columns so catalogs can be `UNION ALL`-ed
and joined to the lineage and BI workers.

**Common columns** (every entity table):

| Column | Type | Meaning |
|---|---|---|
| `catalog_tool` | `VARCHAR NOT NULL` | `'datahub'`, `'openmetadata'`, `'unity_catalog'` |
| `id` | `VARCHAR NOT NULL` | the tool's stable identifier (DataHub: the URN; OpenMetadata: the entity UUID or FQN; Unity Catalog: the full name) |
| `platform` | `VARCHAR` | source system, lower-case (`snowflake`, `bigquery`, `postgres`, `kafka`, `looker`, `airflow`, `databricks`, ...) |
| `qualified_name` | `VARCHAR` | datasets: lower-cased `database.schema.table` (fewer parts when the platform has fewer; full lower-cased name for topics / paths). The join key to `vgi-dbt`, `vgi-openlineage` and `vgi-looker` (`explore_tables`) |
| `description` | `VARCHAR` | the curated (UI-edited) description first, else the ingested one |
| `created_at`, `updated_at` | `TIMESTAMPTZ` | as reported by the source platform |
| `url` | `VARCHAR` | link to the entity in the catalog's UI |

Child tables reference their parent as `dataset_id` (or `entity_id` + `entity_type` for
owners) instead of `id`, and repeat `platform` and `qualified_name`.

**Tables** (columns beyond the common ones that siblings should match):

| Table | Key columns |
|---|---|
| `datasets` | `environment`, `database`, `schema`, `name`, `subtype`, `owners` / `tags` / `glossary_terms` (`VARCHAR[]`), `domain_id`, `domain`, `container_id`, `deprecated` |
| `columns` | `dataset_id`, `column_name`, `ordinal_position` (1-based), `data_type`, `native_data_type`, `nullable`, `is_primary_key`, `tags`, `glossary_terms` |
| `owners` | `entity_id`, `entity_type`, `entity_name`, `owner_id`, `owner_type` (`'user'` / `'group'`), `owner_name`, `owner_email`, `ownership_type` |
| `tags`, `glossary_terms` | definitions: `id`, `name`, `description` (terms: `parent_node_id`, `parent_path`) |
| `domains` | `id`, `name`, `description`, `parent_domain_id`, `asset_count` |
| `lineage_edges` | `level` (`'dataset'` / `'column'`), `upstream_id`, `upstream_type`, `upstream_qualified_name`, `upstream_column`, `downstream_id`, `downstream_type`, `downstream_qualified_name`, `downstream_column`, `transformation` |
| `assertions` | data-quality tests: `id`, `dataset_id`, `assertion_type`, `column_names`, `last_status`, `last_run_at` |
| `assertion_results` | test runs: `assertion_id`, `dataset_id`, `run_at`, `status` (`'passed'` / `'failed'` / `'error'` / `'init'`), `actual_value`, `row_count`, `unexpected_count` |
| `usage` | `dataset_id`, `time_range`, `query_count`, `unique_user_count`, `top_users`, `top_columns` |

Entity types are lower-case snake (`dataset`, `dashboard`, `chart`, `data_flow`, `data_job`,
`container`, `glossary_term`, ...).

```sql
-- dbt models whose DataHub dataset has a failing assertion.
SELECT m.unique_id, a.description
FROM dbt.main.nodes('target/manifest.json') m
JOIN datahub.main.assertions a
  ON a.qualified_name = lower(concat_ws('.', m.db_name, m.db_schema, m.alias))
WHERE m.resource_type = 'model' AND a.last_status = 'failed';
```

## Testing

`make check` runs ruff / mypy / pydoclint, the unit tests, the SQL suite over the stdio,
launch and HTTP transports, and the metadata linter (score 100, zero findings at `info`).

DataHub's quickstart (Kafka, Elasticsearch, MySQL, GMS) is far too heavy for CI, so the
tests run against a **strict replay server**, `test/mock_datahub/server.py`:

- it parses and validates every incoming query against **DataHub's own GraphQL schema**,
  vendored unmodified from DataHub **v1.8.0** in `test/datahub_schema/`, and executes it
  with graphql-core over GraphQL-shaped records in `test/fixtures/datahub.json`
  (regenerate with `test/fixtures/build_fixture.py`) — an unknown field, argument or
  mistyped variable fails exactly as on a real DataHub;
- it only accepts the worker's named operations, and only facet fields DataHub indexes;
- one instance answers the first sighting of every third request with `429 Retry-After`,
  one requires a bearer token, and one refuses unfiltered dataset scans, so the SQL suite
  proves pushdown happened; `GET /__requests` exposes the received operations and
  variables to the unit tests.

The unit tests also validate every query document against the pinned schema directly. CI
needs no network beyond package installs.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Licensed under the BSD Zero Clause License (0BSD); see [LICENSE](LICENSE). The vendored
DataHub GraphQL schema in `test/datahub_schema/` is Apache-2.0, from the DataHub project.
