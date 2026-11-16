# Changelog

## 0.1.3 (2026-11-16)

- Move to Python 3.14: `requires-python >=3.14`, CI, the Docker image and lint tooling target 3.14.

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.

## 0.1.1

- Fix: every query whose filter is pushed down to the worker (a `WHERE` clause, or
  `ORDER BY … LIMIT`) failed with "No DuckDB-compatible engine is installed". The SDK binds
  and evaluates pushed predicates with a local engine, which was only in the dev
  dependency group; the runtime dependency is now `vgi-python[http,haybarn]`.

## 0.1.0

First release: DataHub (open-source and DataHub Cloud) metadata as DuckDB SQL through the
GraphQL API. 22 read-only tables (assets, governance, lineage, quality and usage), the
`search` and `lineage` table functions and offline URN scalars; filter, id and time-window
pushdown; externalized scroll cursors; 429 / 5xx retries honouring `Retry-After`. Tested
against a strict replay server that validates every query against DataHub v1.8.0's
GraphQL schema.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
