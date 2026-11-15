# Changelog

## 0.1.0 (unreleased)

First release: DataHub (open-source and DataHub Cloud) metadata as DuckDB SQL through the
GraphQL API. 22 read-only tables (assets, governance, lineage, quality and usage), the
`search` and `lineage` table functions and offline URN scalars; filter, id and time-window
pushdown; externalized scroll cursors; 429 / 5xx retries honouring `Retry-After`. Tested
against a strict replay server that validates every query against DataHub v1.8.0's
GraphQL schema.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
