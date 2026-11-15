# DataHub GraphQL schema (pinned)

These `.graphql` files are DataHub's GraphQL SDL, copied unmodified from
`datahub-graphql-core/src/main/resources/` at tag **v1.8.0** of
https://github.com/datahub-project/datahub (Apache License 2.0, Copyright the DataHub
project contributors). The mock server (`test/mock_datahub/server.py`) and the unit tests
parse every query the worker sends against this schema, so a field or argument the real
DataHub does not have fails the tests.

To move to a newer DataHub release, replace the files from the new tag and run `make check`.
