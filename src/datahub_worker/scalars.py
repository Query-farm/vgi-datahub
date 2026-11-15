"""Offline URN scalars: build and take apart DataHub dataset URNs without calling DataHub.

They apply the same naming rules as the tables, so ``dataset_qualified_name(urn)`` equals
``datasets.qualified_name`` and joins to the other catalog / lineage / BI workers.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .common import dataset_name_from_urn
from .meta import object_tags

URN_DOC = "The DataHub dataset URN to read (datasets.id, lineage_edges.upstream_id, ...); NULL gives NULL."
FCT_ORDERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS.Marts.FCT_ORDERS,PROD)"

URN_PARTS = pa.struct(
    [
        pa.field("platform", pa.string()),
        pa.field("name", pa.string()),
        pa.field("environment", pa.string()),
        pa.field("database", pa.string()),
        pa.field("schema", pa.string()),
        pa.field("table_name", pa.string()),
        pa.field("qualified_name", pa.string()),
    ]
)


class DatasetQualifiedName(ScalarFunction):
    """``dataset_qualified_name(urn)``."""

    class Meta:
        """Function metadata."""

        name = "dataset_qualified_name"
        description = "The lower-cased database.schema.table join key of a DataHub dataset URN"
        tags = {
            **object_tags(
                title="Dataset Qualified Name",
                category="lineage",
                keywords="urn, qualified name, join key, dataset name, normalize, table name",
                doc_llm=(
                    "Return the cross-worker join key of a dataset URN: the lower-cased 'database.schema.table' for "
                    "relational platforms (snowflake, bigquery, postgres, dbt, ...; fewer parts when the name has "
                    "fewer), else the lower-cased full name (Kafka topics, S3 paths). Same rule as "
                    "datasets.qualified_name, so use it to join URNs from lineage_edges / lineage() or your own data "
                    "to dbt, OpenLineage and Looker tables. Runs locally: no DataHub call, no secret needed. Not a "
                    "dataset URN or NULL gives NULL."
                ),
                doc_md=(
                    "## dataset_qualified_name(urn)\n\n"
                    "The lower-cased `database.schema.table` of a dataset URN.\n\n"
                    "Offline; NULL for anything that is not a dataset URN."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "The join key of a Snowflake table URN and of a Kafka topic URN",
                        "sql": (
                            f"SELECT datahub.main.dataset_qualified_name('{FCT_ORDERS}') AS snowflake_table, "
                            "datahub.main.dataset_qualified_name("
                            "'urn:li:dataset:(urn:li:dataPlatform:kafka,Orders.Events,PROD)') AS kafka_topic"
                        ),
                        "expected_result": [["analytics.marts.fct_orders", "orders.events"]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT datahub.main.dataset_qualified_name(upstream_id) AS source, count(*) AS consumers "
                    "FROM datahub.main.lineage_edges WHERE level = 'dataset' AND upstream_type = 'dataset' "
                    "GROUP BY ALL ORDER BY consumers DESC"
                ),
                description="Most-consumed upstream datasets by qualified name",
            ),
        ]

    @classmethod
    def compute(cls, urn: Annotated[pa.StringArray, Param(doc=URN_DOC)]) -> Annotated[pa.StringArray, Returns()]:
        """Qualified name of every URN."""
        values = [
            None if u is None else dataset_name_from_urn(u).qualified_name if u else None for u in urn.to_pylist()
        ]
        return pa.array(values, type=pa.string())


class ParseDatasetUrn(ScalarFunction):
    """``parse_dataset_urn(urn)``."""

    class Meta:
        """Function metadata."""

        name = "parse_dataset_urn"
        description = "Split a DataHub dataset URN into platform, name, environment, database, schema and table"
        tags = {
            **object_tags(
                title="Parse Dataset URN",
                category="lineage",
                keywords="urn, parse, platform, environment, database, schema, table, dataset identifier",
                doc_llm=(
                    "Split a dataset URN 'urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)' into a "
                    "`STRUCT(platform, name, environment, database, schema, table_name, qualified_name)`. database / "
                    "schema / table_name are filled for relational platforms only. Offline (no DataHub call). NULL "
                    "for NULL or a non-dataset URN. Access fields with `.platform`, `.qualified_name`, ..."
                ),
                doc_md=(
                    "## parse_dataset_urn(urn)\n\n"
                    "The parts of a dataset URN as a struct.\n\n"
                    "Offline; NULL for anything that is not a dataset URN."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Platform, environment and schema of a Snowflake table URN",
                        "sql": (
                            f"SELECT p.platform, p.environment, p.schema FROM (SELECT datahub.main.parse_dataset_urn("
                            f"'{FCT_ORDERS}') AS p)"
                        ),
                        "expected_result": [["snowflake", "PROD", "Marts"]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT datahub.main.parse_dataset_urn(upstream_id).platform AS platform, count(*) AS edges "
                    "FROM datahub.main.lineage_edges WHERE upstream_type = 'dataset' GROUP BY ALL ORDER BY edges DESC"
                ),
                description="Lineage edges per upstream platform",
            ),
        ]

    @classmethod
    def compute(
        cls, urn: Annotated[pa.StringArray, Param(doc=URN_DOC)]
    ) -> Annotated[pa.StructArray, Returns(arrow_type=URN_PARTS)]:
        """Parts of every URN."""
        out: list[dict[str, str | None] | None] = []
        for u in urn.to_pylist():
            parsed = dataset_name_from_urn(u) if u else None
            if parsed is None or parsed.platform is None:
                out.append(None)
                continue
            out.append(
                {
                    "platform": parsed.platform,
                    "name": parsed.full_name,
                    "environment": parsed.environment,
                    "database": parsed.database,
                    "schema": parsed.schema,
                    "table_name": parsed.table,
                    "qualified_name": parsed.qualified_name,
                }
            )
        return pa.array(out, type=URN_PARTS)  # type: ignore[return-value]


def _make_urns(platforms: list[str | None], names: list[str | None], envs: list[str | None]) -> pa.StringArray:
    values = [
        None
        if p is None or n is None or e is None
        else f"urn:li:dataset:(urn:li:dataPlatform:{p.removeprefix('urn:li:dataPlatform:')},{n},{e.upper()})"
        for p, n, e in zip(platforms, names, envs, strict=True)
    ]
    return pa.array(values, type=pa.string())


_PLATFORM_DOC = "Platform name, e.g. 'snowflake' (or its urn:li:dataPlatform: URN); NULL in, NULL out."
_NAME_DOC = "Dataset name as DataHub stores it, e.g. 'analytics.marts.fct_orders'; NULL in, NULL out."


class DatasetUrn(ScalarFunction):
    """``dataset_urn(platform, name)``."""

    class Meta:
        """Function metadata."""

        name = "dataset_urn"
        description = "Build a PROD DataHub dataset URN from a platform and a dataset name"
        tags = {
            **object_tags(
                title="Build Dataset URN",
                category="lineage",
                keywords="urn, build urn, dataset id, platform, lookup, construct identifier",
                doc_llm=(
                    "Build 'urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,PROD)', e.g. to look a table up "
                    "by id (`WHERE id = datahub.main.dataset_urn('snowflake', 'db.schema.table')`, which pushes "
                    "down) or to call lineage(). Names keep their case: DataHub stores most warehouse names "
                    "lower-case. dataset_urn(platform, name, environment) picks another environment. Offline."
                ),
                doc_md=(
                    "## dataset_urn(platform, name)\n\n"
                    "The PROD dataset URN for a platform and name.\n\n"
                    "Use the three-argument form for another environment."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "The URN of a Snowflake table",
                        "sql": "SELECT datahub.main.dataset_urn('snowflake', 'analytics.marts.fct_orders') AS urn",
                        "expected_result": [
                            ["urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.marts.fct_orders,PROD)"]
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT column_name, native_data_type FROM datahub.main.columns WHERE dataset_id = "
                    "datahub.main.dataset_urn('snowflake', 'analytics.marts.fct_orders') ORDER BY ordinal_position"
                ),
                description="A table's columns, looked up by platform and name",
            ),
        ]

    @classmethod
    def compute(
        cls,
        platform: Annotated[pa.StringArray, Param(doc=_PLATFORM_DOC)],
        name: Annotated[pa.StringArray, Param(doc=_NAME_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """URN of every (platform, name)."""
        names = name.to_pylist()
        return _make_urns(platform.to_pylist(), names, ["PROD"] * len(names))


class DatasetUrnInEnvironment(ScalarFunction):
    """``dataset_urn(platform, name, environment)``."""

    class Meta:
        """Function metadata."""

        name = "dataset_urn"
        description = "Build a DataHub dataset URN from a platform, a dataset name and an environment"
        tags = {
            **object_tags(
                title="Build Dataset URN (Environment)",
                category="lineage",
                keywords="urn, build urn, dataset id, environment, fabric type, dev, prod",
                doc_llm=(
                    "Build 'urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<ENVIRONMENT>)'. environment must "
                    "be a constant; it is upper-cased (DataHub FabricType: PROD, DEV, QA, STG, ...). Offline."
                ),
                doc_md=(
                    "## dataset_urn(platform, name, environment)\n\n"
                    "The dataset URN in a given environment.\n\n"
                    "`dataset_urn('bigquery', 'proj.ds.t', 'dev')` ends in `,DEV)`."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "The URN of a BigQuery table in DEV",
                        "sql": "SELECT datahub.main.dataset_urn('bigquery', 'proj.ds.events', 'dev') AS urn",
                        "expected_result": [["urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.ds.events,DEV)"]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, environment FROM datahub.main.datasets "
                    "WHERE id = datahub.main.dataset_urn('bigquery', 'proj.ds.events', 'dev')"
                ),
                description="Look up a DEV table by platform and name",
            ),
        ]

    @classmethod
    def compute(
        cls,
        platform: Annotated[pa.StringArray, Param(doc=_PLATFORM_DOC)],
        name: Annotated[pa.StringArray, Param(doc=_NAME_DOC)],
        environment: Annotated[
            str, ConstParam("Environment (DataHub FabricType), a constant such as 'PROD' or 'dev'; case-insensitive.")
        ],
    ) -> Annotated[pa.StringArray, Returns()]:
        """URN of every (platform, name) in ``environment``."""
        names = name.to_pylist()
        return _make_urns(platform.to_pylist(), names, [environment] * len(names))


SCALARS: list[type[ScalarFunction]] = [DatasetQualifiedName, ParseDatasetUrn, DatasetUrn, DatasetUrnInEnvironment]
