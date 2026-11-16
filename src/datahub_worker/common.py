"""Shared building blocks: Arrow types, batch building, catalog tables, pushdown and URN helpers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter
from vgi.table_function import TableFunctionGenerator

CATALOG_TOOL = "datahub"

TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
STR_MAP = pa.map_(pa.string(), pa.string())


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


def rows_to_batch(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build a batch of ``schema`` (the projected output schema) from row dicts."""
    columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
    return pa.RecordBatch.from_pydict(columns, schema=schema)


def duckdb_type(t: pa.DataType) -> str:
    """Return the DuckDB SQL name of an Arrow type used in this worker's schemas."""
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "VARCHAR"
    if pa.types.is_int64(t):
        return "BIGINT"
    if pa.types.is_int32(t):
        return "INTEGER"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_map(t):
        return f"MAP({duckdb_type(t.key_type)}, {duckdb_type(t.item_type)})"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return (
            "STRUCT("
            + ", ".join(f"{t.field(i).name} {duckdb_type(t.field(i).type)}" for i in range(t.num_fields))
            + ")"
        )
    raise TypeError(f"no DuckDB name for {t}")


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON for a fixed output schema."""
    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


def catalog_table(
    name: str,
    function: type[TableFunctionGenerator[Any, Any]],
    *,
    comment: str,
    tags: dict[str, str],
    schema_path: list[str],
    primary_key: list[str] | None = None,
) -> Table:
    """Build a catalog table over ``function`` with NOT NULL records for its non-nullable columns."""
    fields: pa.Schema = function.FIXED_SCHEMA  # type: ignore[attr-defined]
    constraints = [
        constraint_record(
            schema_path,
            name,
            f"{f.name}_not_null",
            "NOT_NULL",
            [[f.name]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        )
        for f in fields
        if not f.nullable
    ]
    if primary_key:
        constraints.append(
            constraint_record(
                schema_path,
                name,
                "pk",
                "PRIMARY_KEY",
                [[c] for c in primary_key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    comments = {
        f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in fields if (f.metadata or {}).get(b"comment")
    }
    return Table(
        name=name,
        function=function,
        comment=comment,
        tags=tags,
        constraints=tuple(constraints),
        column_comments=comments,
    )


# ----------------------------------------------------------------------------- pushdown helpers


def _scalar(value: Any) -> Any:
    return value.as_py() if hasattr(value, "as_py") else value


def eq_values(filters: Any, column: str) -> list[Any] | None:
    """Values ``column`` is constrained to by top-level ``=`` / ``IN`` filters, else None.

    Several such filters on one column intersect; an empty list means no row matches.
    """
    found: set[Any] | None = None
    for f in filters.filters if filters else ():
        if f.column_name != column:
            continue
        if isinstance(f, ConstantFilter) and f.op is ComparisonOp.EQ:
            values = {_scalar(f.value)}
        elif isinstance(f, InFilter):
            values = {v for v in f.values.to_pylist() if v is not None}
        else:
            continue
        found = values if found is None else found & values
    return None if found is None else sorted(found, key=str)


def range_bounds(filters: Any, column: str) -> tuple[Any, Any]:
    """Loosest-safe ``(lower, upper)`` bounds from top-level comparisons on ``column``.

    Both bounds are inclusive; the SDK re-applies the exact predicates afterwards.
    """
    lower: Any = None
    upper: Any = None
    for f in filters.filters if filters else ():
        if f.column_name != column or not isinstance(f, ConstantFilter):
            continue
        value = _scalar(f.value)
        if value is None:
            continue
        if f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ):
            lower = value if lower is None else max(lower, value)
        if f.op in (ComparisonOp.LT, ComparisonOp.LE, ComparisonOp.EQ):
            upper = value if upper is None else min(upper, value)
    return lower, upper


def to_millis(value: Any) -> int | None:
    """Epoch milliseconds for a pushed-down timestamp/date bound (None passes through)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    if hasattr(value, "year"):  # date
        return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp() * 1000)
    return None


def ts(millis: Any) -> datetime | None:
    """Epoch milliseconds (DataHub's ``Long`` times) to an aware UTC datetime; 0/None -> None."""
    if millis is None or millis == 0:
        return None
    try:
        return datetime.fromtimestamp(int(millis) / 1000.0, tz=UTC)
    except TypeError, ValueError, OverflowError, OSError:
        return None


# ----------------------------------------------------------------------------- URNs and names

# Platforms whose dataset names are dotted relational paths: [database.]schema.table.
RELATIONAL_PLATFORMS = frozenset(
    {
        "athena",
        "azure-sql",
        "bigquery",
        "clickhouse",
        "cockroachdb",
        "databricks",
        "db2",
        "dbt",
        "delta-lake",
        "doris",
        "dremio",
        "druid",
        "duckdb",
        "exasol",
        "fivetran",
        "glue",
        "greenplum",
        "hana",
        "hive",
        "iceberg",
        "impala",
        "mariadb",
        "mssql",
        "mysql",
        "netezza",
        "oracle",
        "postgres",
        "presto",
        "redshift",
        "singlestore",
        "snowflake",
        "sqlite",
        "starrocks",
        "synapse",
        "teradata",
        "trino",
        "unity-catalog",
        "vertica",
    }
)

_DATASET_URN = re.compile(r"^urn:li:dataset:\(urn:li:dataPlatform:([^,]+),(.*),([A-Za-z_]+)\)$")
_V2_TOKEN = re.compile(r"\[[^\]]*\]")


@dataclass(frozen=True, slots=True)
class DatasetName:
    """A dataset's platform, environment and the parts of its (instance-stripped) name."""

    platform: str | None
    environment: str | None
    full_name: str | None
    database: str | None
    schema: str | None
    table: str | None

    @property
    def qualified_name(self) -> str | None:
        """Lower-cased ``database.schema.table`` (fewer parts when the platform has fewer)."""
        if self.table is None:
            return self.full_name.lower() if self.full_name else None
        return ".".join(p for p in (self.database, self.schema, self.table) if p).lower()


def parse_dataset_urn(urn: str | None) -> tuple[str | None, str | None, str | None]:
    """``(platform, name, environment)`` from a dataset URN, or Nones when it is not one."""
    m = _DATASET_URN.match(urn or "")
    if not m:
        return None, None, None
    return m.group(1), m.group(2), m.group(3)


def platform_name(value: str | None) -> str | None:
    """``urn:li:dataPlatform:snowflake`` -> ``snowflake`` (plain names pass through)."""
    if not value:
        return None
    return value.removeprefix("urn:li:dataPlatform:")


def split_dataset_name(
    platform: str | None, name: str | None, environment: str | None = None, instance: str | None = None
) -> DatasetName:
    """Split a dataset name into database / schema / table for relational platforms.

    A platform-instance prefix (``<instance>.db.schema.table``) is dropped first. Names on
    non-relational platforms (Kafka topics, S3 paths, ...) keep only ``full_name``.
    """
    if name and instance and name.startswith(instance + "."):
        name = name[len(instance) + 1 :]
    if not name or (platform or "") not in RELATIONAL_PLATFORMS:
        return DatasetName(platform, environment, name, None, None, None)
    parts = name.split(".")
    if len(parts) >= 3:
        return DatasetName(platform, environment, name, parts[0], parts[1], ".".join(parts[2:]))
    if len(parts) == 2:
        return DatasetName(platform, environment, name, None, parts[0], parts[1])
    return DatasetName(platform, environment, name, None, None, parts[0])


def dataset_name_from_urn(urn: str | None, instance: str | None = None) -> DatasetName:
    """:func:`split_dataset_name` for a dataset URN."""
    platform, name, env = parse_dataset_urn(urn)
    return split_dataset_name(platform, name, env, instance)


def simple_field_path(field_path: str | None) -> str | None:
    """Simplify a v2 field path (``[version=2.0].[type=struct].address.[type=string].city`` -> ``address.city``)."""
    if field_path is None:
        return None
    if not field_path.startswith("["):
        return field_path
    parts = [p for p in _V2_TOKEN.sub("", field_path).split(".") if p]
    return ".".join(parts) or field_path


def as_urn(value: str, prefix: str) -> str:
    """``value`` if it already is a URN, else ``prefix + value`` (e.g. a tag name to its URN)."""
    return value if value.startswith("urn:li:") else prefix + value


def entity_type_name(graphql_type: str | None) -> str | None:
    """GraphQL ``EntityType`` (``DATA_JOB``) to this worker's lower-case form (``data_job``)."""
    return graphql_type.lower() if graphql_type else None


def get(obj: Any, *path: str) -> Any:
    """Walk nested dicts, returning None as soon as a step is missing."""
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def custom_properties(entries: Any) -> list[tuple[str, str | None]] | None:
    """``[{key, value}]`` to MAP entries (None when absent)."""
    if not entries:
        return None
    return [(str(e.get("key")), e.get("value")) for e in entries if isinstance(e, dict) and e.get("key") is not None]
