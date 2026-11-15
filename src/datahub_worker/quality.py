"""Quality and usage tables: assertions, assertion_results, dataset_profiles, operations, usage, incidents.

All hang off datasets: ``dataset_id = / IN`` fetches just those datasets, ``platform``
narrows the dataset scroll, and each table's time column pushes down as the DataHub
time window (``startTimeMillis`` / ``endTimeMillis``) of the nested timeseries query.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .assets import DATASET_IDENTITY, catalog_tool_column, dataset_identity, scan_tags
from .client import SECRET_REQUIREMENT, DataHubError, Endpoint
from .common import CATALOG_TOOL, STR_LIST, STR_MAP, TSTZ, NoArgs, custom_properties, eq_values, get, range_bounds
from .common import to_millis as _ms
from .common import ts as _ts
from .meta import column
from .scan import EntityScan, FilterMap, platform_filter

DEFAULT_EVENT_LIMIT = 100  # newest timeseries events per dataset / assertion without a time filter
WINDOW_EVENT_LIMIT = 1000  # ... with a pushed-down time window

RESULT_STATUS = {"SUCCESS": "passed", "FAILURE": "failed", "ERROR": "error", "INIT": "init"}
TIME_WINDOW_VARS = ", $startTimeMillis: Long, $endTimeMillis: Long, $limit: Int"


def time_window(params: ProcessParams[Any], column_name: str) -> dict[str, Any]:
    """``startTimeMillis`` / ``endTimeMillis`` / ``limit`` variables from range filters on ``column_name``."""
    lower, upper = range_bounds(params.current_pushdown_filters, column_name)
    start, end = _ms(lower), _ms(upper)
    if end is not None:
        end += 1  # inclusive upper bound; the SDK re-applies the exact predicate
    limit = WINDOW_EVENT_LIMIT if start is not None or end is not None else DEFAULT_EVENT_LIMIT
    return {"startTimeMillis": start, "endTimeMillis": end, "limit": limit}


class DatasetChildScan(EntityScan[NoArgs]):
    """A scan over datasets that emits rows for something nested in each dataset."""

    ENTITY_TYPES: ClassVar[tuple[str, ...]] = ("DATASET",)
    ID_COLUMN: ClassVar[str | None] = "dataset_id"
    FILTERS: ClassVar[dict[str, FilterMap]] = {"platform": platform_filter()}


def _dataset_columns() -> list[pa.Field[Any]]:
    return [
        column("dataset_id", pa.string(), "URN of the dataset (datasets.id).", nullable=False),
        column("platform", pa.string(), "Data platform of the dataset."),
        column("qualified_name", pa.string(), "Lower-cased database.schema.table of the dataset."),
    ]


def _dataset_values(entity: dict[str, Any]) -> dict[str, Any]:
    ident = dataset_identity(entity)
    return {
        "catalog_tool": CATALOG_TOOL,
        "dataset_id": entity.get("urn"),
        "platform": ident["platform"],
        "qualified_name": ident["qualified_name"],
    }


# ----------------------------------------------------------------------------- assertions

ASSERTIONS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Assertion URN.", nullable=False),
        *_dataset_columns(),
        column("assertion_type", pa.string(), "FRESHNESS, VOLUME, FIELD, SQL, DATASET, DATA_SCHEMA or CUSTOM."),
        column("description", pa.string(), "Description of what the assertion checks."),
        column(
            "source_type", pa.string(), "NATIVE (DataHub-run), EXTERNAL (dbt, Great Expectations, ...) or INFERRED."
        ),
        column("assertion_platform", pa.string(), "Platform that evaluates it, e.g. 'dbt', 'great-expectations'."),
        column("column_names", STR_LIST, "Columns the assertion is about (field / column assertions)."),
        column("operator", pa.string(), "Comparison operator, e.g. GREATER_THAN, NOT_NULL, BETWEEN, _NATIVE_."),
        column("native_type", pa.string(), "The evaluating tool's own assertion type (e.g. dbt test name)."),
        column("last_status", pa.string(), "Latest run: 'passed', 'failed', 'error', 'init' or NULL if never run."),
        column("last_run_at", TSTZ, "When the latest run happened."),
        column("external_url", pa.string(), "Link to the assertion in the evaluating tool."),
        column("url", pa.string(), "Link to the dataset in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class AssertionsScan(DatasetChildScan):
    """Rows of ``assertions``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ASSERTIONS_SCHEMA
    OPERATION: ClassVar[str] = "Assertions"
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} assertions(start: 0, count: 1000) {{ assertions {{ urn "
        "platform { name } info { type description externalUrl source { type } "
        "datasetAssertion { scope operator nativeType fields { path } } "
        "fieldAssertion { fieldValuesAssertion { field { path } operator } "
        "fieldMetricAssertion { field { path } operator } } } "
        "runEvents(status: COMPLETE, limit: 1) { runEvents { timestampMillis result { type } } } } } }"
    )

    class Meta:
        """Scan metadata."""

        name = "assertions_scan"
        description = "Data quality assertions on each dataset with their latest pass/fail status"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Data Quality Assertions",
            category="quality",
            keywords="assertions, data quality, tests, dbt tests, great expectations, freshness, volume, checks",
            doc_llm=(
                "One row per data quality assertion (DataHub-native monitors and external dbt / Great "
                "Expectations tests): id, dataset_id, platform, qualified_name, assertion_type, description, "
                "source_type, assertion_platform, column_names, operator, native_type, last_status ('passed' / "
                "'failed' / 'error' / 'init' / NULL), last_run_at, external_url, url. Run history is in "
                "assertion_results. Pushdown: dataset_id = / IN (fetches those datasets), platform."
            ),
            doc_md="Assertions (data quality tests) and their latest status.",
            schema=ASSERTIONS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, assertion_type, description, last_run_at FROM datahub.main.assertions "
                    "WHERE last_status = 'failed' ORDER BY last_run_at DESC"
                ),
                description="Assertions whose latest run failed",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per assertion on the dataset."""
        base = _dataset_values(entity)
        rows = []
        for a in get(entity, "assertions", "assertions") or []:
            info = a.get("info") or {}
            ds = info.get("datasetAssertion") or {}
            fa = info.get("fieldAssertion") or {}
            fv = fa.get("fieldValuesAssertion") or fa.get("fieldMetricAssertion") or {}
            columns = [f.get("path") for f in ds.get("fields") or [] if f and f.get("path")]
            if get(fv, "field", "path"):
                columns.append(get(fv, "field", "path"))
            events = get(a, "runEvents", "runEvents") or []
            last = events[0] if events else {}
            result_type = get(last, "result", "type")
            rows.append(
                {
                    **base,
                    "id": a.get("urn"),
                    "assertion_type": info.get("type"),
                    "description": info.get("description"),
                    "source_type": get(info, "source", "type"),
                    "assertion_platform": get(a, "platform", "name"),
                    "column_names": columns,
                    "operator": ds.get("operator") or fv.get("operator"),
                    "native_type": ds.get("nativeType"),
                    "last_status": RESULT_STATUS.get(result_type or ""),
                    "last_run_at": _ts(last.get("timestampMillis")),
                    "external_url": info.get("externalUrl"),
                    "url": ep.entity_url(entity.get("urn"), "DATASET"),
                }
            )
        return rows


ASSERTION_RESULTS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("assertion_id", pa.string(), "Assertion URN (assertions.id).", nullable=False),
        *_dataset_columns(),
        column("assertion_type", pa.string(), "What kind of check ran: FRESHNESS, VOLUME, FIELD, SQL, DATASET, ..."),
        column("run_id", pa.string(), "Run id of the evaluation."),
        column("run_at", TSTZ, "When the assertion was evaluated.", nullable=False),
        column("status", pa.string(), "'passed', 'failed', 'error' or 'init'."),
        column("result_type", pa.string(), "DataHub result type: SUCCESS, FAILURE, ERROR or INIT."),
        column("row_count", pa.int64(), "Number of rows the evaluation looked at (count of rows)."),
        column("unexpected_count", pa.int64(), "Number of rows that violated the assertion (count of rows)."),
        column("missing_count", pa.int64(), "Number of rows with a missing value (count of rows)."),
        column("actual_value", pa.float64(), "Observed aggregate value (e.g. the row count of a volume check)."),
        column("external_url", pa.string(), "Link to the run in the evaluating tool."),
        column("native_results", STR_MAP, "Tool-specific result details."),
    ]
)


@init_single_worker
@bind_fixed_schema
class AssertionResultsScan(DatasetChildScan):
    """Rows of ``assertion_results``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ASSERTION_RESULTS_SCHEMA
    OPERATION: ClassVar[str] = "AssertionResults"
    EXTRA_VARS: ClassVar[str] = TIME_WINDOW_VARS
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} assertions(start: 0, count: 1000) {{ assertions {{ urn "
        "info { type } runEvents(status: COMPLETE, startTimeMillis: $startTimeMillis, "
        "endTimeMillis: $endTimeMillis, limit: $limit) { runEvents { timestampMillis runId status "
        "result { type rowCount missingCount unexpectedCount actualAggValue externalUrl "
        "nativeResults { key value } } } } } } }"
    )

    class Meta:
        """Scan metadata."""

        name = "assertion_results_scan"
        description = "Assertion run history: one row per evaluation with pass/fail and observed values"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Assertion Run History",
            category="quality",
            keywords="assertion results, test results, run events, pass, fail, data quality history",
            doc_llm=(
                "One row per assertion evaluation: assertion_id, dataset_id, platform, qualified_name, "
                "assertion_type, run_id, run_at, status ('passed' / 'failed' / 'error' / 'init'), result_type, "
                "row_count, unexpected_count, missing_count, actual_value, external_url, "
                "native_results (`MAP(VARCHAR, VARCHAR)`). "
                "Without a run_at filter only the newest 100 runs per assertion are read; a run_at range "
                "(>=, >, <=, <, BETWEEN) pushes down as DataHub's time window (up to 1000 runs per assertion). "
                "dataset_id and platform push down too."
            ),
            doc_md="Assertion run history (pass/fail over time).",
            schema=ASSERTION_RESULTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, count(*) FILTER (WHERE status = 'failed') AS failures, count(*) AS runs "
                    "FROM datahub.main.assertion_results WHERE run_at >= TIMESTAMPTZ '2026-01-01 00:00:00+00' "
                    "GROUP BY qualified_name ORDER BY failures DESC"
                ),
                description="Failure counts per dataset since a date (run_at pushes down)",
            ),
        ]

    @classmethod
    def extra_variables(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Time window from ``run_at``."""
        return time_window(params, "run_at")

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per run event."""
        base = _dataset_values(entity)
        rows = []
        for a in get(entity, "assertions", "assertions") or []:
            for ev in get(a, "runEvents", "runEvents") or []:
                result = ev.get("result") or {}
                when = _ts(ev.get("timestampMillis"))
                if when is None:
                    continue
                agg = result.get("actualAggValue")
                rows.append(
                    {
                        **base,
                        "assertion_id": a.get("urn"),
                        "assertion_type": get(a, "info", "type"),
                        "run_id": ev.get("runId"),
                        "run_at": when,
                        "status": RESULT_STATUS.get(result.get("type") or ""),
                        "result_type": result.get("type"),
                        "row_count": result.get("rowCount"),
                        "unexpected_count": result.get("unexpectedCount"),
                        "missing_count": result.get("missingCount"),
                        "actual_value": float(agg) if agg is not None else None,
                        "external_url": result.get("externalUrl"),
                        "native_results": custom_properties(result.get("nativeResults")),
                    }
                )
        return rows


# ----------------------------------------------------------------------------- profiles

FIELD_PROFILE = pa.struct(
    [
        pa.field("column_name", pa.string()),
        pa.field("null_count", pa.int64()),
        pa.field("null_proportion", pa.float64()),
        pa.field("unique_count", pa.int64()),
        pa.field("unique_proportion", pa.float64()),
        pa.field("min", pa.string()),
        pa.field("max", pa.string()),
        pa.field("mean", pa.string()),
        pa.field("median", pa.string()),
        pa.field("stdev", pa.string()),
    ]
)

PROFILES_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        *_dataset_columns(),
        column("profiled_at", TSTZ, "When the profile was taken.", nullable=False),
        column("row_count", pa.int64(), "Rows in the dataset."),
        column("column_count", pa.int64(), "Columns in the dataset."),
        column("size_bytes", pa.int64(), "Storage size in bytes."),
        column(
            "column_profiles",
            pa.list_(FIELD_PROFILE),
            "Per-column statistics: column_name, null_count, null_proportion, unique_count, "
            "unique_proportion, min, max, mean, median, stdev (values as text).",
        ),
    ]
)


@init_single_worker
@bind_fixed_schema
class DatasetProfilesScan(DatasetChildScan):
    """Rows of ``dataset_profiles``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = PROFILES_SCHEMA
    OPERATION: ClassVar[str] = "DatasetProfiles"
    EXTRA_VARS: ClassVar[str] = TIME_WINDOW_VARS
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} datasetProfiles(startTimeMillis: $startTimeMillis, "
        "endTimeMillis: $endTimeMillis, limit: $limit) { timestampMillis rowCount columnCount sizeInBytes "
        "fieldProfiles { fieldPath nullCount nullProportion uniqueCount uniqueProportion min max mean median "
        "stdev } } }"
    )

    class Meta:
        """Scan metadata."""

        name = "dataset_profiles_scan"
        description = "Dataset profiles over time: row counts, sizes and per-column statistics"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Dataset Profile History",
            category="quality",
            keywords="profiles, row count, table size, column statistics, null proportion, data profiling",
            doc_llm=(
                "One row per dataset profile: dataset_id, platform, qualified_name, profiled_at, row_count, "
                "column_count, size_bytes, column_profiles (a `LIST` of `STRUCT(column_name, null_count, "
                "null_proportion, unique_count, unique_proportion, min, max, mean, median, stdev)`). Newest 100 "
                "profiles per dataset unless profiled_at is range-filtered (pushed down as DataHub's time "
                "window, up to 1000). dataset_id and platform push down."
            ),
            doc_md="Profiles (row counts, sizes, column stats) over time.",
            schema=PROFILES_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, max_by(row_count, profiled_at) AS latest_rows "
                    "FROM datahub.main.dataset_profiles GROUP BY qualified_name ORDER BY latest_rows DESC"
                ),
                description="Latest row count per dataset",
            ),
        ]

    @classmethod
    def extra_variables(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Time window from ``profiled_at``."""
        return time_window(params, "profiled_at")

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per profile."""
        base = _dataset_values(entity)
        rows = []
        for p in entity.get("datasetProfiles") or []:
            when = _ts(p.get("timestampMillis"))
            if when is None:
                continue
            rows.append(
                {
                    **base,
                    "profiled_at": when,
                    "row_count": p.get("rowCount"),
                    "column_count": p.get("columnCount"),
                    "size_bytes": p.get("sizeInBytes"),
                    "column_profiles": [
                        {
                            "column_name": f.get("fieldPath"),
                            "null_count": f.get("nullCount"),
                            "null_proportion": f.get("nullProportion"),
                            "unique_count": f.get("uniqueCount"),
                            "unique_proportion": f.get("uniqueProportion"),
                            "min": f.get("min"),
                            "max": f.get("max"),
                            "mean": f.get("mean"),
                            "median": f.get("median"),
                            "stdev": f.get("stdev"),
                        }
                        for f in p.get("fieldProfiles") or []
                        if isinstance(f, dict)
                    ],
                }
            )
        return rows


# ----------------------------------------------------------------------------- operations

OPERATIONS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        *_dataset_columns(),
        column("reported_at", TSTZ, "When the operation was reported to DataHub.", nullable=False),
        column("operation_at", TSTZ, "When the operation happened on the source platform."),
        column("operation_type", pa.string(), "INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, CUSTOM or UNKNOWN."),
        column("custom_operation_type", pa.string(), "Name of a CUSTOM operation."),
        column("source_type", pa.string(), "DATA_PROCESS or DATA_PLATFORM: who reported it."),
        column("actor", pa.string(), "URN of the user / job that performed it."),
        column("affected_rows", pa.int64(), "Number of rows the operation changed (count of rows), when reported."),
    ]
)


@init_single_worker
@bind_fixed_schema
class OperationsScan(DatasetChildScan):
    """Rows of ``operations``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OPERATIONS_SCHEMA
    OPERATION: ClassVar[str] = "Operations"
    EXTRA_VARS: ClassVar[str] = TIME_WINDOW_VARS
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} operations(startTimeMillis: $startTimeMillis, "
        "endTimeMillis: $endTimeMillis, limit: $limit) { timestampMillis lastUpdatedTimestamp actor "
        "operationType customOperationType sourceType numAffectedRows } }"
    )

    class Meta:
        """Scan metadata."""

        name = "operations_scan"
        description = "Write operations on datasets (inserts, updates, DDL) as reported to DataHub"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Dataset Write Operations",
            category="quality",
            keywords="operations, freshness, last updated, inserts, ddl, writes, audit",
            doc_llm=(
                "One row per reported dataset operation: dataset_id, platform, qualified_name, reported_at, "
                "operation_at, operation_type, custom_operation_type, source_type, actor, affected_rows. Use it "
                "for freshness (max(operation_at) per dataset). Newest 100 per dataset unless reported_at is "
                "range-filtered (pushed down as DataHub's time window, up to 1000). dataset_id and platform "
                "push down."
            ),
            doc_md="Dataset write operations (freshness signal).",
            schema=OPERATIONS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, max(operation_at) AS last_write FROM datahub.main.operations "
                    "GROUP BY qualified_name ORDER BY last_write"
                ),
                description="Last write per dataset (stalest first)",
            ),
        ]

    @classmethod
    def extra_variables(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """Time window from ``reported_at``."""
        return time_window(params, "reported_at")

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per operation."""
        base = _dataset_values(entity)
        rows = []
        for op in entity.get("operations") or []:
            when = _ts(op.get("timestampMillis"))
            if when is None:
                continue
            rows.append(
                {
                    **base,
                    "reported_at": when,
                    "operation_at": _ts(op.get("lastUpdatedTimestamp")),
                    "operation_type": op.get("operationType"),
                    "custom_operation_type": op.get("customOperationType"),
                    "source_type": op.get("sourceType"),
                    "actor": op.get("actor"),
                    "affected_rows": op.get("numAffectedRows"),
                }
            )
        return rows


# ----------------------------------------------------------------------------- usage

USER_USAGE = pa.struct(
    [
        pa.field("user_id", pa.string()),
        pa.field("username", pa.string()),
        pa.field("email", pa.string()),
        pa.field("query_count", pa.int64()),
    ]
)
COLUMN_USAGE = pa.struct([pa.field("column_name", pa.string()), pa.field("query_count", pa.int64())])
TIME_RANGES = ("DAY", "WEEK", "MONTH", "QUARTER", "HALF_YEAR", "YEAR", "ALL")

USAGE_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        *_dataset_columns(),
        column(
            "time_range",
            pa.string(),
            "Window of the statistics: DAY, WEEK, MONTH (default), QUARTER, HALF_YEAR, YEAR or ALL. Filter "
            "time_range = '<value>' to choose it.",
            nullable=False,
        ),
        column("query_count", pa.int64(), "Queries that touched the dataset in the window."),
        column("unique_user_count", pa.int64(), "Distinct users that queried it in the window."),
        column("top_users", pa.list_(USER_USAGE), "Users by query count: user_id, username, email, query_count."),
        column("top_columns", pa.list_(COLUMN_USAGE), "Columns by query count: column_name, query_count."),
        column("query_count_last_30_days", pa.int64(), "DataHub's 30-day query count summary."),
        column("unique_user_count_last_30_days", pa.int64(), "DataHub's 30-day distinct user summary."),
    ]
)


@init_single_worker
@bind_fixed_schema
class UsageScan(DatasetChildScan):
    """Rows of ``usage``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = USAGE_SCHEMA
    OPERATION: ClassVar[str] = "Usage"
    EXTRA_VARS: ClassVar[str] = ", $range: TimeRange"
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} usageStats(range: $range) {{ aggregations {{ uniqueUserCount "
        "totalSqlQueries users { user { urn username } count userEmail } fields { fieldName count } } } "
        "statsSummary { queryCountLast30Days uniqueUserCountLast30Days } }"
    )

    class Meta:
        """Scan metadata."""

        name = "usage_scan"
        description = "Dataset usage statistics: query counts, distinct users, top users and top columns"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Dataset Usage Statistics",
            category="quality",
            keywords="usage, popularity, query counts, top users, column usage, unused tables",
            doc_llm=(
                "One row per dataset that has usage statistics: dataset_id, platform, qualified_name, "
                "time_range, query_count, unique_user_count, top_users (a `LIST` of `STRUCT(user_id, username, email, "
                "query_count)`), top_columns (a `LIST` of `STRUCT(column_name, query_count)`), "
                "query_count_last_30_days, unique_user_count_last_30_days. The window defaults to MONTH; filter "
                "`time_range = 'WEEK'` (one value) to ask DataHub for another. dataset_id and platform push down."
            ),
            doc_md="Dataset usage statistics (popularity, top users, top columns).",
            schema=USAGE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, query_count, unique_user_count FROM datahub.main.usage "
                    "WHERE time_range = 'MONTH' ORDER BY query_count DESC LIMIT 10"
                ),
                description="The ten most-queried datasets this month",
            ),
        ]

    @classmethod
    def extra_variables(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """``range`` from ``time_range = ...`` (default MONTH)."""
        values = eq_values(params.current_pushdown_filters, "time_range")
        if not values:
            return {"range": "MONTH"}
        wanted = {str(v).upper() for v in values}
        if len(wanted) > 1 or not wanted <= set(TIME_RANGES):
            raise DataHubError(f"usage.time_range must be filtered to one of {', '.join(TIME_RANGES)}")
        return {"range": wanted.pop()}

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per dataset with usage statistics."""
        agg = get(entity, "usageStats", "aggregations")
        summary = entity.get("statsSummary") or {}
        if not agg and not any(v is not None for v in summary.values()):
            return []
        agg = agg or {}
        users = sorted((u for u in agg.get("users") or [] if isinstance(u, dict)), key=lambda u: -(u.get("count") or 0))
        fields = sorted(
            (f for f in agg.get("fields") or [] if isinstance(f, dict)), key=lambda f: -(f.get("count") or 0)
        )
        return [
            {
                **_dataset_values(entity),
                "time_range": plan["vars"]["range"],
                "query_count": agg.get("totalSqlQueries"),
                "unique_user_count": agg.get("uniqueUserCount"),
                "top_users": [
                    {
                        "user_id": get(u, "user", "urn"),
                        "username": get(u, "user", "username"),
                        "email": u.get("userEmail"),
                        "query_count": u.get("count"),
                    }
                    for u in users
                ],
                "top_columns": [{"column_name": f.get("fieldName"), "query_count": f.get("count")} for f in fields],
                "query_count_last_30_days": summary.get("queryCountLast30Days"),
                "unique_user_count_last_30_days": summary.get("uniqueUserCountLast30Days"),
            }
        ]


# ----------------------------------------------------------------------------- incidents

INCIDENT_STATES = ("ACTIVE", "RESOLVED")

INCIDENTS_SCHEMA = pa.schema(
    [
        catalog_tool_column(),
        column("id", pa.string(), "Incident URN.", nullable=False),
        *_dataset_columns(),
        column("incident_type", pa.string(), "FRESHNESS, VOLUME, FIELD, SQL, DATA_SCHEMA, OPERATIONAL or CUSTOM."),
        column("custom_type", pa.string(), "Name of a CUSTOM incident type."),
        column("title", pa.string(), "One-line summary of the problem."),
        column("description", pa.string(), "Details of the problem and its impact."),
        column("state", pa.string(), "ACTIVE or RESOLVED."),
        column("stage", pa.string(), "TRIAGE, INVESTIGATION, WORK_IN_PROGRESS, FIXED, NO_ACTION_REQUIRED, ..."),
        column("priority", pa.string(), "LOW, MEDIUM, HIGH or CRITICAL."),
        column("source_type", pa.string(), "MANUAL or ASSERTION_FAILURE."),
        column("created_at", TSTZ, "When the incident was raised."),
        column("started_at", TSTZ, "When the problem started, if reported."),
        column("updated_at", TSTZ, "Last status change."),
        column("created_by", pa.string(), "URN of whoever raised it."),
        column("url", pa.string(), "Link to the dataset in the DataHub UI."),
    ]
)


@init_single_worker
@bind_fixed_schema
class IncidentsScan(DatasetChildScan):
    """Rows of ``incidents``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INCIDENTS_SCHEMA
    OPERATION: ClassVar[str] = "Incidents"
    EXTRA_VARS: ClassVar[str] = ", $state: IncidentState"
    SELECTION: ClassVar[str] = (
        f"... on Dataset {{ {DATASET_IDENTITY} incidents(state: $state, start: 0, count: 1000) {{ incidents {{ "
        "urn incidentType customType title description priority startedAt source { type } "
        "status { state stage lastUpdated { time } } created { time actor } } } }"
    )

    class Meta:
        """Scan metadata."""

        name = "incidents_scan"
        description = "Data incidents raised on datasets (manual or from failing assertions)"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = scan_tags(
            title="Data Incidents",
            category="quality",
            keywords="incidents, data incidents, outages, data downtime, freshness, broken data",
            doc_llm=(
                "One row per incident on a dataset: id, dataset_id, platform, qualified_name, incident_type, "
                "custom_type, title, description, state (ACTIVE / RESOLVED), stage, priority, source_type, "
                "created_at, started_at, updated_at, created_by, url. `state = 'ACTIVE'` pushes down to "
                "DataHub; dataset_id and platform push down too."
            ),
            doc_md="Incidents on datasets.",
            schema=INCIDENTS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT qualified_name, title, priority, created_at FROM datahub.main.incidents "
                    "WHERE state = 'ACTIVE' ORDER BY created_at DESC"
                ),
                description="Open incidents, newest first (state pushes down)",
            ),
        ]

    @classmethod
    def extra_variables(cls, params: ProcessParams[Any]) -> dict[str, Any]:
        """``state`` from ``state = ...`` (one value), else every state."""
        values = eq_values(params.current_pushdown_filters, "state")
        wanted = {str(v).upper() for v in values or []} & set(INCIDENT_STATES)
        return {"state": wanted.pop() if values is not None and len(wanted) == 1 and len(values) == 1 else None}

    @classmethod
    def build_rows(cls, ep: Endpoint, entity: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
        """One row per incident."""
        base = _dataset_values(entity)
        rows = []
        for inc in get(entity, "incidents", "incidents") or []:
            status = inc.get("status") or {}
            rows.append(
                {
                    **base,
                    "id": inc.get("urn"),
                    "incident_type": inc.get("incidentType"),
                    "custom_type": inc.get("customType"),
                    "title": inc.get("title"),
                    "description": inc.get("description"),
                    "state": status.get("state"),
                    "stage": status.get("stage"),
                    "priority": inc.get("priority"),
                    "source_type": get(inc, "source", "type"),
                    "created_at": _ts(get(inc, "created", "time")),
                    "started_at": _ts(inc.get("startedAt")),
                    "updated_at": _ts(get(status, "lastUpdated", "time")),
                    "created_by": get(inc, "created", "actor"),
                    "url": ep.entity_url(entity.get("urn"), "DATASET"),
                }
            )
        return rows


QUALITY_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("assertions", AssertionsScan, ["id"]),
    ("assertion_results", AssertionResultsScan, None),
    ("dataset_profiles", DatasetProfilesScan, ["dataset_id", "profiled_at"]),
    ("operations", OperationsScan, None),
    ("usage", UsageScan, ["dataset_id"]),
    ("incidents", IncidentsScan, ["id"]),
]
