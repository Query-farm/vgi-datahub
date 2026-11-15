"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import glob
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from graphql import build_schema, parse, validate
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter

from datahub_worker.client import DataHubError, endpoint_from_secret, graphql, retry_delay
from datahub_worker.common import (
    dataset_name_from_urn,
    simple_field_path,
    split_dataset_name,
    to_millis,
    ts,
)
from datahub_worker.lineage import Lineage, LineageEdgesScan, Search, degree_filter
from datahub_worker.quality import AssertionResultsScan, IncidentsScan, UsageScan
from datahub_worker.scan import ScanCursor
from datahub_worker.worker import ALL_TABLES, DatahubWorker

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = build_schema(
    "\n".join(Path(f).read_text() for f in sorted(glob.glob(str(ROOT / "test/datahub_schema/*.graphql"))))
)

# ----------------------------------------------------------------------------- GraphQL documents


def _documents() -> list[tuple[str, str]]:
    docs = []
    for name, scan, _ in ALL_TABLES:
        docs += [(f"{name}.scroll", scan.scroll_document()), (f"{name}.get", scan.get_document())]
    return [*docs, ("search", Search.scroll_document()), ("lineage", Lineage.DOCUMENT)]


@pytest.mark.parametrize(("name", "document"), _documents())
def test_every_query_is_valid_against_the_pinned_datahub_schema(name: str, document: str) -> None:
    errors = validate(SCHEMA, parse(document))
    assert not errors, [e.message for e in errors]


def test_catalog_lists_every_table() -> None:
    schema = DatahubWorker.catalog.schemas[0]
    names = {t.name for t in schema.tables}
    assert {
        "datasets",
        "columns",
        "owners",
        "tags",
        "glossary_terms",
        "domains",
        "lineage_edges",
        "assertions",
        "assertion_results",
        "usage",
    } <= names
    assert len(names) == 22


# ----------------------------------------------------------------------------- endpoint / secret


def test_endpoint_from_gms_url() -> None:
    ep = endpoint_from_secret({"gms_url": "http://localhost:8080/", "token": "t"})
    assert ep.graphql_url == "http://localhost:8080/api/graphql"
    assert ep.frontend_url is None
    assert ep.entity_url("urn:li:tag:pii", "TAG") is None
    assert ep.token == "t"
    assert ep.page_size == 100


def test_endpoint_datahub_cloud_derives_frontend() -> None:
    ep = endpoint_from_secret({"gms_url": "https://acme.acryl.io/gms", "page_size": "50"})
    assert ep.graphql_url == "https://acme.acryl.io/gms/api/graphql"
    assert ep.frontend_url == "https://acme.acryl.io"
    assert ep.entity_url("urn:li:tag:pii", "TAG") == "https://acme.acryl.io/tag/urn%3Ali%3Atag%3Apii"
    assert ep.page_size == 50


def test_endpoint_frontend_only_uses_v2_route() -> None:
    ep = endpoint_from_secret({"frontend_url": "http://localhost:9002"})
    assert ep.graphql_url == "http://localhost:9002/api/v2/graphql"


@pytest.mark.parametrize(
    ("secret", "message"),
    [
        (None, "no datahub secret"),
        ({"token": "x"}, "needs GMS_URL"),
        ({"gms_url": "localhost:8080"}, "must start with http"),
        ({"gms_url": "http://x", "page_size": "0"}, "between 1 and 1000"),
        ({"gms_url": "http://x", "page_size": "ten"}, "must be an integer"),
    ],
)
def test_endpoint_errors(secret: dict[str, Any] | None, message: str) -> None:
    with pytest.raises(DataHubError, match=message):
        endpoint_from_secret(secret)


def test_retry_delay_honours_retry_after() -> None:
    assert retry_delay("3", 1) == 3.0
    assert retry_delay("999", 1) == 60.0
    assert retry_delay(None, 3) == 4.0
    assert retry_delay("soon", 1) == 1.0


# ----------------------------------------------------------------------------- names


def test_split_relational_names() -> None:
    n = split_dataset_name("snowflake", "ANALYTICS.Marts.FCT_ORDERS")
    assert (n.database, n.schema, n.table, n.qualified_name) == (
        "ANALYTICS",
        "Marts",
        "FCT_ORDERS",
        "analytics.marts.fct_orders",
    )
    n = split_dataset_name("mysql", "shop.orders")
    assert (n.database, n.schema, n.table, n.qualified_name) == (None, "shop", "orders", "shop.orders")
    n = split_dataset_name("postgres", "prod-pg.app.public.users", instance="prod-pg")
    assert n.qualified_name == "app.public.users"
    n = split_dataset_name("kafka", "Orders.Events")
    assert (n.table, n.qualified_name) == (None, "orders.events")


def test_dataset_urn_parsing() -> None:
    n = dataset_name_from_urn("urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.ds.tbl,DEV)")
    assert (n.platform, n.environment, n.qualified_name) == ("bigquery", "DEV", "proj.ds.tbl")
    assert dataset_name_from_urn("urn:li:chart:(looker,1)").platform is None


def test_simple_field_path() -> None:
    assert simple_field_path("[version=2.0].[type=struct].address.[type=string].city") == "address.city"
    assert simple_field_path("[version=2.0].[key=True].[type=long].id") == "id"
    assert simple_field_path("plain.path") == "plain.path"
    assert simple_field_path(None) is None


def test_time_helpers() -> None:
    assert ts(0) is None
    assert ts(1_772_323_200_000) == datetime(2026, 3, 1, tzinfo=UTC)
    assert to_millis(datetime(2026, 3, 1, tzinfo=UTC)) == 1_772_323_200_000
    assert degree_filter(1) == ["1"]
    assert degree_filter(7) == ["1", "2", "3+"]


# ----------------------------------------------------------------------------- pushdown plans


@dataclass
class _Filters:
    filters: list[Any] = field(default_factory=list)


@dataclass
class _Params:
    current_pushdown_filters: _Filters
    args: Any = None


def _eq(column: str, value: Any) -> ConstantFilter:
    return ConstantFilter(column_name=column, column_index=0, op=ComparisonOp.EQ, value=pa.scalar(value))


def _in(column: str, values: list[Any]) -> InFilter:
    return InFilter(column_name=column, column_index=0, values=pa.array(values))


def _plan(scan: Any, *filters: Any, args: Any = None) -> dict[str, Any]:
    return dict(scan.plan(_Params(_Filters(list(filters)), args)))


def test_plan_pushes_facets() -> None:
    from datahub_worker.assets import DatasetsScan

    plan = _plan(
        DatasetsScan,
        _eq("platform", "snowflake"),
        _in("environment", ["prod", "dev"]),
        _eq("domain_id", "urn:li:domain:sales"),
    )
    assert plan["mode"] == "scroll"
    assert plan["types"] == ["DATASET"]
    assert {f["field"]: f["values"] for f in plan["facets"]} == {
        "platform": ["urn:li:dataPlatform:snowflake"],
        "origin": ["DEV", "PROD"],
        "domains": ["urn:li:domain:sales"],
    }


def test_plan_ids_become_urn_fetch() -> None:
    from datahub_worker.assets import ColumnsScan

    plan = _plan(ColumnsScan, _in("dataset_id", ["urn:b", "urn:a"]))
    assert plan == {"mode": "urns", "urns": ["urn:a", "urn:b"], "vars": {}}


def test_plan_entity_type_and_contradiction() -> None:
    from datahub_worker.governance import OwnersScan

    assert _plan(OwnersScan, _in("entity_type", ["dataset", "chart"]))["types"] == ["DATASET", "CHART"]
    assert _plan(OwnersScan, _eq("entity_type", "nope"))["mode"] == "empty"


def test_plan_time_window_and_enums() -> None:
    start = datetime(2026, 3, 5, tzinfo=UTC)
    ge = ConstantFilter(column_name="run_at", column_index=0, op=ComparisonOp.GE, value=pa.scalar(start))
    assert _plan(AssertionResultsScan, ge)["vars"] == {
        "startTimeMillis": to_millis(start),
        "endTimeMillis": None,
        "limit": 1000,
    }
    assert _plan(AssertionResultsScan)["vars"]["limit"] == 100
    assert _plan(UsageScan, _eq("time_range", "week"))["vars"] == {"range": "WEEK"}
    assert _plan(IncidentsScan, _eq("state", "ACTIVE"))["vars"] == {"state": "ACTIVE"}
    assert _plan(IncidentsScan, _in("state", ["ACTIVE", "RESOLVED"]))["vars"] == {"state": None}


def test_plan_search_arguments() -> None:
    @dataclass
    class Args:
        query: str = "  "
        tag: str | None = "pii"
        glossary_term: str | None = None
        owner: str | None = "jdoe"

    plan = _plan(Search, _eq("entity_type", "dataset"), args=Args())
    assert plan["query"] == "*"
    assert plan["types"] == ["DATASET"]
    assert {f["field"]: f["values"] for f in plan["facets"]} == {
        "tags": ["urn:li:tag:pii"],
        "owners": ["urn:li:corpuser:jdoe"],
    }


# ----------------------------------------------------------------------------- against the mock


def _scan_all(scan: Any, mock: str, plan: dict[str, Any], page_size: int = 2) -> list[dict[str, Any]]:
    ep = endpoint_from_secret({"gms_url": mock, "page_size": str(page_size)})
    state = ScanCursor()
    rows: list[dict[str, Any]] = []
    while not state.done:
        for entity in scan.fetch_page(ep, plan, state):
            rows.extend(scan.build_rows(ep, entity, plan))
    return rows


def test_scroll_pages_and_sends_filters(mock_datahub: str, requests_log: Callable[[], list[dict[str, Any]]]) -> None:
    from datahub_worker.assets import DatasetsScan

    plan = _plan(DatasetsScan, _eq("platform", "snowflake"))
    rows = _scan_all(DatasetsScan, mock_datahub, plan)
    assert sorted(r["qualified_name"] for r in rows) == [
        "analytics.marts.dim_customers",
        "analytics.marts.fct_orders",
        "analytics.raw.customers",
        "analytics.raw.orders",
        "analytics.staging.stg_orders",
    ]
    sent = requests_log()
    assert [r["operationName"] for r in sent] == ["vgiDatasetsScroll"] * 3
    first = sent[0]["variables"]["input"]
    assert first["orFilters"] == [
        {"and": [{"field": "platform", "values": ["urn:li:dataPlatform:snowflake"], "condition": "EQUAL"}]}
    ]
    assert "scrollId" not in first and sent[1]["variables"]["input"]["scrollId"]


def test_lineage_edges_by_urn(mock_datahub: str, requests_log: Callable[[], list[dict[str, Any]]]) -> None:
    urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.marts.fct_orders,PROD)"
    rows = _scan_all(LineageEdgesScan, mock_datahub, _plan(LineageEdgesScan, _eq("downstream_id", urn)))
    assert {(r["level"], r["upstream_qualified_name"], r.get("upstream_column")) for r in rows} == {
        ("dataset", "analytics.staging.stg_orders", None),
        ("column", "analytics.staging.stg_orders", "order_id"),
        ("column", "analytics.staging.stg_orders", "amount"),
    }
    assert [r["operationName"] for r in requests_log()] == ["vgiLineageEdgesGet"]


def test_graphql_errors_surface(mock_datahub: str) -> None:
    ep = endpoint_from_secret({"gms_url": mock_datahub})
    with pytest.raises(DataHubError, match="Cannot query field 'nope'"):
        graphql(ep, "vgiTagsScroll", "query vgiTagsScroll { nope }", {})
    with pytest.raises(DataHubError, match="unexpected operationName"):
        graphql(ep, "Other", "query Other { __typename }", {})
