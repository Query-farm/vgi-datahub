"""Write ``datahub.json``: a small DataHub metadata graph as GraphQL-shaped records.

Run ``python test/fixtures/build_fixture.py`` after editing; the JSON is committed and read by
``test/mock_datahub/server.py``. Each entity is shaped like the DataHub v1.8.0 GraphQL type it
stands for (``test/datahub_schema``). ``{"$ref": urn}`` points at another entity. Keys that start
with ``_`` are timeseries / relationship data the mock turns into argument-taking fields
(``datasetProfiles``, ``operations``, ``assertions``, ``usageStats``, ``incidents``, ``lineage``).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OUT = Path(__file__).with_name("datahub.json")


def ms(text: str) -> int:
    """Epoch milliseconds of an ISO UTC timestamp."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def ref(urn: str) -> dict[str, str]:
    """A reference to another entity."""
    return {"$ref": urn}


def ds_urn(platform: str, name: str, env: str = "PROD") -> str:
    """A dataset URN."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


TECH = "urn:li:ownershipType:__system__technical_owner"
BIZ = "urn:li:ownershipType:__system__business_owner"


def owners(*pairs: tuple[str, str]) -> dict[str, Any]:
    """An ``Ownership`` with (owner urn, ownership type urn) pairs."""
    legacy = {TECH: "TECHNICAL_OWNER", BIZ: "BUSINESS_OWNER"}
    return {
        "owners": [{"owner": ref(o), "type": legacy[t], "ownershipType": ref(t)} for o, t in pairs],
        "lastModified": {"time": ms("2026-01-01T00:00:00")},
    }


def tags(*names: str) -> dict[str, Any]:
    """A ``GlobalTags``."""
    return {"tags": [{"tag": ref(f"urn:li:tag:{n}")} for n in names]}


def terms(*ids: str) -> dict[str, Any]:
    """A ``GlossaryTerms``."""
    return {"terms": [{"term": ref(f"urn:li:glossaryTerm:{i}")} for i in ids]}


def domain(d: str) -> dict[str, Any]:
    """A ``DomainAssociation``."""
    return {"domain": ref(f"urn:li:domain:{d}")}


def field(
    path: str,
    type_: str,
    native: str,
    *,
    nullable: bool = True,
    desc: str | None = None,
    tag_names: tuple[str, ...] = (),
    term_ids: tuple[str, ...] = (),
) -> dict[str, Any]:
    """A ``SchemaField``."""
    return {
        "fieldPath": path,
        "nullable": nullable,
        "description": desc,
        "type": type_,
        "nativeDataType": native,
        "recursive": False,
        "isPartOfKey": False,
        "globalTags": tags(*tag_names) if tag_names else None,
        "glossaryTerms": terms(*term_ids) if term_ids else None,
    }


entities: list[dict[str, Any]] = []


def add(e: dict[str, Any]) -> dict[str, Any]:
    """Register an entity."""
    entities.append(e)
    return e


# ----------------------------------------------------------------------------- platforms, people

for p in ("snowflake", "postgres", "kafka", "looker", "airflow", "datahub", "dbt"):
    add({"urn": f"urn:li:dataPlatform:{p}", "type": "DATA_PLATFORM", "name": p})

for username, display, email, title, dept in (
    ("jdoe", "Jane Doe", "jane@example.com", "Data Engineer", "Data"),
    ("asmith", "Alex Smith", "alex@example.com", "Analyst", "Finance"),
):
    add(
        {
            "urn": f"urn:li:corpuser:{username}",
            "type": "CORP_USER",
            "username": username,
            "status": "ACTIVE",
            "properties": {
                "active": True,
                "displayName": display,
                "email": email,
                "title": title,
                "departmentName": dept,
            },
            "editableProperties": None,
        }
    )
add(
    {
        "urn": "urn:li:corpuser:former",
        "type": "CORP_USER",
        "username": "former",
        "status": "SUSPENDED",
        "properties": {
            "active": True,
            "displayName": "Former Employee",
            "email": None,
            "title": None,
            "departmentName": None,
        },
        "editableProperties": {"displayName": "Former (left 2025)", "email": None, "title": None},
    }
)
for name, display, desc in (
    ("data-eng", "Data Engineering", "Builds pipelines."),
    ("analytics", "Analytics", "Owns marts and dashboards."),
):
    add(
        {
            "urn": f"urn:li:corpGroup:{name}",
            "type": "CORP_GROUP",
            "name": name,
            "properties": {
                "displayName": display,
                "description": desc,
                "email": f"{name}@example.com",
                "slack": f"#{name}",
            },
        }
    )
for urn, name in ((TECH, "Technical Owner"), (BIZ, "Business Owner")):
    add({"urn": urn, "type": "CUSTOM_OWNERSHIP_TYPE", "info": {"name": name, "description": None}})

# ----------------------------------------------------------------------------- tags, glossary, domains

for name, display, color, desc in (
    ("pii", "pii", "#D0021B", "Personally identifiable information."),
    ("tier1", "tier1", "#4A90E2", "Business-critical asset."),
    ("finance", "finance", None, None),
):
    add(
        {
            "urn": f"urn:li:tag:{name}",
            "type": "TAG",
            "name": name,
            "description": desc,
            "properties": {"name": display, "description": desc, "colorHex": color},
        }
    )

add(
    {
        "urn": "urn:li:glossaryNode:finance",
        "type": "GLOSSARY_NODE",
        "properties": {"name": "Finance", "description": "Financial concepts."},
        "parentNodes": {"count": 0, "nodes": []},
    }
)
add(
    {
        "urn": "urn:li:glossaryNode:revenue",
        "type": "GLOSSARY_NODE",
        "properties": {"name": "Revenue", "description": None},
        "parentNodes": {"count": 1, "nodes": [ref("urn:li:glossaryNode:finance")]},
    }
)
add(
    {
        "urn": "urn:li:glossaryNode:classification",
        "type": "GLOSSARY_NODE",
        "properties": {"name": "Classification", "description": "Data sensitivity classes."},
        "parentNodes": {"count": 0, "nodes": []},
    }
)
for term_id, name, definition, parents, dom in (
    (
        "gross_revenue",
        "Gross Revenue",
        "Order amount before refunds and discounts.",
        ["urn:li:glossaryNode:revenue", "urn:li:glossaryNode:finance"],
        "sales",
    ),
    ("customer_id", "Customer ID", "Stable identifier of a customer.", ["urn:li:glossaryNode:finance"], None),
    ("email_address", "Email Address", "A personal email address.", ["urn:li:glossaryNode:classification"], None),
):
    add(
        {
            "urn": f"urn:li:glossaryTerm:{term_id}",
            "type": "GLOSSARY_TERM",
            "name": term_id,
            "hierarchicalName": ".".join(
                [*reversed([p.split(":")[-1].capitalize() for p in parents]), name.replace(" ", "")]
            ),
            "properties": {
                "name": name,
                "definition": definition,
                "termSource": "INTERNAL",
                "sourceRef": None,
                "sourceUrl": None,
            },
            "parentNodes": {"count": len(parents), "nodes": [ref(p) for p in parents]},
            "ownership": owners(("urn:li:corpuser:asmith", BIZ)) if term_id == "gross_revenue" else None,
            "domain": domain(dom) if dom else None,
        }
    )

add(
    {
        "urn": "urn:li:domain:sales",
        "type": "DOMAIN",
        "id": "sales",
        "properties": {"name": "Sales", "description": "Orders, customers and revenue."},
        "parentDomains": {"count": 0, "domains": []},
        "ownership": owners(("urn:li:corpGroup:analytics", BIZ)),
    }
)
add(
    {
        "urn": "urn:li:domain:sales_ops",
        "type": "DOMAIN",
        "id": "sales_ops",
        "properties": {"name": "Sales Ops", "description": None},
        "parentDomains": {"count": 1, "domains": [ref("urn:li:domain:sales")]},
        "ownership": None,
    }
)
add(
    {
        "urn": "urn:li:domain:marketing",
        "type": "DOMAIN",
        "id": "marketing",
        "properties": {"name": "Marketing", "description": "Campaigns."},
        "parentDomains": {"count": 0, "domains": []},
        "ownership": None,
    }
)

add(
    {
        "urn": "urn:li:dataProduct:orders_analytics",
        "type": "DATA_PRODUCT",
        "properties": {
            "name": "Orders Analytics",
            "description": "Curated order facts and the dashboards on them.",
            "externalUrl": None,
            "numAssets": 3,
        },
        "domain": domain("sales"),
        "ownership": owners(("urn:li:corpuser:asmith", BIZ)),
        "tags": tags("tier1"),
        "glossaryTerms": None,
    }
)

# ----------------------------------------------------------------------------- containers

SF = "urn:li:dataPlatform:snowflake"
add(
    {
        "urn": "urn:li:container:sf_analytics",
        "type": "CONTAINER",
        "platform": ref(SF),
        "properties": {
            "name": "ANALYTICS",
            "description": "Analytics database.",
            "externalUrl": None,
            "qualifiedName": "analytics",
        },
        "editableProperties": None,
        "subTypes": {"typeNames": ["Database"]},
        "container": None,
        "parentContainers": {"count": 0, "containers": []},
        "ownership": owners(("urn:li:corpGroup:data-eng", TECH)),
        "tags": None,
        "domain": None,
    }
)
for schema, dom in (("raw", None), ("staging", None), ("marts", "sales")):
    add(
        {
            "urn": f"urn:li:container:sf_{schema}",
            "type": "CONTAINER",
            "platform": ref(SF),
            "properties": {"name": schema.upper(), "description": None, "externalUrl": None, "qualifiedName": None},
            "editableProperties": {"description": f"{schema} layer"} if schema == "marts" else None,
            "subTypes": {"typeNames": ["Schema"]},
            "container": ref("urn:li:container:sf_analytics"),
            "parentContainers": {"count": 1, "containers": [ref("urn:li:container:sf_analytics")]},
            "ownership": None,
            "tags": None,
            "domain": domain(dom) if dom else None,
        }
    )

# ----------------------------------------------------------------------------- datasets

RAW_ORDERS = ds_urn("snowflake", "analytics.raw.orders")
RAW_CUSTOMERS = ds_urn("snowflake", "analytics.raw.customers")
STG_ORDERS = ds_urn("snowflake", "analytics.staging.stg_orders")
FCT_ORDERS = ds_urn("snowflake", "analytics.marts.fct_orders")
DIM_CUSTOMERS = ds_urn("snowflake", "analytics.marts.dim_customers")
PG_USERS = ds_urn("postgres", "prod-pg.app.public.users")
KAFKA_ORDERS = ds_urn("kafka", "orders.events")


def dataset(
    urn: str,
    platform: str,
    name: str,
    *,
    subtype: str,
    container: str | None,
    desc: str | None,
    fields: list[dict[str, Any]],
    pks: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """A ``Dataset`` record."""
    record: dict[str, Any] = {
        "urn": urn,
        "type": "DATASET",
        "name": name,
        "origin": "PROD",
        "lastIngested": ms("2026-03-10T06:00:00"),
        "platform": ref(f"urn:li:dataPlatform:{platform}"),
        "dataPlatformInstance": None,
        "properties": {
            "name": name if platform == "kafka" else name.split(".")[-1],
            "qualifiedName": None,
            "origin": "PROD",
            "description": desc,
            "customProperties": [],
            "externalUrl": None,
            "created": None,
            "lastModified": {"time": 0, "actor": None},
        },
        "editableProperties": None,
        "subTypes": {"typeNames": [subtype]},
        "container": ref(container) if container else None,
        "deprecation": None,
        "ownership": None,
        "tags": None,
        "glossaryTerms": None,
        "domain": None,
        "schemaMetadata": {
            "name": name,
            "platformUrn": f"urn:li:dataPlatform:{platform}",
            "version": 0,
            "hash": "",
            "fields": fields,
            "primaryKeys": pks,
        },
        "editableSchemaMetadata": None,
        "fineGrainedLineages": [],
        "statsSummary": None,
    }
    record.update(extra)
    return add(record)


dataset(
    RAW_ORDERS,
    "snowflake",
    "analytics.raw.orders",
    subtype="Table",
    container="urn:li:container:sf_raw",
    desc="Orders as loaded from the shop database.",
    fields=[
        field("order_id", "NUMBER", "NUMBER(38,0)", nullable=False),
        field("customer_id", "NUMBER", "NUMBER(38,0)"),
        field("amount", "NUMBER", "NUMBER(12,2)"),
        field("created_at", "TIME", "TIMESTAMP_NTZ"),
    ],
    pks=["order_id"],
    ownership=owners(("urn:li:corpGroup:data-eng", TECH)),
    properties={
        "name": "orders",
        "qualifiedName": "ANALYTICS.RAW.ORDERS",
        "origin": "PROD",
        "description": "Orders as loaded from the shop database.",
        "customProperties": [{"key": "retention_days", "value": "365", "associatedUrn": RAW_ORDERS}],
        "externalUrl": "https://app.snowflake.com/x/#/data/databases/ANALYTICS/schemas/RAW/table/ORDERS",
        "created": ms("2025-06-01T00:00:00"),
        "lastModified": {"time": ms("2026-03-09T23:00:00")},
    },
    _profiles=[
        {
            "timestampMillis": ms("2026-03-09T00:00:00"),
            "rowCount": 5000,
            "columnCount": 4,
            "sizeInBytes": 204800,
            "fieldProfiles": [],
        }
    ],
    _operations=[
        {
            "timestampMillis": ms("2026-03-09T23:05:00"),
            "lastUpdatedTimestamp": ms("2026-03-09T23:00:00"),
            "actor": "urn:li:corpuser:jdoe",
            "operationType": "INSERT",
            "customOperationType": None,
            "sourceType": "DATA_PLATFORM",
            "numAffectedRows": 250,
        }
    ],
    _usage={
        "MONTH": {
            "totalSqlQueries": 10,
            "uniqueUserCount": 1,
            "users": [{"user": ref("urn:li:corpuser:jdoe"), "count": 10, "userEmail": "jane@example.com"}],
            "fields": [{"fieldName": "order_id", "count": 10}],
        }
    },
)

dataset(
    RAW_CUSTOMERS,
    "snowflake",
    "analytics.raw.customers",
    subtype="Table",
    container="urn:li:container:sf_raw",
    desc="Customers copied from the app database.",
    fields=[
        field("customer_id", "NUMBER", "NUMBER(38,0)", nullable=False, term_ids=("customer_id",)),
        field("email", "STRING", "VARCHAR(256)", desc="Login email.", tag_names=("pii",), term_ids=("email_address",)),
        field("[version=2.0].[type=struct].[type=struct].address", "STRUCT", "OBJECT"),
        field("[version=2.0].[type=struct].[type=struct].address.[type=string].city", "STRING", "VARCHAR"),
    ],
    tags=tags("pii"),
    ownership=owners(("urn:li:corpuser:jdoe", TECH)),
    editableSchemaMetadata={
        "editableSchemaFieldInfo": [
            {
                "fieldPath": "[version=2.0].[type=struct].[type=struct].address.[type=string].city",
                "description": "City of the billing address.",
                "globalTags": tags("pii"),
                "glossaryTerms": None,
            }
        ]
    },
    _assertions=[
        {
            "urn": "urn:li:assertion:freshness_customers",
            "type": "ASSERTION",
            "platform": ref("urn:li:dataPlatform:datahub"),
            "info": {
                "type": "FRESHNESS",
                "description": "Loaded within 24 hours",
                "externalUrl": None,
                "source": {"type": "NATIVE"},
                "datasetAssertion": None,
                "fieldAssertion": None,
            },
            "_runEvents": [
                {
                    "timestampMillis": ms("2026-03-09T06:00:00"),
                    "runId": "fr-1",
                    "status": "COMPLETE",
                    "assertionUrn": "urn:li:assertion:freshness_customers",
                    "asserteeUrn": RAW_CUSTOMERS,
                    "result": {
                        "type": "SUCCESS",
                        "rowCount": None,
                        "missingCount": None,
                        "unexpectedCount": None,
                        "actualAggValue": None,
                        "externalUrl": None,
                        "nativeResults": [],
                    },
                }
            ],
        }
    ],
)

dataset(
    STG_ORDERS,
    "snowflake",
    "analytics.staging.stg_orders",
    subtype="View",
    container="urn:li:container:sf_staging",
    desc="Cleaned orders.",
    fields=[
        field("order_id", "NUMBER", "NUMBER(38,0)", nullable=False),
        field("customer_id", "NUMBER", "NUMBER(38,0)"),
        field("amount", "NUMBER", "NUMBER(12,2)"),
    ],
    fineGrainedLineages=[
        {
            "upstreams": [{"urn": RAW_ORDERS, "path": "order_id"}],
            "downstreams": [{"urn": STG_ORDERS, "path": "order_id"}],
            "transformOperation": "IDENTITY",
            "query": None,
        },
        {
            "upstreams": [{"urn": RAW_ORDERS, "path": "amount"}],
            "downstreams": [{"urn": STG_ORDERS, "path": "amount"}],
            "transformOperation": "IDENTITY",
            "query": None,
        },
    ],
)

dataset(
    FCT_ORDERS,
    "snowflake",
    "analytics.marts.fct_orders",
    subtype="Table",
    container="urn:li:container:sf_marts",
    desc="One row per order with revenue.",
    fields=[
        field("order_id", "NUMBER", "NUMBER(38,0)", nullable=False, desc="Order key."),
        field("customer_id", "NUMBER", "NUMBER(38,0)", term_ids=("customer_id",)),
        field("gross_revenue", "NUMBER", "NUMBER(14,2)", term_ids=("gross_revenue",)),
        field("order_date", "DATE", "DATE"),
    ],
    pks=["order_id"],
    ownership=owners(("urn:li:corpGroup:analytics", TECH), ("urn:li:corpuser:asmith", BIZ)),
    tags=tags("tier1", "finance"),
    glossaryTerms=terms("gross_revenue"),
    domain=domain("sales"),
    editableProperties={"description": "Order facts: one row per order, revenue in USD.", "name": None},
    fineGrainedLineages=[
        {
            "upstreams": [{"urn": STG_ORDERS, "path": "order_id"}],
            "downstreams": [{"urn": FCT_ORDERS, "path": "order_id"}],
            "transformOperation": "IDENTITY",
            "query": None,
        },
        {
            "upstreams": [{"urn": STG_ORDERS, "path": "amount"}],
            "downstreams": [{"urn": FCT_ORDERS, "path": "gross_revenue"}],
            "transformOperation": "SQL",
            "query": "urn:li:query:fct_orders_build",
        },
    ],
    statsSummary={"queryCountLast30Days": 120, "uniqueUserCountLast30Days": 2},
    _profiles=[
        {
            "timestampMillis": ms(f"2026-03-0{d}T00:00:00"),
            "rowCount": rows,
            "columnCount": 4,
            "sizeInBytes": rows * 100,
            "fieldProfiles": [
                {
                    "fieldPath": "gross_revenue",
                    "nullCount": 0,
                    "nullProportion": 0.0,
                    "uniqueCount": rows // 2,
                    "uniqueProportion": 0.5,
                    "min": "1.00",
                    "max": "999.00",
                    "mean": "42.5",
                    "median": "30.0",
                    "stdev": "12.1",
                }
            ],
        }
        for d, rows in ((1, 1000), (5, 1100), (9, 1200))
    ],
    _operations=[
        {
            "timestampMillis": ms(f"2026-03-0{d}T02:05:00"),
            "lastUpdatedTimestamp": ms(f"2026-03-0{d}T02:00:00"),
            "actor": "urn:li:corpuser:jdoe",
            "operationType": "INSERT",
            "customOperationType": None,
            "sourceType": "DATA_PROCESS",
            "numAffectedRows": n,
        }
        for d, n in ((5, 100), (9, 100))
    ],
    _usage={
        "MONTH": {
            "totalSqlQueries": 120,
            "uniqueUserCount": 2,
            "users": [
                {"user": ref("urn:li:corpuser:asmith"), "count": 40, "userEmail": "alex@example.com"},
                {"user": ref("urn:li:corpuser:jdoe"), "count": 80, "userEmail": "jane@example.com"},
            ],
            "fields": [{"fieldName": "order_id", "count": 30}, {"fieldName": "gross_revenue", "count": 90}],
        },
        "WEEK": {
            "totalSqlQueries": 30,
            "uniqueUserCount": 1,
            "users": [{"user": ref("urn:li:corpuser:asmith"), "count": 30, "userEmail": "alex@example.com"}],
            "fields": [],
        },
    },
    _assertions=[
        {
            "urn": "urn:li:assertion:volume_fct_orders",
            "type": "ASSERTION",
            "platform": ref("urn:li:dataPlatform:datahub"),
            "info": {
                "type": "VOLUME",
                "description": "Row count grows every day",
                "externalUrl": None,
                "source": {"type": "NATIVE"},
                "datasetAssertion": None,
                "fieldAssertion": None,
            },
            "_runEvents": [
                {
                    "timestampMillis": ms(f"2026-03-0{d}T03:00:00"),
                    "runId": f"vol-{d}",
                    "status": "COMPLETE",
                    "assertionUrn": "urn:li:assertion:volume_fct_orders",
                    "asserteeUrn": FCT_ORDERS,
                    "result": {
                        "type": result,
                        "rowCount": rows,
                        "missingCount": None,
                        "unexpectedCount": None,
                        "actualAggValue": float(rows),
                        "externalUrl": None,
                        "nativeResults": [{"key": "expected_min", "value": "1000"}],
                    },
                }
                for d, result, rows in ((1, "SUCCESS", 1000), (5, "SUCCESS", 1100), (9, "FAILURE", 900))
            ],
        },
        {
            "urn": "urn:li:assertion:dbt_not_null_order_id",
            "type": "ASSERTION",
            "platform": ref("urn:li:dataPlatform:dbt"),
            "info": {
                "type": "DATASET",
                "description": "not_null(order_id)",
                "externalUrl": "https://dbt.example.com/tests/not_null_fct_orders_order_id",
                "source": {"type": "EXTERNAL"},
                "datasetAssertion": {
                    "datasetUrn": FCT_ORDERS,
                    "scope": "DATASET_COLUMN",
                    "fields": [{"urn": FCT_ORDERS, "path": "order_id"}],
                    "operator": "NOT_NULL",
                    "nativeType": "not_null",
                },
                "fieldAssertion": None,
            },
            "_runEvents": [
                {
                    "timestampMillis": ms("2026-03-09T02:30:00"),
                    "runId": "dbt-77",
                    "status": "COMPLETE",
                    "assertionUrn": "urn:li:assertion:dbt_not_null_order_id",
                    "asserteeUrn": FCT_ORDERS,
                    "result": {
                        "type": "SUCCESS",
                        "rowCount": 1200,
                        "missingCount": 0,
                        "unexpectedCount": 0,
                        "actualAggValue": None,
                        "externalUrl": None,
                        "nativeResults": [],
                    },
                }
            ],
        },
    ],
    _incidents=[
        {
            "urn": "urn:li:incident:fct_orders_volume",
            "type": "INCIDENT",
            "incidentType": "VOLUME",
            "customType": None,
            "title": "fct_orders shrank",
            "description": "Row count dropped below 1000.",
            "priority": "HIGH",
            "startedAt": ms("2026-03-09T03:00:00"),
            "source": {"type": "ASSERTION_FAILURE"},
            "status": {
                "state": "ACTIVE",
                "stage": "INVESTIGATION",
                "message": None,
                "lastUpdated": {"time": ms("2026-03-09T04:00:00")},
            },
            "created": {"time": ms("2026-03-09T03:01:00"), "actor": "urn:li:corpuser:datahub"},
        },
        {
            "urn": "urn:li:incident:fct_orders_late",
            "type": "INCIDENT",
            "incidentType": "FRESHNESS",
            "customType": None,
            "title": "Late load",
            "description": None,
            "priority": "LOW",
            "startedAt": None,
            "source": {"type": "MANUAL"},
            "status": {
                "state": "RESOLVED",
                "stage": "FIXED",
                "message": "Rerun",
                "lastUpdated": {"time": ms("2026-02-02T10:00:00")},
            },
            "created": {"time": ms("2026-02-02T08:00:00"), "actor": "urn:li:corpuser:jdoe"},
        },
    ],
)

dataset(
    DIM_CUSTOMERS,
    "snowflake",
    "analytics.marts.dim_customers",
    subtype="Table",
    container="urn:li:container:sf_marts",
    desc=None,
    fields=[
        field("customer_id", "NUMBER", "NUMBER(38,0)", nullable=False),
        field("email", "STRING", "VARCHAR(256)", tag_names=("pii",)),
    ],
    domain=domain("sales"),
    deprecation={"deprecated": True, "note": "Use dim_customer_v2"},
)

dataset(
    PG_USERS,
    "postgres",
    "prod-pg.app.public.users",
    subtype="Table",
    container=None,
    desc="Application users.",
    fields=[field("id", "NUMBER", "integer", nullable=False), field("email", "STRING", "text", tag_names=("pii",))],
    dataPlatformInstance={
        "urn": "urn:li:dataPlatformInstance:(urn:li:dataPlatform:postgres,prod-pg)",
        "type": "DATA_PLATFORM_INSTANCE",
        "platform": ref("urn:li:dataPlatform:postgres"),
        "instanceId": "prod-pg",
    },
    ownership=owners(("urn:li:corpuser:former", TECH)),
)

dataset(
    KAFKA_ORDERS,
    "kafka",
    "orders.events",
    subtype="Topic",
    container=None,
    desc="Order events stream.",
    fields=[field("order_id", "NUMBER", "long")],
    domain=domain("marketing"),
)

# ----------------------------------------------------------------------------- BI and pipelines

LOOKER = "urn:li:dataPlatform:looker"
CHART = "urn:li:chart:(looker,34)"
DASH = "urn:li:dashboard:(looker,12)"
add(
    {
        "urn": CHART,
        "type": "CHART",
        "chartId": "34",
        "tool": "looker",
        "platform": ref(LOOKER),
        "properties": {
            "name": "Revenue by Day",
            "description": None,
            "externalUrl": "https://looker.example.com/looks/34",
            "type": "LINE",
            "created": {"time": ms("2025-11-01T00:00:00")},
            "lastModified": {"time": ms("2026-02-01T00:00:00")},
        },
        "editableProperties": None,
        "container": None,
        "ownership": owners(("urn:li:corpuser:asmith", TECH)),
        "tags": None,
        "glossaryTerms": None,
        "domain": domain("sales"),
    }
)
add(
    {
        "urn": DASH,
        "type": "DASHBOARD",
        "dashboardId": "12",
        "tool": "looker",
        "platform": ref(LOOKER),
        "properties": {
            "name": "Sales Overview",
            "description": "Daily revenue.",
            "externalUrl": "https://looker.example.com/dashboards/12",
            "lastRefreshed": ms("2026-03-09T07:00:00"),
            "created": {"time": ms("2025-11-01T00:00:00")},
            "lastModified": {"time": ms("2026-02-01T00:00:00")},
        },
        "editableProperties": None,
        "statsSummary": {"viewCount": 321},
        "container": None,
        "ownership": owners(("urn:li:corpGroup:analytics", BIZ)),
        "tags": tags("tier1"),
        "glossaryTerms": None,
        "domain": domain("sales"),
    }
)
add(
    {
        "urn": "urn:li:dashboard:(looker,99)",
        "type": "DASHBOARD",
        "dashboardId": "99",
        "tool": "looker",
        "platform": ref(LOOKER),
        "properties": {
            "name": "Scratch",
            "description": None,
            "externalUrl": None,
            "lastRefreshed": None,
            "created": {"time": 0},
            "lastModified": {"time": 0},
        },
        "editableProperties": None,
        "statsSummary": {"viewCount": 0},
        "container": None,
        "ownership": None,
        "tags": None,
        "glossaryTerms": None,
        "domain": None,
    }
)

FLOW = "urn:li:dataFlow:(airflow,elt_daily,prod)"
JOB = "urn:li:dataJob:(urn:li:dataFlow:(airflow,elt_daily,prod),load_customers)"
add(
    {
        "urn": FLOW,
        "type": "DATA_FLOW",
        "orchestrator": "airflow",
        "flowId": "elt_daily",
        "cluster": "prod",
        "platform": ref("urn:li:dataPlatform:airflow"),
        "properties": {
            "name": "elt_daily",
            "description": "Nightly ELT.",
            "project": None,
            "externalUrl": "https://airflow.example.com/dags/elt_daily",
        },
        "editableProperties": None,
        "container": None,
        "ownership": owners(("urn:li:corpGroup:data-eng", TECH)),
        "tags": None,
        "glossaryTerms": None,
        "domain": None,
    }
)
add(
    {
        "urn": JOB,
        "type": "DATA_JOB",
        "jobId": "load_customers",
        "platform": ref("urn:li:dataPlatform:airflow"),
        "dataFlow": ref(FLOW),
        "properties": {
            "name": "load_customers",
            "description": "Copy users to Snowflake.",
            "externalUrl": "https://airflow.example.com/dags/elt_daily/load_customers",
        },
        "editableProperties": None,
        "container": None,
        "inputOutput": {
            "inputDatasets": [ref(PG_USERS)],
            "outputDatasets": [ref(RAW_CUSTOMERS)],
            "inputDatajobs": [],
            "fineGrainedLineages": [
                {
                    "upstreams": [{"urn": PG_USERS, "path": "email"}],
                    "downstreams": [{"urn": RAW_CUSTOMERS, "path": "email"}],
                    "transformOperation": "IDENTITY",
                    "query": None,
                }
            ],
        },
        "ownership": owners(("urn:li:corpuser:jdoe", TECH)),
        "tags": None,
        "glossaryTerms": None,
        "domain": None,
    }
)

# Direct lineage edges (upstream -> downstream, DataHub relationship type).
lineage = [
    (KAFKA_ORDERS, RAW_ORDERS, "DownstreamOf"),
    (RAW_ORDERS, STG_ORDERS, "DownstreamOf"),
    (STG_ORDERS, FCT_ORDERS, "DownstreamOf"),
    (RAW_CUSTOMERS, DIM_CUSTOMERS, "DownstreamOf"),
    (PG_USERS, JOB, "Consumes"),
    (JOB, RAW_CUSTOMERS, "Produces"),
    (FCT_ORDERS, CHART, "Consumes"),
    (CHART, DASH, "Contains"),
]

OUT.write_text(
    json.dumps(
        {
            "datahub_version": "v1.8.0",
            "entities": entities,
            "lineage": [
                {"upstream": u, "downstream": d, "type": t, "createdOn": ms("2026-01-15T00:00:00")}
                for u, d, t in lineage
            ],
        },
        indent=1,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
print(f"wrote {OUT} ({len(entities)} entities)")
