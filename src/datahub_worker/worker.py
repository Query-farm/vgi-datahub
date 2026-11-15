"""Assemble the ``datahub`` catalog and run the worker.

Run it:

    vgi-datahub-worker                      # stdio: what DuckDB launches by default
    vgi-datahub-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'datahub' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-datahub-worker');
    CREATE SECRET (TYPE datahub, GMS_URL 'http://localhost:8080', TOKEN '...');
    SELECT qualified_name FROM datahub.main.datasets WHERE platform = 'snowflake';
"""

from __future__ import annotations

import json
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table

from .assets import ASSET_TABLES
from .client import DATAHUB_SECRET_TYPE
from .common import catalog_table
from .governance import GOVERNANCE_TABLES
from .lineage import FUNCTIONS, LINEAGE_TABLES
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .quality import QUALITY_TABLES
from .scalars import SCALARS

ALL_TABLES = [*ASSET_TABLES, *GOVERNANCE_TABLES, *LINEAGE_TABLES, *QUALITY_TABLES]
DOMAIN_TAG = {"assets": "data catalog", "governance": "data governance", "lineage": "data lineage"}


def _examples(objects: list[type[Any]]) -> list[dict[str, str]]:
    return [{"description": ex.description, "sql": ex.sql} for fn in objects for ex in getattr(fn.Meta, "examples", [])]


def _stamp_example_queries(objects: list[type[Any]]) -> None:
    """Publish each object's examples as ``vgi.example_queries`` (overloads share one merged list)."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in objects:
        bucket = by_name.setdefault(fn.Meta.name, [])
        bucket.extend(e for e in _examples([fn]) if e not in bucket)
    for fn in objects:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}


def _tables() -> list[Table]:
    tables = []
    for name, scan, pk in ALL_TABLES:
        category = scan.Meta.tags["vgi.category"]
        tags = {**scan.Meta.tags, "domain": DOMAIN_TAG.get(category, "data quality")}
        tables.append(
            catalog_table(name, scan, comment=scan.Meta.description, tags=tags, schema_path=["main"], primary_key=pk)
        )
    return tables


_SCANS = [scan for _, scan, _ in ALL_TABLES]
OBJECTS: list[type[Any]] = [*FUNCTIONS, *SCALARS]
_stamp_example_queries(_SCANS + OBJECTS)


class DatahubWorker(Worker):
    """The worker process: the ``datahub`` catalog with one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="DataHub datasets, columns, governance metadata, lineage, assertions, profiles and usage.",
                functions=OBJECTS,
                tables=_tables(),
                tags={
                    "vgi.title": "DataHub Metadata",
                    "domain": "data catalog",
                    "vgi.keywords": keywords_array(
                        "datahub, datasets, columns, owners, glossary, domains, lineage, assertions, usage, search"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_examples(_SCANS + OBJECTS)),
                },
            )
        ],
    )

    secret_types = [DATAHUB_SECRET_TYPE]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    DatahubWorker.main()


if __name__ == "__main__":
    main()
