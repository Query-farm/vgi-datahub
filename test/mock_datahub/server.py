"""Strict DataHub GraphQL replay server for the tests (no Kafka / Elasticsearch / MySQL needed).

It serves ``POST /api/graphql`` (GMS) and ``POST /api/v2/graphql`` (frontend) from the records
in ``test/fixtures/datahub.json``, and is strict about what it accepts:

* every request must be a named operation the worker is known to send (``vgi*``), and the
  ``operationName`` must match the document;
* every document is parsed and validated against DataHub's own v1.8.0 GraphQL schema
  (``test/datahub_schema``), and executed with graphql-core, so an unknown field, a wrong
  argument or a mistyped variable is an error exactly as it would be on a real DataHub;
* search ``orFilters`` may only use facet fields DataHub indexes (``platform``, ``origin``,
  ``domains``, ``container``, ``tags``, ``glossaryTerms``, ``owners``, ``urn``; ``degree`` and
  ``platform`` for lineage); anything else is rejected.

Environment / flags:

* ``MOCK_TOKEN=<t>``: require ``Authorization: Bearer <t>`` (401 otherwise).
* ``MOCK_FLAKY=1``: answer the first sighting of every third distinct request with
  ``429 Retry-After: 0``, so the retry path runs in every suite.
* ``MOCK_STRICT_PUSHDOWN=1``: refuse an unfiltered DATASET scroll (no ``orFilters``), so a SQL
  test can prove a filter was pushed down.
* ``GET /__requests`` returns the log of ``{operationName, variables}`` received;
  ``POST /__reset`` clears it.

Prints ``PORT:<n>`` on stdout once listening (``--port 0`` picks a free port).
"""

# GraphQL argument names (startTimeMillis, filter, ...) are bound as resolver keyword arguments.
# ruff: noqa: N803, A002, A006
from __future__ import annotations

import argparse
import base64
import contextlib
import copy
import glob
import hashlib
import json
import os
import sys
import threading
from collections import deque
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from graphql import GraphQLError, build_schema, graphql_sync, parse, validate
from graphql.language import OperationDefinitionNode

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = build_schema(
    "\n".join(Path(f).read_text() for f in sorted(glob.glob(str(ROOT / "test/datahub_schema/*.graphql"))))
)
FIXTURE = json.loads((ROOT / "test/fixtures/datahub.json").read_text())

ALLOWED_OPERATIONS = {
    f"vgi{stem}{kind}"
    for stem in (
        "Datasets",
        "Columns",
        "Containers",
        "Dashboards",
        "Charts",
        "DataFlows",
        "DataJobs",
        "Owners",
        "Tags",
        "GlossaryTerms",
        "GlossaryNodes",
        "Domains",
        "DataProducts",
        "Users",
        "Groups",
        "LineageEdges",
        "Assertions",
        "AssertionResults",
        "DatasetProfiles",
        "Operations",
        "Usage",
        "Incidents",
        "Search",
    )
    for kind in ("Scroll", "Get")
} | {"vgiLineage"}

SEARCH_FIELDS = {"platform", "origin", "domains", "container", "tags", "glossaryTerms", "owners", "urn"}
LINEAGE_FIELDS = {"degree", "platform"}
MAX_COUNT = 10_000

TYPENAMES = {
    "DATASET": "Dataset",
    "DASHBOARD": "Dashboard",
    "CHART": "Chart",
    "DATA_FLOW": "DataFlow",
    "DATA_JOB": "DataJob",
    "CONTAINER": "Container",
    "DOMAIN": "Domain",
    "DATA_PRODUCT": "DataProduct",
    "GLOSSARY_TERM": "GlossaryTerm",
    "GLOSSARY_NODE": "GlossaryNode",
    "TAG": "Tag",
    "CORP_USER": "CorpUser",
    "CORP_GROUP": "CorpGroup",
    "DATA_PLATFORM": "DataPlatform",
    "DATA_PLATFORM_INSTANCE": "DataPlatformInstance",
    "ASSERTION": "Assertion",
    "INCIDENT": "Incident",
    "CUSTOM_OWNERSHIP_TYPE": "OwnershipTypeEntity",
}


class MockError(Exception):
    """A request the strict mock refuses."""


# ----------------------------------------------------------------------------- the graph


def _window(events: list[dict[str, Any]], start: Any, end: Any, limit: Any) -> list[dict[str, Any]]:
    """Newest-first events in [start, end) capped at ``limit`` (DataHub's timeseries semantics)."""
    out = sorted(events, key=lambda e: -e["timestampMillis"])
    if start is not None:
        out = [e for e in out if e["timestampMillis"] >= int(start)]
    if end is not None:
        out = [e for e in out if e["timestampMillis"] < int(end)]
    return out[: int(limit)] if limit is not None else out


class Graph:
    """The fixture's entities with references resolved and argument-taking fields attached."""

    def __init__(self, fixture: dict[str, Any]) -> None:
        raw = copy.deepcopy(fixture["entities"])
        self.by_urn: dict[str, dict[str, Any]] = {e["urn"]: e for e in raw}
        self.edges: list[dict[str, Any]] = fixture["lineage"]
        for e in raw:
            e["__typename"] = TYPENAMES[e["type"]]
        for e in raw:
            for key, value in list(e.items()):
                if not key.startswith("_"):
                    e[key] = self._resolve(value)
            self._attach(e)

    def _resolve(self, value: Any) -> Any:
        if isinstance(value, dict):
            if set(value) == {"$ref"}:
                target = self.by_urn.get(value["$ref"])
                if target is None:
                    raise ValueError(f"fixture references unknown urn {value['$ref']}")
                return target
            return {k: self._resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve(v) for v in value]
        return value

    def _attach(self, e: dict[str, Any]) -> None:
        urn = e["urn"]
        e["lineage"] = lambda info, input: self.lineage_result(urn, input)
        if e["type"] == "DATASET":
            profiles = e.pop("_profiles", [])
            operations = e.pop("_operations", [])
            usage = e.pop("_usage", {})
            assertions = [self._assertion(a) for a in e.pop("_assertions", [])]
            incidents = [
                self._resolve({**i, "__typename": "Incident", "entity": {"$ref": urn}}) for i in e.pop("_incidents", [])
            ]
            e["datasetProfiles"] = lambda info, startTimeMillis=None, endTimeMillis=None, filter=None, limit=None: (
                _window(profiles, startTimeMillis, endTimeMillis, limit)
            )
            e["operations"] = lambda info, startTimeMillis=None, endTimeMillis=None, filter=None, limit=None: _window(
                operations, startTimeMillis, endTimeMillis, limit
            )
            e["usageStats"] = lambda info, resource=None, range=None, startTimeMillis=None, timeZone=None: (
                {"buckets": [], "aggregations": self._resolve(usage[range])} if range in usage else None
            )
            e["assertions"] = lambda info, start=0, count=20, includeSoftDeleted=None: {
                "start": start,
                "count": len(assertions[start : start + count]),
                "total": len(assertions),
                "assertions": assertions[start : start + count],
            }

            def incidents_result(
                info: Any,
                state: Any = None,
                stage: Any = None,
                priority: Any = None,
                assigneeUrns: Any = None,
                start: int = 0,
                count: int = 20,
            ) -> dict[str, Any]:
                hits = [i for i in incidents if state is None or i["status"]["state"] == state]
                return {
                    "start": start,
                    "count": len(hits[start : start + count]),
                    "total": len(hits),
                    "incidents": hits[start : start + count],
                }

            e["incidents"] = incidents_result
        if e["type"] == "DOMAIN":
            e["entities"] = lambda info, input=None: {
                "start": 0,
                "count": 0,
                "total": sum(1 for x in self.by_urn.values() if self._domain(x) == urn),
                "searchResults": [],
            }

    def _assertion(self, a: dict[str, Any]) -> dict[str, Any]:
        events = a.pop("_runEvents", [])
        out = self._resolve(a)
        out["__typename"] = "Assertion"

        def run_events(
            info: Any,
            status: Any = None,
            startTimeMillis: Any = None,
            endTimeMillis: Any = None,
            filter: Any = None,
            limit: Any = None,
        ) -> dict[str, Any]:
            hits = _window(events, startTimeMillis, endTimeMillis, limit)
            kinds = [h["result"]["type"] for h in hits]
            return {
                "total": len(hits),
                "failed": kinds.count("FAILURE"),
                "succeeded": kinds.count("SUCCESS"),
                "errored": kinds.count("ERROR"),
                "runEvents": hits,
            }

        out["runEvents"] = run_events
        return out

    # -------------------------------------------------------------- search index

    @staticmethod
    def _domain(e: dict[str, Any]) -> str | None:
        d = e.get("domain")
        return d["domain"]["urn"] if isinstance(d, dict) and d.get("domain") else None

    def index(self, e: dict[str, Any]) -> dict[str, set[str]]:
        """The facet values DataHub would index for ``e``."""

        def urns(container: Any, list_key: str, item_key: str) -> set[str]:
            items = (container or {}).get(list_key) or [] if isinstance(container, dict) else []
            return {i[item_key]["urn"] for i in items if i.get(item_key)}

        platform = e.get("platform")
        return {
            "urn": {e["urn"]},
            "platform": {platform["urn"]} if isinstance(platform, dict) else set(),
            "origin": {e["origin"]} if e.get("origin") else set(),
            "domains": {d} if (d := self._domain(e)) else set(),
            "container": {e["container"]["urn"]} if isinstance(e.get("container"), dict) else set(),
            "tags": urns(e.get("tags"), "tags", "tag"),
            "glossaryTerms": urns(e.get("glossaryTerms"), "terms", "term"),
            "owners": urns(e.get("ownership"), "owners", "owner"),
        }

    @staticmethod
    def _text(e: dict[str, Any]) -> dict[str, str]:
        props = e.get("properties") or {}
        fields = [f["fieldPath"] for f in (e.get("schemaMetadata") or {}).get("fields") or []]
        return {
            "name": " ".join(str(x) for x in (e.get("name"), props.get("name"), e.get("username")) if x),
            "description": str(props.get("description") or props.get("definition") or ""),
            "fieldPaths": " ".join(fields),
        }

    def matches(
        self,
        e: dict[str, Any],
        or_filters: list[dict[str, Any]] | None,
        allowed: set[str],
        extra: dict[str, set[str]] | None = None,
    ) -> bool:
        """Evaluate DataHub ``orFilters`` (OR of ANDs of facet filters) against ``e``."""
        if not or_filters:
            return True
        idx = {**self.index(e), **(extra or {})}
        for conj in or_filters:
            ok = True
            for f in conj.get("and") or []:
                if f["field"] not in allowed:
                    raise MockError(f"unknown search facet field {f['field']!r}")
                condition = f.get("condition") or "EQUAL"
                if condition not in ("EQUAL", "IN"):
                    raise MockError(f"mock does not implement filter condition {condition}")
                hit = bool(idx.get(f["field"], set()) & set(f.get("values") or []))
                if hit == bool(f.get("negated")):
                    ok = False
                    break
            if ok:
                return True
        return False

    def search(self, inp: dict[str, Any]) -> list[tuple[dict[str, Any], list[dict[str, str]]]]:
        """Entities matching a search/scroll input, sorted by URN, with matched fields."""
        types = set(inp.get("types") or TYPENAMES)
        query = (inp.get("query") or "*").strip()
        out = []
        for urn in sorted(self.by_urn):
            e = self.by_urn[urn]
            if e["type"] not in types or not self.matches(e, inp.get("orFilters"), SEARCH_FIELDS):
                continue
            matched: list[dict[str, str]] = []
            if query not in ("", "*"):
                for name, text in self._text(e).items():
                    if query.lower() in text.lower():
                        matched.append({"name": name, "value": text})
                if not matched:
                    continue
            out.append((e, matched))
        return out

    # -------------------------------------------------------------- lineage

    def neighbours(self, urn: str, direction: str) -> list[dict[str, Any]]:
        """Direct edges of ``urn`` in ``direction`` as (other urn, edge)."""
        if direction == "UPSTREAM":
            return [{"other": x["upstream"], **x} for x in self.edges if x["downstream"] == urn]
        return [{"other": x["downstream"], **x} for x in self.edges if x["upstream"] == urn]

    def lineage_result(self, urn: str, inp: dict[str, Any]) -> dict[str, Any]:
        """``EntityLineageResult`` for one hop."""
        rels = [
            {
                "type": n["type"],
                "entity": self.by_urn[n["other"]],
                "degree": 1,
                "createdOn": n["createdOn"],
                "paths": [],
            }
            for n in self.neighbours(urn, inp["direction"])
        ]
        start, count = inp.get("start") or 0, inp.get("count") or 100
        page = rels[start : start + count]
        return {"start": start, "count": len(page), "total": len(rels), "filtered": 0, "relationships": page}

    def traverse(self, urn: str, direction: str) -> list[tuple[dict[str, Any], int, list[str]]]:
        """BFS from ``urn``: (entity, degree, shortest path of urns)."""
        seen = {urn: [urn]}
        queue = deque([urn])
        out = []
        while queue:
            cur = queue.popleft()
            for n in self.neighbours(cur, direction):
                nxt = n["other"]
                if nxt in seen:
                    continue
                seen[nxt] = [*seen[cur], nxt]
                out.append((self.by_urn[nxt], len(seen[nxt]) - 1, seen[nxt]))
                queue.append(nxt)
        return sorted(out, key=lambda t: (t[1], t[0]["urn"]))


GRAPH = Graph(FIXTURE)


def _scroll_page(items: list[Any], inp: dict[str, Any]) -> tuple[list[Any], str | None]:
    count = inp.get("count")
    if count is None or not 0 < int(count) <= MAX_COUNT:
        raise MockError(f"scroll count must be 1..{MAX_COUNT}, got {count}")
    offset = 0
    if inp.get("scrollId"):
        try:
            offset = int(json.loads(base64.b64decode(inp["scrollId"]))["o"])
        except (ValueError, KeyError) as err:
            raise MockError(f"invalid scrollId {inp['scrollId']!r}") from err
    page = items[offset : offset + int(count)]
    nxt = offset + int(count)
    return page, (base64.b64encode(json.dumps({"o": nxt}).encode()).decode() if nxt < len(items) else None)


def make_root(strict_pushdown: bool) -> dict[str, Callable[..., Any]]:
    """Root ``Query`` resolvers."""

    def scroll_across_entities(info: Any, input: dict[str, Any]) -> dict[str, Any]:
        if strict_pushdown and "DATASET" in (input.get("types") or []) and not input.get("orFilters"):
            raise MockError("strict mock: unfiltered DATASET scroll refused (expected a pushed-down filter)")
        hits = GRAPH.search(input)
        page, nxt = _scroll_page(hits, input)
        return {
            "nextScrollId": nxt,
            "count": len(page),
            "total": len(hits),
            "searchResults": [
                {"entity": e, "matchedFields": m, "insights": [], "extraProperties": []} for e, m in page
            ],
            "facets": [],
        }

    def entities(info: Any, urns: list[str], checkForExistence: Any = None) -> list[Any]:
        return [GRAPH.by_urn.get(u) for u in urns]

    def scroll_across_lineage(info: Any, input: dict[str, Any]) -> dict[str, Any]:
        start = GRAPH.by_urn.get(input.get("urn") or "")
        if start is None:
            raise MockError(f"unknown urn {input.get('urn')!r}")
        types = set(input.get("types") or TYPENAMES)
        hits = []
        for e, degree, path in GRAPH.traverse(start["urn"], input["direction"]):
            if e["type"] not in types:
                continue
            extra = {"degree": {str(degree) if degree < 3 else "3+"}}
            if not GRAPH.matches(e, input.get("orFilters"), LINEAGE_FIELDS, extra):
                continue
            hits.append((e, degree, path))
        page, nxt = _scroll_page(hits, input)
        return {
            "nextScrollId": nxt,
            "count": len(page),
            "total": len(hits),
            "searchResults": [
                {
                    "entity": e,
                    "degree": d,
                    "degrees": [d],
                    "paths": [{"path": [GRAPH.by_urn[u] for u in p]}],
                    "matchedFields": [],
                    "insights": [],
                    "explored": True,
                    "truncatedChildren": False,
                    "ignoredAsHop": False,
                }
                for e, d, p in page
            ],
            "facets": [],
        }

    return {
        "scrollAcrossEntities": scroll_across_entities,
        "entities": entities,
        "scrollAcrossLineage": scroll_across_lineage,
    }


# ----------------------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    """POST /api/graphql and /api/v2/graphql, GET /__requests, POST /__reset."""

    token: str | None = None
    flaky = False
    strict_pushdown = False
    log: list[dict[str, Any]] = []
    seen: set[str] = set()
    lock = threading.Lock()

    def log_message(self, format: str, *args: Any) -> None:
        """Quiet: tests read the request log instead."""

    def _send(self, code: int, body: Any, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        """Request log / health."""
        if self.path == "/__requests":
            with self.lock:
                self._send(200, list(self.log))
        elif self.path == "/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        """Execute one GraphQL operation."""
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        if self.path == "/__reset":
            with self.lock:
                self.log.clear()
            self._send(200, {"ok": True})
            return
        if self.path not in ("/api/graphql", "/api/v2/graphql"):
            self._send(404, {"error": f"no route {self.path}"})
            return
        if self.token and self.headers.get("Authorization") != f"Bearer {self.token}":
            self._send(401, {"message": "Unauthorized to perform this action. Please log in."})
            return
        digest = hashlib.sha256(raw).hexdigest()
        if self.flaky:
            with self.lock:
                first = digest not in self.seen
                self.seen.add(digest)
            if first and int(digest[:8], 16) % 3 == 0:
                self._send(429, {"message": "rate limited"}, {"Retry-After": "0"})
                return
        try:
            body = json.loads(raw)
            self._send(200, self.execute(body))
        except MockError as err:
            self._send(200, {"errors": [{"message": str(err)}], "data": None})
        except (ValueError, KeyError, TypeError) as err:
            self._send(400, {"message": f"bad request: {err}"})

    def execute(self, body: dict[str, Any]) -> dict[str, Any]:
        """Validate and run one request against the schema and fixture."""
        op = body.get("operationName")
        if op not in ALLOWED_OPERATIONS:
            raise MockError(f"unexpected operationName {op!r}")
        document = parse(body["query"])
        names = [d.name.value for d in document.definitions if isinstance(d, OperationDefinitionNode) and d.name]
        if names != [op]:
            raise MockError(f"operationName {op!r} does not match the document's operations {names}")
        errors = validate(SCHEMA, document)
        if errors:
            raise MockError("; ".join(e.message for e in errors))
        variables = body.get("variables") or {}
        with self.lock:
            self.log.append({"operationName": op, "variables": variables})
        result = graphql_sync(
            SCHEMA,
            body["query"],
            root_value=make_root(self.strict_pushdown),
            variable_values=variables,
            operation_name=op,
        )
        out: dict[str, Any] = {"data": result.data}
        if result.errors:
            out["errors"] = [_error(e) for e in result.errors]
        return out


def _error(err: GraphQLError) -> dict[str, Any]:
    original = err.original_error
    message = str(original) if isinstance(original, MockError) else err.message
    return {"message": message, "path": err.path}


def main() -> None:
    """Serve until killed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    Handler.token = os.environ.get("MOCK_TOKEN") or None
    Handler.flaky = os.environ.get("MOCK_FLAKY") == "1"
    Handler.strict_pushdown = os.environ.get("MOCK_STRICT_PUSHDOWN") == "1"
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()
    sys.exit(0)


if __name__ == "__main__":
    main()
