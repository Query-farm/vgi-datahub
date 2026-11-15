"""DataHub GraphQL client: endpoint from a ``datahub`` DuckDB secret, retries, errors.

Every table and function reads one ``datahub`` secret::

    -- self-hosted DataHub: the GMS (metadata service) URL
    CREATE SECRET datahub (TYPE datahub, GMS_URL 'http://localhost:8080', TOKEN '<personal access token>');
    -- DataHub Cloud (Acryl): the tenant's /gms endpoint
    CREATE SECRET datahub (TYPE datahub, GMS_URL 'https://acme.acryl.io/gms', TOKEN '...');
    -- only the frontend is reachable: GraphQL goes to <FRONTEND_URL>/api/v2/graphql
    CREATE SECRET datahub (TYPE datahub, FRONTEND_URL 'http://localhost:9002', TOKEN '...');

``GMS_URL`` takes precedence for API calls (``<GMS_URL>/api/graphql``). ``FRONTEND_URL``
is also used to build each row's ``url`` (a link into the DataHub UI); for a DataHub
Cloud ``.../gms`` URL the frontend is derived by dropping ``/gms``. ``PAGE_SIZE``
(default 100) is the scroll ``count``. The token is sent as ``Authorization: Bearer``,
read on every call and never kept in table-function state (which can travel back to
DuckDB over HTTP).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi.arguments import SecretLookupEntry
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

SECRET_TYPE = "datahub"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 6
MAX_BACKOFF_SECONDS = 60.0
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
USER_AGENT = "vgi-datahub-worker/0.1"
DOC_URL = "https://github.com/Query-farm/vgi-datahub#connect-to-datahub"


def _doc_field(name: str, doc: str, *, redact: bool = False) -> pa.Field[Any]:
    metadata = {"vgi_doc": doc}
    if redact:
        metadata["redact"] = "true"
    return pa.field(name, pa.string(), metadata=metadata)


DATAHUB_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description=(
        "Connection to a DataHub (open-source or DataHub Cloud) metadata service: GMS_URL and/or "
        "FRONTEND_URL plus a personal access TOKEN."
    ),
    schema=pa.schema(
        [
            _doc_field(
                "gms_url",
                "DataHub GMS (metadata service) root, e.g. 'http://localhost:8080' or "
                "'https://<tenant>.acryl.io/gms'. GraphQL is POSTed to <gms_url>/api/graphql.",
            ),
            _doc_field(
                "frontend_url",
                "DataHub UI root, e.g. 'http://localhost:9002'. Used for row links (url column); when "
                "GMS_URL is unset, GraphQL goes to <frontend_url>/api/v2/graphql.",
            ),
            _doc_field(
                "token",
                "Personal access token (Settings > Access Tokens), sent as 'Authorization: Bearer <token>'. "
                "Leave unset only for a DataHub with metadata-service authentication disabled.",
                redact=True,
            ),
            _doc_field("page_size", "Entities requested per scroll page; 1 to 1000, default 100."),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)

# Every scan asks DuckDB for the datahub secret at bind time.
SECRET_REQUIREMENT = [SecretLookupEntry(secret_type=SECRET_TYPE)]


class DataHubError(RuntimeError):
    """DataHub returned an error, or the worker is not configured to reach it."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    """GraphQL URL, UI root, page size and bearer token (never logged)."""

    graphql_url: str
    frontend_url: str | None
    token: str | None
    page_size: int

    def entity_url(self, urn: str | None, entity_type: str | None) -> str | None:
        """Link to an entity's page in the DataHub UI, or None without a known frontend."""
        if not urn or not self.frontend_url:
            return None
        path = UI_PATHS.get(entity_type or "", "entity")
        return f"{self.frontend_url}/{path}/{urllib.parse.quote(urn, safe='')}"


# GraphQL EntityType -> the DataHub UI route segment.
UI_PATHS = {
    "DATASET": "dataset",
    "DASHBOARD": "dashboard",
    "CHART": "chart",
    "DATA_FLOW": "pipelines",
    "DATA_JOB": "tasks",
    "CONTAINER": "container",
    "DOMAIN": "domain",
    "DATA_PRODUCT": "dataProduct",
    "GLOSSARY_TERM": "glossaryTerm",
    "GLOSSARY_NODE": "glossaryNode",
    "TAG": "tag",
    "CORP_USER": "user",
    "CORP_GROUP": "group",
    "MLMODEL": "mlModels",
    "MLMODEL_GROUP": "mlModelGroup",
    "MLFEATURE_TABLE": "featureTables",
    "MLFEATURE": "features",
    "NOTEBOOK": "notebook",
}


def _field(secret: dict[str, Any], name: str) -> str | None:
    value = secret.get(name)
    if value is None:
        return None
    py = value.as_py() if hasattr(value, "as_py") else value
    text = str(py).strip() if py is not None else ""
    return text or None


def _http_root(value: str, name: str) -> str:
    if not value.startswith(("http://", "https://")):
        raise DataHubError(f"datahub secret {name} must start with http:// or https://, got {value!r}")
    return value.rstrip("/")


def endpoint_from_secret(secret: dict[str, Any] | None) -> Endpoint:
    """Resolve the GraphQL endpoint from a ``datahub`` secret's fields.

    Args:
        secret: The resolved secret's fields (Arrow scalars or plain values), or None.

    Returns:
        The endpoint.

    Raises:
        DataHubError: No secret, neither URL set, a malformed URL, or a bad PAGE_SIZE.

    """
    if not secret:
        raise DataHubError(
            "no datahub secret found. Create one first: "
            "CREATE SECRET (TYPE datahub, GMS_URL 'http://localhost:8080', TOKEN '<personal access token>')"
        )
    gms = _field(secret, "gms_url")
    frontend = _field(secret, "frontend_url")
    if not gms and not frontend:
        raise DataHubError("the datahub secret needs GMS_URL (e.g. 'http://localhost:8080') or FRONTEND_URL")
    gms_root = _http_root(gms, "gms_url").removesuffix("/api/graphql") if gms else None
    front_root = _http_root(frontend, "frontend_url").removesuffix("/api/v2/graphql") if frontend else None
    if gms_root:
        graphql_url = f"{gms_root}/api/graphql"
        if not front_root and gms_root.endswith("/gms"):
            front_root = gms_root.removesuffix("/gms")  # DataHub Cloud: https://<tenant>.acryl.io/gms
    else:
        graphql_url = f"{front_root}/api/v2/graphql"
    raw_size = _field(secret, "page_size")
    try:
        page_size = int(raw_size) if raw_size else DEFAULT_PAGE_SIZE
    except ValueError as err:
        raise DataHubError(f"datahub secret page_size must be an integer, got {raw_size!r}") from err
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise DataHubError(f"datahub secret page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}")
    return Endpoint(
        graphql_url=graphql_url, frontend_url=front_root, token=_field(secret, "token"), page_size=page_size
    )


def endpoint(secrets: Any) -> Endpoint:
    """Resolve the endpoint from a call's resolved secrets (``params.secrets``).

    Args:
        secrets: The call's ``ResolvedSecrets``.

    Returns:
        The endpoint.

    Raises:
        DataHubError: Several different datahub secrets apply, or none is usable.

    """
    found = list(secrets.of_type(SECRET_TYPE)) if secrets is not None else []
    keys = ("gms_url", "frontend_url", "token", "page_size")
    distinct = {tuple(_field(s, f) for f in keys): s for s in found}
    found = list(distinct.values())
    if len(found) > 1:
        raise DataHubError("more than one datahub secret is defined; keep one per session (DROP SECRET the others)")
    return endpoint_from_secret(found[0] if found else None)


def retry_delay(retry_after: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt``: ``Retry-After`` if given, else exponential backoff."""
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_BACKOFF_SECONDS)
        except ValueError:
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def graphql(ep: Endpoint, operation: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    """POST one named GraphQL operation and return its ``data``.

    429 and 5xx responses are retried with exponential backoff honouring ``Retry-After``.

    Args:
        ep: The endpoint.
        operation: The operation name (also sent as ``operationName``).
        query: The GraphQL document.
        variables: The operation's variables.

    Returns:
        The response's ``data`` object.

    Raises:
        DataHubError: HTTP failure after retries, a non-retryable status, or GraphQL errors.

    """
    body = json.dumps({"operationName": operation, "query": query, "variables": variables}).encode()
    headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": USER_AGENT}
    if ep.token:
        headers["Authorization"] = f"Bearer {ep.token}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(ep.graphql_url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            break
        except urllib.error.HTTPError as err:
            if (err.code != 429 and err.code < 500) or attempt == MAX_ATTEMPTS:
                detail = err.read(300).decode(errors="replace").strip()
                hint = " (check the datahub secret's TOKEN)" if err.code in (401, 403) else ""
                raise DataHubError(f"DataHub {operation} returned HTTP {err.code}{hint}: {detail}") from err
            delay = retry_delay(err.headers.get("Retry-After") if err.headers else None, attempt)
            log.info("datahub: HTTP %s on %s, retrying in %.1fs", err.code, operation, delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                raise DataHubError(f"cannot reach DataHub at {ep.graphql_url}: {getattr(err, 'reason', err)}") from err
            time.sleep(retry_delay(None, attempt))
    else:  # pragma: no cover - the loop always breaks or raises
        raise DataHubError(f"DataHub {operation} failed")
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if errors:
        messages = "; ".join(str(e.get("message", e)) if isinstance(e, dict) else str(e) for e in errors[:3])
        raise DataHubError(f"DataHub {operation} failed: {messages}")
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise DataHubError(f"DataHub {operation} returned no data")
    return data
