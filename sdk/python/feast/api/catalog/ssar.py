"""SSAR (SelfSubjectAccessReview) middleware for the Iceberg REST Catalog API.

Intercepts every request to /v1/* endpoints, checks the caller's bearer token
against pseudo-resources in the datacatalog.opendatahub.io API group, and returns
403 if the user lacks the required RoleBinding.

Uses the same pattern proven in the RHOAI MLflow deployment (kubernetes-auth plugin).

Design ref: design/04-rbac.md §4
"""

import logging
import os
import re
import time
from typing import Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

logger = logging.getLogger(__name__)

API_GROUP = "datacatalog.opendatahub.io"

SSAR_CACHE_TTL = int(os.environ.get("SSAR_CACHE_TTL", "30"))
SSAR_CACHE_MAX_SIZE = int(os.environ.get("SSAR_CACHE_MAX_SIZE", "10000"))

HTTP_TO_K8S_VERB = {
    "GET": "get",
    "HEAD": "get",
    "POST": "create",
    "PUT": "update",
    "PATCH": "update",
    "DELETE": "delete",
}

PATH_PATTERN = re.compile(
    r"^/v1/(?P<prefix>[^/]+)"
    r"(?:/namespaces/[^/]+"
    r"(?:/(?P<resource>tables|volumes)(?:/[^/]+)?)?"
    r")?"
    r"(?:/(?P<top_resource>namespaces|search|config|tables))?",
    re.IGNORECASE,
)

RESOURCE_CONTEXT_PATTERN = re.compile(
    r"^/v1/[^/]+/namespaces(?:/[^/]+)?(?:/(?P<resource>tables|volumes))?",
)


def _extract_prefix(path: str) -> Optional[str]:
    """Extract the {prefix} segment from /v1/{prefix}/..."""
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "v1":
        return parts[1]
    return None


def _map_to_resource(path: str) -> str:
    """Determine the SSAR resource from the request path.

    Routes:
      /v1/{prefix}/config                                → namespaces
      /v1/{prefix}/namespaces...                         → namespaces
      /v1/{prefix}/namespaces/{ns}/tables...              → tables
      /v1/{prefix}/namespaces/{ns}/volumes...             → volumes
      /v1/{prefix}/search                                → tables (list permission)
      /v1/{prefix}/tables/rename                         → tables
    """
    parts = path.strip("/").split("/")
    for i, part in enumerate(parts):
        if part in ("tables", "volumes"):
            return part
    if "search" in parts:
        return "tables"
    return "namespaces"


def _map_to_verb(method: str, path: str) -> str:
    """Map HTTP method + path to K8s SSAR verb.

    Special cases:
      - GET on collection endpoints (list) → "list"
      - GET on item endpoints (get) → "get"
      - POST on .../tables/{t} (update) → "update"
      - POST on .../tables (create) → "create"
      - Search → "list"
    """
    parts = path.strip("/").split("/")

    if "search" in parts:
        return "list"

    if method == "GET":
        resource = _map_to_resource(path)
        if resource in ("tables", "volumes"):
            resource_idx = None
            for i, p in enumerate(parts):
                if p == resource:
                    resource_idx = i
                    break
            if resource_idx is not None and resource_idx + 1 < len(parts):
                return "get"
            return "list"
        if resource == "namespaces":
            for i, p in enumerate(parts):
                if p == "namespaces":
                    if i + 1 < len(parts) and parts[i + 1] != "namespaces":
                        return "get"
            return "list"

    if method == "HEAD":
        return "get"

    if method == "POST":
        resource = _map_to_resource(path)
        if resource in ("tables", "volumes"):
            resource_key = resource
            for i, p in enumerate(parts):
                if p == resource_key:
                    if i + 1 < len(parts) and parts[i + 1] != "rename":
                        return "update"
                    break
        if "properties" in parts:
            return "update"
        return "create"

    return HTTP_TO_K8S_VERB.get(method, "get")


class SSARCache:
    """In-memory TTL cache for SSAR results.

    Cache key: (token_hash, namespace, resource, verb)
    TTL: configurable, default 30s (matches MLflow implementation).
    Max size: bounded to prevent memory growth.
    """

    def __init__(self, ttl: int = SSAR_CACHE_TTL, max_size: int = SSAR_CACHE_MAX_SIZE):
        self._cache: dict[tuple, tuple[bool, float]] = {}
        self._ttl = ttl
        self._max_size = max_size

    def get(self, key: tuple) -> Optional[bool]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        allowed, ts = entry
        if time.time() - ts > self._ttl:
            del self._cache[key]
            return None
        return allowed

    def put(self, key: tuple, allowed: bool) -> None:
        if len(self._cache) >= self._max_size:
            now = time.time()
            expired = [k for k, (_, ts) in self._cache.items() if now - ts > self._ttl]
            for k in expired:
                del self._cache[k]
            if len(self._cache) >= self._max_size:
                oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
                del self._cache[oldest_key]
        self._cache[key] = (allowed, time.time())


async def _check_ssar(
    token: str,
    resource: str,
    verb: str,
    namespace: str,
) -> bool:
    """Issue a SelfSubjectAccessReview against the K8s API server.

    Uses the caller's bearer token (impersonation via Authorization header)
    to ask: "can this user perform {verb} on {resource} in {namespace}?"
    """
    try:
        from kubernetes import client as k8s_client
        from kubernetes import config as k8s_config
    except ImportError:
        logger.error("kubernetes package not installed — SSAR checks will fail")
        raise

    api_client = k8s_client.ApiClient()
    api_client.configuration = k8s_client.Configuration()

    try:
        k8s_config.load_incluster_config(client_configuration=api_client.configuration)
    except k8s_config.ConfigException:
        try:
            k8s_config.load_kube_config(client_configuration=api_client.configuration)
        except k8s_config.ConfigException:
            logger.error("Cannot configure Kubernetes client for SSAR")
            raise

    api_client.default_headers["Authorization"] = f"Bearer {token}"

    auth_api = k8s_client.AuthorizationV1Api(api_client)

    ssar_body = k8s_client.V1SelfSubjectAccessReview(
        spec=k8s_client.V1SelfSubjectAccessReviewSpec(
            resource_attributes=k8s_client.V1ResourceAttributes(
                group=API_GROUP,
                resource=resource,
                verb=verb,
                namespace=namespace,
            )
        )
    )

    try:
        result = auth_api.create_self_subject_access_review(body=ssar_body)
        return result.status.allowed
    except k8s_client.ApiException as e:
        logger.error("SSAR API call failed: %s", e)
        raise


class SSARMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware enforcing SSAR on all /v1/* catalog routes.

    Extracts the bearer token from the Authorization header, determines the
    target namespace ({prefix}) and required permission (resource + verb),
    and issues a SelfSubjectAccessReview. Caches results per-process with
    a configurable TTL.
    """

    def __init__(self, app: FastAPI, enabled: bool = True):
        super().__init__(app)
        self.enabled = enabled
        self.cache = SSARCache()

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if not self.enabled:
            return await call_next(request)

        path = request.url.path
        if not path.startswith("/v1/"):
            return await call_next(request)

        if path.rstrip("/").endswith("/config"):
            return await call_next(request)

        prefix = _extract_prefix(path)
        if not prefix:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={
                    "error": {
                        "message": "Authentication required. Provide a bearer token in the Authorization header.",
                        "type": "NotAuthorizedException",
                        "code": 401,
                    }
                },
            )

        token = auth_header[7:]

        resource = _map_to_resource(path)
        verb = _map_to_verb(request.method, path)

        token_hash = hash(token)
        cache_key = (token_hash, prefix, resource, verb)

        cached = self.cache.get(cache_key)
        if cached is not None:
            if cached:
                return await call_next(request)
            return _forbidden_response(prefix, resource, verb)

        try:
            allowed = await _check_ssar(token, resource, verb, prefix)
        except Exception as e:
            logger.error("SSAR check failed: %s", e)
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "message": "Authorization service unavailable",
                        "type": "ServiceUnavailableException",
                        "code": 503,
                    }
                },
            )

        self.cache.put(cache_key, allowed)

        if not allowed:
            return _forbidden_response(prefix, resource, verb)

        return await call_next(request)


def _forbidden_response(namespace: str, resource: str, verb: str) -> JSONResponse:
    return JSONResponse(
        status_code=403,
        content={
            "error": {
                "message": f"Access denied to {resource} in namespace '{namespace}'. "
                f"Required: {API_GROUP}/{resource}:{verb}",
                "type": "NotAuthorizedException",
                "code": 403,
            }
        },
    )


def add_ssar_middleware(app: FastAPI) -> None:
    """Add SSAR middleware to the FastAPI app.

    Controlled by DATACATALOG_SSAR_ENABLED env var (default: "true").
    Set to "false" to disable for local dev/testing.
    """
    enabled = os.environ.get("DATACATALOG_SSAR_ENABLED", "true").lower() != "false"
    if not enabled:
        logger.info("SSAR middleware disabled (DATACATALOG_SSAR_ENABLED=false)")
        return

    logger.info("SSAR middleware enabled (API group: %s, cache TTL: %ds)", API_GROUP, SSAR_CACHE_TTL)
    app.add_middleware(SSARMiddleware, enabled=True)
