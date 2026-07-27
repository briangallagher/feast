import logging
from typing import Optional

from fastapi import FastAPI, Query, Request

from feast import FeatureStore

logger = logging.getLogger(__name__)
from feast.api.catalog.catalog_api import get_catalog_api_router
from feast.api.catalog.credentials import create_vender_from_env
from feast.api.catalog.errors import register_iceberg_exception_handlers
from feast.api.catalog.metadata_reader import create_reader_from_env
from feast.api.catalog.models import CatalogConfig
from feast.api.catalog.namespaces import get_namespace_router
from feast.api.catalog.search import get_cross_project_search_router, get_search_router
from feast.api.catalog.ssar import add_ssar_middleware
from feast.api.catalog.tables import get_table_router
from feast.api.catalog.volumes import get_volume_router

CATALOG_ENDPOINTS = [
    "GET /v1/{prefix}/config",
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    # Extensions
    "GET /v1/projects",
    "GET /v1/search",
    "GET /v1/{prefix}/search",
    "GET /v1/{prefix}/namespaces/{namespace}/volumes",
    "POST /v1/{prefix}/namespaces/{namespace}/volumes",
    "GET /v1/{prefix}/namespaces/{namespace}/volumes/{volume}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/volumes/{volume}",
]


def add_catalog_routes(app: FastAPI, store: FeatureStore) -> None:
    register_iceberg_exception_handlers(app)
    add_ssar_middleware(app)

    credential_vender = create_vender_from_env()
    metadata_reader = create_reader_from_env()

    prefix = "/v1"

    @app.get(f"{prefix}/config")
    def catalog_config_bootstrap(
        warehouse: Optional[str] = Query(default=None),
    ) -> CatalogConfig:
        """Bootstrap config endpoint per Iceberg REST spec.

        PyIceberg/Spark call GET /v1/config?warehouse=X before any prefixed
        requests. The server responds with the prefix override so subsequent
        calls use /v1/{prefix}/...
        """
        pfx = warehouse or "default"
        return CatalogConfig(
            defaults={},
            overrides={"prefix": pfx},
            endpoints=CATALOG_ENDPOINTS,
        )

    @app.get(f"{prefix}/{{prefix}}/config")
    def catalog_config(prefix: str) -> CatalogConfig:
        return CatalogConfig(
            defaults={},
            overrides={"prefix": prefix},
            endpoints=CATALOG_ENDPOINTS,
        )

    app.include_router(get_namespace_router(store), prefix=prefix)
    app.include_router(
        get_table_router(
            store,
            credential_vender=credential_vender,
            metadata_reader=metadata_reader,
        ),
        prefix=prefix,
    )
    app.include_router(
        get_volume_router(store, credential_vender=credential_vender),
        prefix=prefix,
    )
    app.include_router(get_search_router(store), prefix=prefix)
    app.include_router(get_cross_project_search_router(store), prefix=prefix)

    # Proprietary Catalog API — user-facing, all asset types
    catalog_api_prefix = "/catalog"
    app.include_router(get_catalog_api_router(store), prefix=catalog_api_prefix)
    logger.info("Proprietary Catalog API mounted at %s/*", catalog_api_prefix)

    @app.get(f"{prefix}/projects")
    async def list_accessible_projects(request: Request):
        """List Feast projects, filtered by SSAR access when enabled.

        Used by the UI project dropdown to show only namespaces the user can access.
        When SSAR is disabled, returns all projects.
        """
        from feast.api.catalog.ssar import _check_ssar, _ensure_k8s_config
        import os

        all_projects = store.registry.list_projects(allow_cache=False)
        project_names = [p.name for p in all_projects]

        ssar_enabled = os.environ.get("DATACATALOG_SSAR_ENABLED", "true").lower() != "false"
        if not ssar_enabled:
            return {"projects": project_names}

        token = None
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

        if not token:
            return {"projects": project_names}

        accessible = []
        _ensure_k8s_config()
        for project_name in project_names:
            allowed = await _check_ssar(token, project_name, "namespaces", "list")
            if allowed:
                accessible.append(project_name)

        return {"projects": accessible}
