from fastapi import FastAPI

from feast import FeatureStore
from feast.api.catalog.credentials import create_vender_from_env
from feast.api.catalog.errors import register_iceberg_exception_handlers
from feast.api.catalog.metadata_reader import create_reader_from_env
from feast.api.catalog.models import CatalogConfig
from feast.api.catalog.namespaces import get_namespace_router
from feast.api.catalog.search import get_search_router
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
