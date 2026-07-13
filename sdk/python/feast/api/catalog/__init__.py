from fastapi import FastAPI

from feast import FeatureStore
from feast.api.catalog.credentials import create_vender_from_env
from feast.api.catalog.errors import register_iceberg_exception_handlers
from feast.api.catalog.metadata_reader import create_reader_from_env
from feast.api.catalog.models import CatalogConfig
from feast.api.catalog.namespaces import get_namespace_router
from feast.api.catalog.search import get_search_router
from feast.api.catalog.tables import get_table_router
from feast.api.catalog.volumes import get_volume_router


def add_catalog_routes(app: FastAPI, store: FeatureStore) -> None:
    register_iceberg_exception_handlers(app)

    credential_vender = create_vender_from_env()
    metadata_reader = create_reader_from_env()

    prefix = "/v1"

    @app.get(f"{prefix}/config")
    def catalog_config() -> CatalogConfig:
        return CatalogConfig()

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
