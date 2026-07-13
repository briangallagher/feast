import logging
from typing import Optional

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from feast import FeatureStore
from feast.api.catalog.credentials import STSCredentialVender
from feast.api.catalog.errors import (
    NamespaceNotFoundException,
    TableAlreadyExistsException,
    TableNotFoundException,
)
from feast.api.catalog.mapping import (
    CATALOG_MANAGED_TAG,
    saved_dataset_to_load_table_response,
)
from feast.api.catalog.metadata_reader import IcebergMetadataReader
from feast.api.catalog.models import (
    CreateTableRequest,
    ListTablesResponse,
    LoadTableResponse,
    RenameTableRequest,
    TableIdentifier,
    UpdateTableRequest,
)
from feast.api.catalog.namespaces import DEFAULT_SCHEMA, resolve_namespace
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

TABLE_ASSET_TYPE = "table"


def _is_table(ds: SavedDataset) -> bool:
    return ds.tags.get("asset_type") == TABLE_ASSET_TYPE


def get_table_router(
    store: FeatureStore,
    credential_vender: Optional[STSCredentialVender] = None,
    metadata_reader: Optional[IcebergMetadataReader] = None,
) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-tables"])

    def _ensure_namespace_exists(namespace: str) -> None:
        try:
            store.registry.get_project(namespace, allow_cache=True)
        except FeastObjectNotFoundException:
            raise NamespaceNotFoundException(namespace)

    @router.get("/namespaces/{namespace}/namespaces/{schema}/tables")
    def list_tables(namespace: str, schema: str) -> ListTablesResponse:
        project = resolve_namespace(namespace, schema)
        _ensure_namespace_exists(project)
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": TABLE_ASSET_TYPE},
            namespace=schema,
        )
        return ListTablesResponse(
            identifiers=[
                TableIdentifier(
                    namespace=[namespace, ds.namespace or DEFAULT_SCHEMA], name=ds.name
                )
                for ds in datasets
            ]
        )

    @router.post("/namespaces/{namespace}/namespaces/{schema}/tables", status_code=200)
    def create_table(
        namespace: str, schema: str, request: CreateTableRequest
    ) -> LoadTableResponse:
        project = resolve_namespace(namespace, schema)
        _ensure_namespace_exists(project)

        try:
            existing = store.registry.get_saved_dataset(
                request.name, project=project, allow_cache=False, namespace=schema
            )
            if _is_table(existing):
                raise TableAlreadyExistsException(
                    f"{namespace}.{schema}", request.name
                )
        except FeastObjectNotFoundException:
            pass

        columns = []
        if request.schema_ and request.schema_.fields:
            columns = [
                {"name": f.name, "type": f.type, "nullable": not f.required}
                for f in request.schema_.fields
            ]

        location = (
            request.location
            or f"feast://{namespace}/{schema}/tables/{request.name}"
        )
        properties = dict(request.properties) if request.properties else {}
        properties[CATALOG_MANAGED_TAG] = "true"
        properties["asset_type"] = TABLE_ASSET_TYPE
        properties["location"] = location

        ds = SavedDataset(
            name=request.name,
            tags=properties,
            namespace=schema,
            columns=columns,
        )
        store.registry.apply_saved_dataset(ds, project=project, commit=True)
        return saved_dataset_to_load_table_response(ds, namespace)

    @router.get("/namespaces/{namespace}/namespaces/{schema}/tables/{table}")
    def load_table(namespace: str, schema: str, table: str):
        project = resolve_namespace(namespace, schema)
        _ensure_namespace_exists(project)
        try:
            ds = store.registry.get_saved_dataset(
                table, project=project, allow_cache=True, namespace=schema
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        if not _is_table(ds):
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        location = ds.tags.get("location", "")

        if metadata_reader and location.startswith("s3://"):
            catalog_properties = {
                k: v
                for k, v in ds.tags.items()
                if k not in (CATALOG_MANAGED_TAG, "asset_type", "location")
            }
            real_response = metadata_reader.build_load_table_response(
                location, table_properties=catalog_properties
            )
            if real_response:
                if credential_vender:
                    try:
                        vended = credential_vender.vend(location)
                        real_response["config"].update(vended)
                    except Exception as e:
                        logger.warning("STS vending failed for %s: %s", table, e)
                return JSONResponse(content=real_response)
            logger.warning(
                "Could not read Iceberg metadata for %s at %s — falling back to synthetic",
                table, location,
            )

        result = saved_dataset_to_load_table_response(ds, namespace)
        if credential_vender and location.startswith("s3://"):
            try:
                vended = credential_vender.vend(location)
                result.config.update(vended)
            except Exception as e:
                logger.warning("STS vending failed for %s: %s", table, e)
        return result

    @router.head("/namespaces/{namespace}/namespaces/{schema}/tables/{table}")
    def table_exists(namespace: str, schema: str, table: str) -> Response:
        project = resolve_namespace(namespace, schema)
        _ensure_namespace_exists(project)
        try:
            ds = store.registry.get_saved_dataset(
                table, project=project, allow_cache=True, namespace=schema
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        if not _is_table(ds):
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        return Response(status_code=204)

    @router.delete(
        "/namespaces/{namespace}/namespaces/{schema}/tables/{table}", status_code=204
    )
    def drop_table(namespace: str, schema: str, table: str) -> Response:
        project = resolve_namespace(namespace, schema)
        _ensure_namespace_exists(project)
        try:
            ds = store.registry.get_saved_dataset(
                table, project=project, allow_cache=False, namespace=schema
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        if not _is_table(ds):
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        store.registry.delete_saved_dataset(
            table, project=project, commit=True, namespace=schema
        )
        return Response(status_code=204)

    @router.put("/namespaces/{namespace}/namespaces/{schema}/tables/{table}")
    def update_table(
        namespace: str, schema: str, table: str, request: UpdateTableRequest
    ) -> LoadTableResponse:
        project = resolve_namespace(namespace, schema)
        _ensure_namespace_exists(project)
        try:
            ds = store.registry.get_saved_dataset(
                table, project=project, allow_cache=False, namespace=schema
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(f"{namespace}.{schema}", table)
        if not _is_table(ds):
            raise TableNotFoundException(f"{namespace}.{schema}", table)

        tags = dict(ds.tags)
        if request.properties:
            tags.update(request.properties)
        if request.data_source_format is not None:
            tags["format"] = request.data_source_format
        if request.location is not None:
            tags["location"] = request.location

        columns = ds.columns
        if request.schema_ and request.schema_.fields:
            columns = [
                {"name": f.name, "type": f.type, "nullable": not f.required}
                for f in request.schema_.fields
            ]

        data_source_ref = ds.data_source_ref
        if request.data_source_ref is not None:
            data_source_ref = request.data_source_ref

        updated = SavedDataset(
            name=table,
            tags=tags,
            namespace=schema,
            columns=columns,
            data_source_ref=data_source_ref,
        )
        updated.created_timestamp = ds.created_timestamp
        store.registry.apply_saved_dataset(updated, project=project, commit=True)
        return saved_dataset_to_load_table_response(updated, namespace)

    @router.post("/tables/rename", status_code=200)
    def rename_table(request: RenameTableRequest) -> None:
        src_ns = request.source.namespace[0] if request.source.namespace else ""
        src_schema = (
            request.source.namespace[1]
            if len(request.source.namespace) > 1
            else DEFAULT_SCHEMA
        )
        dst_ns = (
            request.destination.namespace[0] if request.destination.namespace else ""
        )
        dst_schema = (
            request.destination.namespace[1]
            if len(request.destination.namespace) > 1
            else DEFAULT_SCHEMA
        )

        _ensure_namespace_exists(src_ns)
        if dst_ns != src_ns:
            _ensure_namespace_exists(dst_ns)

        try:
            src_ds = store.registry.get_saved_dataset(
                request.source.name,
                project=src_ns,
                allow_cache=False,
                namespace=src_schema,
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(src_ns, request.source.name)
        if not _is_table(src_ds):
            raise TableNotFoundException(src_ns, request.source.name)

        try:
            existing = store.registry.get_saved_dataset(
                request.destination.name,
                project=dst_ns,
                allow_cache=False,
                namespace=dst_schema,
            )
            if _is_table(existing):
                raise TableAlreadyExistsException(dst_ns, request.destination.name)
        except FeastObjectNotFoundException:
            pass

        new_ds = SavedDataset(
            name=request.destination.name,
            tags=dict(src_ds.tags),
            namespace=dst_schema,
            columns=src_ds.columns,
            data_source_ref=src_ds.data_source_ref,
        )
        store.registry.apply_saved_dataset(new_ds, project=dst_ns, commit=True)
        store.registry.delete_saved_dataset(
            request.source.name, project=src_ns, commit=True, namespace=src_schema
        )

    return router
