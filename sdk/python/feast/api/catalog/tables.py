import logging
from typing import Optional

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from feast import FeatureStore
from feast.api.catalog.connections import resolve_credentials
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
from feast.api.catalog.namespaces import DEFAULT_SCHEMA, decode_namespace
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

TABLE_ASSET_TYPE = "table"

SUPPORTED_UPDATE_ACTIONS = {"set-properties", "remove-properties"}


def _is_table(ds: SavedDataset) -> bool:
    return ds.tags.get("asset_type") == TABLE_ASSET_TYPE


def get_table_router(
    store: FeatureStore,
    credential_vender: Optional[STSCredentialVender] = None,
    metadata_reader: Optional[IcebergMetadataReader] = None,
) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-tables"])

    def _ensure_project_exists(prefix: str) -> None:
        try:
            store.registry.get_project(prefix, allow_cache=True)
        except FeastObjectNotFoundException:
            raise NamespaceNotFoundException(prefix)

    @router.get("/{prefix}/namespaces/{namespace}/tables")
    def list_tables(prefix: str, namespace: str) -> ListTablesResponse:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        datasets = store.registry.list_saved_datasets(
            project=prefix,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": TABLE_ASSET_TYPE},
        )
        filtered = [ds for ds in datasets if (ds.namespace or DEFAULT_SCHEMA) == ns_name]
        return ListTablesResponse(
            identifiers=[
                TableIdentifier(
                    namespace=decode_namespace(ds.namespace or DEFAULT_SCHEMA),
                    name=ds.name,
                )
                for ds in filtered
            ]
        )

    @router.post("/{prefix}/namespaces/{namespace}/tables", status_code=200)
    def create_table(
        prefix: str, namespace: str, request: CreateTableRequest
    ) -> LoadTableResponse:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        try:
            existing = store.registry.get_saved_dataset(
                request.name, project=prefix, allow_cache=False, namespace=ns_name
            )
            if _is_table(existing):
                raise TableAlreadyExistsException(namespace, request.name)
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
            or f"feast://{prefix}/{ns_name}/tables/{request.name}"
        )
        properties = dict(request.properties) if request.properties else {}
        properties[CATALOG_MANAGED_TAG] = "true"
        properties["asset_type"] = TABLE_ASSET_TYPE
        properties["location"] = location

        ds = SavedDataset(
            name=request.name,
            tags=properties,
            namespace=ns_name,
            columns=columns,
        )
        store.registry.apply_saved_dataset(ds, project=prefix, commit=True)
        return saved_dataset_to_load_table_response(ds, prefix)

    @router.get("/{prefix}/namespaces/{namespace}/tables/{table}")
    def load_table(prefix: str, namespace: str, table: str):
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                table, project=prefix, allow_cache=True, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)
        location = ds.tags.get("location", "")

        connection_creds = resolve_credentials(ds, prefix)

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
                if connection_creds:
                    real_response["config"].update(connection_creds)
                elif credential_vender:
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

        result = saved_dataset_to_load_table_response(ds, prefix)
        if connection_creds:
            result.config.update(connection_creds)
        elif credential_vender and location.startswith("s3://"):
            try:
                vended = credential_vender.vend(location)
                result.config.update(vended)
            except Exception as e:
                logger.warning("STS vending failed for %s: %s", table, e)
        return result

    @router.head("/{prefix}/namespaces/{namespace}/tables/{table}")
    def table_exists(prefix: str, namespace: str, table: str) -> Response:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                table, project=prefix, allow_cache=True, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)
        return Response(status_code=204)

    @router.delete(
        "/{prefix}/namespaces/{namespace}/tables/{table}", status_code=204
    )
    def drop_table(prefix: str, namespace: str, table: str) -> Response:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                table, project=prefix, allow_cache=False, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)
        store.registry.delete_saved_dataset(
            table, project=prefix, commit=True, namespace=ns_name
        )
        return Response(status_code=204)

    @router.post("/{prefix}/namespaces/{namespace}/tables/{table}")
    def update_table(
        prefix: str, namespace: str, table: str, request: UpdateTableRequest
    ) -> LoadTableResponse:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                table, project=prefix, allow_cache=False, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)

        tags = dict(ds.tags)
        for update in request.updates:
            if update.action not in SUPPORTED_UPDATE_ACTIONS:
                return JSONResponse(
                    status_code=501,
                    content={
                        "error": {
                            "message": f"Update action '{update.action}' is not supported in Phase 1. "
                            f"Supported actions: {', '.join(sorted(SUPPORTED_UPDATE_ACTIONS))}",
                            "type": "UnsupportedOperationException",
                            "code": 501,
                        }
                    },
                )

            if update.action == "set-properties" and update.updates:
                tags.update(update.updates)
            elif update.action == "remove-properties" and update.removals:
                for key in update.removals:
                    tags.pop(key, None)

        updated = SavedDataset(
            name=table,
            tags=tags,
            namespace=ns_name,
            columns=ds.columns,
            data_source_ref=ds.data_source_ref,
        )
        updated.created_timestamp = ds.created_timestamp
        store.registry.apply_saved_dataset(updated, project=prefix, commit=True)
        return saved_dataset_to_load_table_response(updated, prefix)

    @router.post("/{prefix}/tables/rename", status_code=200)
    def rename_table(prefix: str, request: RenameTableRequest) -> None:
        src_ns = (
            request.source.namespace[0]
            if request.source.namespace
            else DEFAULT_SCHEMA
        )
        dst_ns = (
            request.destination.namespace[0]
            if request.destination.namespace
            else DEFAULT_SCHEMA
        )

        _ensure_project_exists(prefix)

        try:
            src_ds = store.registry.get_saved_dataset(
                request.source.name,
                project=prefix,
                allow_cache=False,
                namespace=src_ns,
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(src_ns, request.source.name)
        if not _is_table(src_ds):
            raise TableNotFoundException(src_ns, request.source.name)

        try:
            existing = store.registry.get_saved_dataset(
                request.destination.name,
                project=prefix,
                allow_cache=False,
                namespace=dst_ns,
            )
            if _is_table(existing):
                raise TableAlreadyExistsException(dst_ns, request.destination.name)
        except FeastObjectNotFoundException:
            pass

        new_ds = SavedDataset(
            name=request.destination.name,
            tags=dict(src_ds.tags),
            namespace=dst_ns,
            columns=src_ds.columns,
            data_source_ref=src_ds.data_source_ref,
        )
        store.registry.apply_saved_dataset(new_ds, project=prefix, commit=True)
        store.registry.delete_saved_dataset(
            request.source.name, project=prefix, commit=True, namespace=src_ns
        )

    return router
