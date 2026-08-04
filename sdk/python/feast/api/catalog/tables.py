import logging
from typing import Optional

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from feast import FeatureStore
from feast.api.catalog.errors import (
    TableAlreadyExistsException,
    TableNotFoundException,
)
from feast.api.catalog.mapping import (
    CATALOG_PROJECT,
    DEFAULT_COLLECTION,
    ensure_catalog_project,
    make_scoped_name,
    parse_display_name,
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
from feast.api.catalog.namespaces import decode_namespace
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

TABLE_ASSET_TYPE = "table"

SUPPORTED_UPDATE_ACTIONS = {"set-properties", "remove-properties"}


def _is_table(ds: SavedDataset) -> bool:
    return ds.tags.get("asset_type") == TABLE_ASSET_TYPE


def get_table_router(
    store: FeatureStore,
    metadata_reader: Optional[IcebergMetadataReader] = None,
) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-tables"])

    # ------------------------------------------------------------------
    # URL mapping:
    #   prefix    = RHAI namespace  (e.g. "insurance-demo")
    #   namespace = collection      (e.g. "claims")
    #   table     = display name    (e.g. "auto-claims")
    #
    # DB name = scoped: "insurance-demo/claims/auto-claims"
    # project = CATALOG_PROJECT = "data-registry"
    # ------------------------------------------------------------------

    @router.get("/{prefix}/namespaces/{namespace}/tables")
    def list_tables(prefix: str, namespace: str) -> ListTablesResponse:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags={"asset_type": TABLE_ASSET_TYPE},
            namespace=prefix,
        )
        filtered = [
            ds
            for ds in datasets
            if (ds.collection or DEFAULT_COLLECTION) == ns_name
            and ds.tags.get("format") == "iceberg"
        ]
        return ListTablesResponse(
            identifiers=[
                TableIdentifier(
                    namespace=[ns_name],
                    name=parse_display_name(ds.name),
                )
                for ds in filtered
            ]
        )

    @router.post("/{prefix}/namespaces/{namespace}/tables", status_code=200)
    def create_table(
        prefix: str, namespace: str, request: CreateTableRequest
    ) -> LoadTableResponse:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        scoped = make_scoped_name(prefix, ns_name, request.name)

        try:
            existing = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
            if _is_table(existing):
                raise TableAlreadyExistsException(namespace, request.name)
        except FeastObjectNotFoundException:
            pass

        columns: list[dict] = []
        if request.schema_ and request.schema_.fields:
            columns = [
                {"name": f.name, "type": f.type, "nullable": not f.required}
                for f in request.schema_.fields
            ]

        location = (
            request.location or ""
        )
        properties = dict(request.properties) if request.properties else {}
        properties["asset_type"] = TABLE_ASSET_TYPE
        properties["format"] = "iceberg"
        properties["location"] = location

        ds = SavedDataset(
            name=scoped,
            tags=properties,
            namespace=prefix,
            collection=ns_name,
            columns=columns,
        )
        store.registry.apply_saved_dataset(ds, project=CATALOG_PROJECT, commit=True)
        return saved_dataset_to_load_table_response(ds, prefix)

    @router.get("/{prefix}/namespaces/{namespace}/tables/{table}")
    def load_table(prefix: str, namespace: str, table: str):
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        scoped = make_scoped_name(prefix, ns_name, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=True
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)
        if ds.tags.get("format") != "iceberg":
            raise TableNotFoundException(namespace, table)

        location = ds.tags.get("location", "")

        if metadata_reader and location.startswith("s3://"):
            catalog_properties = {
                k: v
                for k, v in ds.tags.items()
                if k not in ("asset_type", "location")
            }
            real_response = metadata_reader.build_load_table_response(
                location, table_properties=catalog_properties
            )
            if real_response:
                return JSONResponse(content=real_response)
            logger.warning(
                "Could not read Iceberg metadata for %s at %s — falling back to synthetic",
                table,
                location,
            )

        return saved_dataset_to_load_table_response(ds, prefix)

    @router.head("/{prefix}/namespaces/{namespace}/tables/{table}")
    def table_exists(prefix: str, namespace: str, table: str) -> Response:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=True
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)
        return Response(status_code=204)

    @router.delete("/{prefix}/namespaces/{namespace}/tables/{table}", status_code=204)
    def drop_table(prefix: str, namespace: str, table: str) -> Response:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if not _is_table(ds):
            raise TableNotFoundException(namespace, table)
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True
        )
        return Response(status_code=204)

    @router.post("/{prefix}/namespaces/{namespace}/tables/{table}")
    def update_table(
        prefix: str, namespace: str, table: str, request: UpdateTableRequest
    ) -> LoadTableResponse:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
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
            name=scoped,
            tags=tags,
            namespace=prefix,
            collection=ns_name,
            columns=ds.columns,
            data_source_ref=ds.data_source_ref,
        )
        updated.created_timestamp = ds.created_timestamp
        store.registry.apply_saved_dataset(
            updated, project=CATALOG_PROJECT, commit=True
        )
        return saved_dataset_to_load_table_response(updated, prefix)

    @router.post("/{prefix}/tables/rename", status_code=204)
    def rename_table(prefix: str, request: RenameTableRequest) -> Response:
        src_ns = (
            request.source.namespace[0]
            if request.source.namespace
            else DEFAULT_COLLECTION
        )
        dst_ns = (
            request.destination.namespace[0]
            if request.destination.namespace
            else DEFAULT_COLLECTION
        )

        ensure_catalog_project(store)

        src_scoped = make_scoped_name(prefix, src_ns, request.source.name)
        try:
            src_ds = store.registry.get_saved_dataset(
                src_scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(src_ns, request.source.name)
        if not _is_table(src_ds):
            raise TableNotFoundException(src_ns, request.source.name)

        dst_scoped = make_scoped_name(prefix, dst_ns, request.destination.name)
        try:
            existing = store.registry.get_saved_dataset(
                dst_scoped, project=CATALOG_PROJECT, allow_cache=False
            )
            if _is_table(existing):
                raise TableAlreadyExistsException(dst_ns, request.destination.name)
        except FeastObjectNotFoundException:
            pass

        new_ds = SavedDataset(
            name=dst_scoped,
            tags=dict(src_ds.tags),
            namespace=prefix,
            collection=dst_ns,
            columns=src_ds.columns,
            data_source_ref=src_ds.data_source_ref,
        )
        store.registry.apply_saved_dataset(new_ds, project=CATALOG_PROJECT, commit=True)
        store.registry.delete_saved_dataset(
            src_scoped, project=CATALOG_PROJECT, commit=True
        )
        return Response(status_code=204)

    return router
