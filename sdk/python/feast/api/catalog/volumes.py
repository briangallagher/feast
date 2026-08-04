import logging
from typing import Optional

from fastapi import APIRouter, Request, Response

from feast import FeatureStore
from feast.api.catalog.errors import (
    VolumeAlreadyExistsException,
    VolumeNotFoundException,
)
from feast.api.catalog.mapping import (
    CATALOG_PROJECT,
    DEFAULT_COLLECTION,
    ensure_catalog_project,
    make_scoped_name,
    saved_dataset_to_volume_info,
)
from feast.api.catalog.models import (
    CreateVolumeRequest,
    ListVolumesResponse,
    UpdateVolumeRequest,
    VolumeInfo,
)
from feast.api.catalog.namespaces import decode_namespace
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

VOLUME_ASSET_TYPE = "volume"


def _is_volume(ds: SavedDataset) -> bool:
    return ds.tags.get("asset_type") == VOLUME_ASSET_TYPE


def get_volume_router(store: FeatureStore) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-volumes"])

    # ------------------------------------------------------------------
    # URL mapping:
    #   prefix    = RHAI namespace  (e.g. "insurance-demo")
    #   namespace = collection      (e.g. "claims")
    #   volume    = display name    (e.g. "raw-docs")
    #
    # DB name = scoped: "insurance-demo/claims/raw-docs"
    # project = CATALOG_PROJECT = "data-registry"
    # ------------------------------------------------------------------

    @router.get("/{prefix}/namespaces/{namespace}/volumes")
    def list_volumes(prefix: str, namespace: str) -> ListVolumesResponse:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags={"asset_type": VOLUME_ASSET_TYPE},
            namespace=prefix,
        )
        filtered = [
            ds for ds in datasets if (ds.collection or DEFAULT_COLLECTION) == ns_name
        ]
        return ListVolumesResponse(
            volumes=[saved_dataset_to_volume_info(ds, prefix) for ds in filtered]
        )

    @router.post("/{prefix}/namespaces/{namespace}/volumes", status_code=200)
    def create_volume(
        prefix: str, namespace: str, request: CreateVolumeRequest, http_request: Request
    ) -> VolumeInfo:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        scoped = make_scoped_name(prefix, ns_name, request.name)

        try:
            existing = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
            if _is_volume(existing):
                raise VolumeAlreadyExistsException(namespace, request.name)
        except FeastObjectNotFoundException:
            pass

        tags = dict(request.properties) if request.properties else {}
        tags["asset_type"] = VOLUME_ASSET_TYPE
        tags["volume_type"] = request.resolved_type()
        tags["location"] = request.resolved_location()
        if request.description:
            tags["description"] = request.description
        if request.comment:
            tags["comment"] = request.comment
        if request.connection_ref:
            tags["connection-ref"] = request.connection_ref
        tags["registered_by"] = http_request.headers.get("X-User") or http_request.headers.get("kubeflow-userid", "unknown")

        ds = SavedDataset(
            name=scoped,
            tags=tags,
            namespace=prefix,
            collection=ns_name,
        )
        store.registry.apply_saved_dataset(ds, project=CATALOG_PROJECT, commit=True)
        return saved_dataset_to_volume_info(ds, prefix)

    @router.get("/{prefix}/namespaces/{namespace}/volumes/{volume}")
    def get_volume(prefix: str, namespace: str, volume: str) -> VolumeInfo:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, volume)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=True
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)
        return saved_dataset_to_volume_info(ds, prefix)

    @router.head("/{prefix}/namespaces/{namespace}/volumes/{volume}")
    def volume_exists(prefix: str, namespace: str, volume: str) -> Response:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, volume)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=True
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)
        return Response(status_code=204)

    @router.delete("/{prefix}/namespaces/{namespace}/volumes/{volume}", status_code=204)
    def delete_volume(prefix: str, namespace: str, volume: str) -> Response:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, volume)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True
        )
        return Response(status_code=204)

    @router.put("/{prefix}/namespaces/{namespace}/volumes/{volume}")
    def update_volume(
        prefix: str, namespace: str, volume: str, request: UpdateVolumeRequest
    ) -> VolumeInfo:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        scoped = make_scoped_name(prefix, ns_name, volume)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)

        tags = dict(ds.tags)
        if request.comment is not None:
            tags["comment"] = request.comment
        if request.properties:
            tags.update(request.properties)
        if request.storage_location is not None:
            tags["location"] = request.storage_location

        updated = SavedDataset(
            name=scoped,
            tags=tags,
            namespace=prefix,
            collection=ns_name,
        )
        updated.created_timestamp = ds.created_timestamp
        store.registry.apply_saved_dataset(
            updated, project=CATALOG_PROJECT, commit=True
        )
        return saved_dataset_to_volume_info(updated, prefix)

    return router
