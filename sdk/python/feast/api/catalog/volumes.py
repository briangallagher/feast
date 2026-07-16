import logging
from typing import Optional

from fastapi import APIRouter, Response

from feast import FeatureStore
from feast.api.catalog.connections import resolve_credentials
from feast.api.catalog.credentials import STSCredentialVender
from feast.api.catalog.errors import (
    NamespaceNotFoundException,
    VolumeAlreadyExistsException,
    VolumeNotFoundException,
)
from feast.api.catalog.mapping import (
    CATALOG_MANAGED_TAG,
    saved_dataset_to_volume_info,
)
from feast.api.catalog.models import (
    CreateVolumeRequest,
    ListVolumesResponse,
    UpdateVolumeRequest,
    VolumeInfo,
)
from feast.api.catalog.namespaces import DEFAULT_SCHEMA, decode_namespace
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

VOLUME_ASSET_TYPE = "volume"


def _is_volume(ds: SavedDataset) -> bool:
    return ds.tags.get("asset_type") == VOLUME_ASSET_TYPE


def get_volume_router(
    store: FeatureStore,
    credential_vender: Optional[STSCredentialVender] = None,
) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-volumes"])

    def _ensure_project_exists(prefix: str) -> None:
        try:
            store.registry.get_project(prefix, allow_cache=True)
        except FeastObjectNotFoundException:
            raise NamespaceNotFoundException(prefix)

    @router.get("/{prefix}/namespaces/{namespace}/volumes")
    def list_volumes(prefix: str, namespace: str) -> ListVolumesResponse:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        datasets = store.registry.list_saved_datasets(
            project=prefix,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": VOLUME_ASSET_TYPE},
            namespace=ns_name,
        )
        return ListVolumesResponse(
            volumes=[saved_dataset_to_volume_info(ds, prefix) for ds in datasets]
        )

    @router.post("/{prefix}/namespaces/{namespace}/volumes", status_code=200)
    def create_volume(
        prefix: str, namespace: str, request: CreateVolumeRequest
    ) -> VolumeInfo:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        try:
            existing = store.registry.get_saved_dataset(
                request.name, project=prefix, allow_cache=False, namespace=ns_name
            )
            if _is_volume(existing):
                raise VolumeAlreadyExistsException(namespace, request.name)
        except FeastObjectNotFoundException:
            pass

        tags = dict(request.properties) if request.properties else {}
        tags[CATALOG_MANAGED_TAG] = "true"
        tags["asset_type"] = VOLUME_ASSET_TYPE
        tags["volume_type"] = request.volume_type
        tags["location"] = request.storage_location
        if request.comment:
            tags["comment"] = request.comment

        ds = SavedDataset(
            name=request.name,
            tags=tags,
            namespace=ns_name,
            data_source_ref=request.data_source_ref or "",
        )
        store.registry.apply_saved_dataset(ds, project=prefix, commit=True)
        return saved_dataset_to_volume_info(ds, prefix)

    @router.get("/{prefix}/namespaces/{namespace}/volumes/{volume}")
    def get_volume(prefix: str, namespace: str, volume: str) -> VolumeInfo:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                volume, project=prefix, allow_cache=True, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)
        result = saved_dataset_to_volume_info(ds, prefix)
        connection_creds = resolve_credentials(ds, prefix)
        if connection_creds:
            result.config.update(connection_creds)
        elif credential_vender:
            location = ds.tags.get("location", "")
            if location.startswith("s3://"):
                try:
                    vended = credential_vender.vend(location)
                    result.config.update(vended)
                except Exception as e:
                    logger.warning("STS vending failed for %s: %s", volume, e)
        return result

    @router.head("/{prefix}/namespaces/{namespace}/volumes/{volume}")
    def volume_exists(prefix: str, namespace: str, volume: str) -> Response:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                volume, project=prefix, allow_cache=True, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)
        return Response(status_code=204)

    @router.delete(
        "/{prefix}/namespaces/{namespace}/volumes/{volume}", status_code=204
    )
    def delete_volume(prefix: str, namespace: str, volume: str) -> Response:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                volume, project=prefix, allow_cache=False, namespace=ns_name
            )
        except FeastObjectNotFoundException:
            raise VolumeNotFoundException(namespace, volume)
        if not _is_volume(ds):
            raise VolumeNotFoundException(namespace, volume)
        store.registry.delete_saved_dataset(
            volume, project=prefix, commit=True, namespace=ns_name
        )
        return Response(status_code=204)

    @router.put("/{prefix}/namespaces/{namespace}/volumes/{volume}")
    def update_volume(
        prefix: str, namespace: str, volume: str, request: UpdateVolumeRequest
    ) -> VolumeInfo:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        try:
            ds = store.registry.get_saved_dataset(
                volume, project=prefix, allow_cache=False, namespace=ns_name
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

        data_source_ref = ds.data_source_ref
        if request.data_source_ref is not None:
            data_source_ref = request.data_source_ref

        updated = SavedDataset(
            name=ds.name,
            tags=tags,
            namespace=ns_name,
            data_source_ref=data_source_ref,
        )
        updated.created_timestamp = ds.created_timestamp
        store.registry.apply_saved_dataset(updated, project=prefix, commit=True)
        return saved_dataset_to_volume_info(updated, prefix)

    return router
