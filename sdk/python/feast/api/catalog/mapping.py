import logging
import uuid
from typing import Any, Dict, List, Optional, Set

from feast.api.catalog.models import (
    IcebergField,
    IcebergSchema,
    LoadTableResponse,
    NamespaceResponse,
    TableMetadata,
    VolumeInfo,
)
from feast.errors import FeastObjectNotFoundException
from feast.project import Project
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

CATALOG_UUID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

CATALOG_PROJECT = "data-registry"
DEFAULT_COLLECTION = "default"
SCOPED_NAME_SEP = "/"


def make_scoped_name(rhai_ns: str, collection: str, display_name: str) -> str:
    return f"{rhai_ns}{SCOPED_NAME_SEP}{collection}{SCOPED_NAME_SEP}{display_name}"


def parse_display_name(scoped_name: str) -> str:
    parts = scoped_name.split(SCOPED_NAME_SEP)
    return parts[-1] if len(parts) >= 3 else scoped_name


def ensure_catalog_project(store) -> None:
    try:
        store.registry.get_project(CATALOG_PROJECT, allow_cache=True)
    except FeastObjectNotFoundException:
        project = Project(name=CATALOG_PROJECT, description="RHAI Data Registry")
        store.registry.apply_project(project, commit=True)
        logger.info("Created catalog project: %s", CATALOG_PROJECT)


def list_rhai_namespaces(store) -> Set[str]:
    datasets = store.registry.list_saved_datasets(
        project=CATALOG_PROJECT, allow_cache=True,
    )
    namespaces: Set[str] = set()
    for ds in datasets:
        if ds.namespace:
            namespaces.add(ds.namespace)
    try:
        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=True)
        if project.tags:
            for key in project.tags:
                if key.startswith("_ns_"):
                    parts = key[4:].split("_", 1)
                    if parts[0]:
                        namespaces.add(parts[0])
    except FeastObjectNotFoundException:
        pass
    return namespaces


def list_collections_for_ns(store, rhai_ns: str) -> Set[str]:
    datasets = store.registry.list_saved_datasets(
        project=CATALOG_PROJECT, allow_cache=True,
        namespace=rhai_ns,
    )
    collections: Set[str] = set()
    for ds in datasets:
        collections.add(ds.collection or DEFAULT_COLLECTION)
    try:
        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=True)
        if project.tags:
            prefix = f"_ns_{rhai_ns}_"
            for key in project.tags:
                if key.startswith(prefix):
                    coll = key[len(prefix):]
                    if coll:
                        collections.add(coll)
    except FeastObjectNotFoundException:
        pass
    return collections


def _make_uuid(namespace: str, name: str) -> str:
    return str(uuid.uuid5(CATALOG_UUID_NAMESPACE, f"{namespace}.{name}"))


def _timestamp_ms(dt: Any) -> int:
    if dt is None:
        return 0
    return int(dt.timestamp() * 1000)


def _columns_to_iceberg_schema(
    columns: List[Dict[str, Any]], schema_id: int = 0
) -> IcebergSchema:
    iceberg_fields = []
    for i, col in enumerate(columns):
        iceberg_fields.append(
            IcebergField(
                id=i + 1,
                name=str(col.get("name", "")),
                required=not col.get("nullable", True),
                type=str(col.get("type", "string")),
            )
        )
    return IcebergSchema(**{"schema-id": schema_id}, type="struct", fields=iceberg_fields)


# --- Project <-> Namespace ---


def project_to_namespace_response(
    project: Project, collection: Optional[str] = None
) -> NamespaceResponse:
    properties: Dict[str, str] = dict(project.tags) if project.tags else {}
    internal_keys = [k for k in properties if k.startswith("_ns_meta_")]
    for k in internal_keys:
        del properties[k]
    if project.description:
        properties["description"] = project.description
    if project.owner:
        properties["owner"] = project.owner
    created_ms = _timestamp_ms(getattr(project, "created_timestamp", None))
    if created_ms:
        properties["created_at"] = str(created_ms)
    updated_ms = _timestamp_ms(getattr(project, "last_updated_timestamp", None))
    if updated_ms:
        properties["updated_at"] = str(updated_ms)
    ns = [collection] if collection else [project.name]
    return NamespaceResponse(
        namespace=ns,
        properties=properties,
    )


def namespace_properties_to_project_kwargs(
    namespace: List[str], properties: Dict[str, str]
) -> dict:
    name = ".".join(namespace)
    props = dict(properties) if properties else {}
    description = props.pop("description", "")
    owner = props.pop("owner", "")
    return {
        "name": name,
        "description": description,
        "owner": owner,
        "tags": props,
    }


# --- SavedDataset <-> Table ---


def saved_dataset_to_load_table_response(
    ds: SavedDataset, namespace: str
) -> LoadTableResponse:
    display_name = parse_display_name(ds.name)
    location = ds.tags.get("location", "")
    schema = _columns_to_iceberg_schema(ds.columns)

    properties: Dict[str, str] = {
        k: v
        for k, v in ds.tags.items()
        if k not in ("asset_type", "location")
    }

    last_updated = _timestamp_ms(getattr(ds, "last_updated_timestamp", None))

    metadata = TableMetadata(  # type: ignore[call-arg]
        format_version=2,
        table_uuid=_make_uuid(namespace, display_name),
        location=location,
        last_updated_ms=last_updated,
        properties=properties,
        schemas=[schema],
        current_schema_id=0,
        last_column_id=len(schema.fields),
        last_sequence_number=0,
    )

    return LoadTableResponse(  # type: ignore[call-arg]
        metadata_location=f"feast://{namespace}/tables/{display_name}/metadata",
        metadata=metadata,
    )


# --- SavedDataset <-> Volume ---


def saved_dataset_to_volume_info(ds: SavedDataset, namespace: str) -> VolumeInfo:
    display_name = parse_display_name(ds.name)
    tags = {
        k: v
        for k, v in ds.tags.items()
        if k not in ("asset_type", "volume_type", "comment")
    }
    volume_type = ds.tags.get("volume_type", "EXTERNAL")
    comment: Optional[str] = ds.tags.get("comment")
    location = ds.tags.get("location", "")

    created = getattr(ds, "created_timestamp", None)
    updated = getattr(ds, "last_updated_timestamp", None)

    return VolumeInfo(  # type: ignore[call-arg]
        name=display_name,
        catalog_name=namespace,
        schema_name=ds.collection or ds.namespace or "default",
        volume_type=volume_type,
        storage_location=location,
        comment=comment,
        created_at=created.isoformat() if created else None,
        updated_at=updated.isoformat() if updated else None,
        properties=tags,
    )
