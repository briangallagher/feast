import uuid
from typing import Any, Dict, List, Optional

from feast.api.catalog.models import (
    IcebergField,
    IcebergSchema,
    LoadTableResponse,
    NamespaceResponse,
    TableMetadata,
    VolumeInfo,
)
from feast.project import Project
from feast.saved_dataset import SavedDataset

CATALOG_UUID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

CATALOG_MANAGED_TAG = "_catalog_managed"


def _make_uuid(namespace: str, name: str) -> str:
    return str(uuid.uuid5(CATALOG_UUID_NAMESPACE, f"{namespace}.{name}"))


def _timestamp_ms(dt: Any) -> int:
    if dt is None:
        return 0
    return int(dt.timestamp() * 1000)


def is_catalog_managed(ds: SavedDataset) -> bool:
    return ds.tags.get(CATALOG_MANAGED_TAG) == "true"


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
    location = ds.tags.get("location", f"feast://{namespace}/tables/{ds.name}")
    schema = _columns_to_iceberg_schema(ds.columns)

    properties: Dict[str, str] = {
        k: v
        for k, v in ds.tags.items()
        if k not in (CATALOG_MANAGED_TAG, "asset_type", "location")
    }

    last_updated = _timestamp_ms(getattr(ds, "last_updated_timestamp", None))

    metadata = TableMetadata(  # type: ignore[call-arg]
        format_version=2,
        table_uuid=_make_uuid(namespace, ds.name),
        location=location,
        last_updated_ms=last_updated,
        properties=properties,
        schemas=[schema],
        current_schema_id=0,
        last_column_id=len(schema.fields),
        last_sequence_number=0,
    )

    return LoadTableResponse(  # type: ignore[call-arg]
        metadata_location=f"feast://{namespace}/tables/{ds.name}/metadata",
        metadata=metadata,
    )


# --- SavedDataset <-> Volume ---


def saved_dataset_to_volume_info(ds: SavedDataset, namespace: str) -> VolumeInfo:
    tags = {
        k: v
        for k, v in ds.tags.items()
        if k not in (CATALOG_MANAGED_TAG, "asset_type", "volume_type", "comment")
    }
    volume_type = ds.tags.get("volume_type", "EXTERNAL")
    comment: Optional[str] = ds.tags.get("comment")
    location = ds.tags.get("location", f"feast://{namespace}/volumes/{ds.name}")

    return VolumeInfo(  # type: ignore[call-arg]
        name=ds.name,
        catalog_name=namespace,
        schema_name=ds.namespace or "default",
        volume_type=volume_type,
        storage_location=location,
        comment=comment,
        created_at=_timestamp_ms(getattr(ds, "created_timestamp", None)),
        updated_at=_timestamp_ms(getattr(ds, "last_updated_timestamp", None)),
        properties=tags,
    )
