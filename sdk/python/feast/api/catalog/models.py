from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# --- Namespace models ---


class CreateNamespaceRequest(BaseModel):
    namespace: List[str]
    properties: Optional[Dict[str, str]] = None


class NamespaceResponse(BaseModel):
    namespace: List[str]
    properties: Dict[str, str]


class ListNamespacesResponse(BaseModel):
    namespaces: List[List[str]]


class UpdateNamespacePropertiesRequest(BaseModel):
    removals: Optional[List[str]] = None
    updates: Optional[Dict[str, str]] = None


class UpdateNamespacePropertiesResponse(BaseModel):
    removed: List[str]
    updated: List[str]
    missing: List[str]


# --- Table models ---


class TableIdentifier(BaseModel):
    namespace: List[str]
    name: str


class IcebergField(BaseModel):
    id: int
    name: str
    required: bool
    type: str


class IcebergSchema(BaseModel):
    type: str = "struct"
    schema_id: int = Field(default=0, alias="schema-id")
    fields: List[IcebergField] = []

    model_config = {"populate_by_name": True}


class PartitionSpec(BaseModel):
    spec_id: int = Field(default=0, alias="spec-id")
    fields: List[Any] = []

    model_config = {"populate_by_name": True}


class SortOrder(BaseModel):
    order_id: int = Field(default=0, alias="order-id")
    fields: List[Any] = []

    model_config = {"populate_by_name": True}


class TableMetadata(BaseModel):
    format_version: int = Field(default=2, alias="format-version")
    table_uuid: str = Field(alias="table-uuid")
    location: str
    last_updated_ms: int = Field(alias="last-updated-ms")
    properties: Dict[str, str]
    schemas: List[IcebergSchema]
    current_schema_id: int = Field(default=0, alias="current-schema-id")
    partition_specs: List[PartitionSpec] = Field(
        default_factory=lambda: [PartitionSpec()], alias="partition-specs"
    )
    default_spec_id: int = Field(default=0, alias="default-spec-id")
    sort_orders: List[SortOrder] = Field(
        default_factory=lambda: [SortOrder()], alias="sort-orders"
    )
    default_sort_order_id: int = Field(default=0, alias="default-sort-order-id")
    last_column_id: int = Field(default=0, alias="last-column-id")
    last_sequence_number: int = Field(default=0, alias="last-sequence-number")
    last_partition_id: int = Field(default=999, alias="last-partition-id")
    snapshots: List[Any] = []
    current_snapshot_id: int = Field(default=-1, alias="current-snapshot-id")

    model_config = {"populate_by_name": True}


class LoadTableResponse(BaseModel):
    metadata_location: str = Field(alias="metadata-location")
    metadata: TableMetadata
    config: Dict[str, str] = {}

    model_config = {"populate_by_name": True}


class CreateTableRequest(BaseModel):
    name: str
    schema_: IcebergSchema = Field(alias="schema")
    location: Optional[str] = None
    properties: Optional[Dict[str, str]] = None
    partition_spec: Optional[PartitionSpec] = Field(
        default=None, alias="partition-spec"
    )
    sort_order: Optional[SortOrder] = Field(default=None, alias="sort-order")

    model_config = {"populate_by_name": True}


class ListTablesResponse(BaseModel):
    identifiers: List[TableIdentifier]


class RenameTableRequest(BaseModel):
    source: TableIdentifier
    destination: TableIdentifier


class TableUpdate(BaseModel):
    action: str
    updates: Optional[Dict[str, str]] = None
    removals: Optional[List[str]] = None
    schema_: Optional[IcebergSchema] = Field(default=None, alias="schema")

    model_config = {"populate_by_name": True}


class TableRequirement(BaseModel):
    type: str
    ref: Optional[str] = None
    uuid: Optional[str] = None
    last_assigned_field_id: Optional[int] = Field(
        default=None, alias="last-assigned-field-id"
    )

    model_config = {"populate_by_name": True}


class UpdateTableRequest(BaseModel):
    requirements: Optional[List[TableRequirement]] = None
    updates: List[TableUpdate]

    model_config = {"populate_by_name": True}


# --- Config model ---


class CatalogConfig(BaseModel):
    defaults: Dict[str, str] = {}
    overrides: Dict[str, str] = {}
    endpoints: Optional[List[str]] = None


# --- Volume models ---


class VolumeInfo(BaseModel):
    name: str
    catalog_name: str = Field(alias="catalog-name")
    schema_name: str = Field(alias="schema-name")
    volume_type: str = Field(alias="volume-type")
    storage_location: str = Field(alias="storage-location")
    comment: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[str] = Field(default=None, alias="created-at")
    updated_at: Optional[str] = Field(default=None, alias="updated-at")
    properties: Dict[str, str] = {}
    config: Dict[str, str] = {}

    model_config = {"populate_by_name": True}


class CreateVolumeRequest(BaseModel):
    name: str
    location: Optional[str] = None
    storage_location: Optional[str] = Field(default=None, alias="storage-location")
    volume_type: Optional[str] = Field(default=None, alias="volume-type")
    content_type: Optional[str] = None
    connection_ref: Optional[str] = None
    comment: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, str]] = None

    model_config = {"populate_by_name": True}

    def resolved_location(self) -> str:
        return self.location or self.storage_location or ""

    def resolved_type(self) -> str:
        return self.volume_type or self.content_type or "EXTERNAL"


class UpdateVolumeRequest(BaseModel):
    comment: Optional[str] = None
    owner: Optional[str] = None
    storage_location: Optional[str] = Field(default=None, alias="storage_location")
    properties: Optional[Dict[str, str]] = None


class ListVolumesResponse(BaseModel):
    volumes: List[VolumeInfo]


# --- Search models ---


class SearchResult(BaseModel):
    type: str
    namespace: List[str]
    name: str
    description: Optional[str] = None
    properties: Dict[str, str] = {}
    score: int = 0
    project: Optional[str] = None


class SearchResponse(BaseModel):
    query: str
    results: List[SearchResult]
    total: int = 0
    page: int = 1
    limit: int = 50
    searched_projects: Optional[List[str]] = None
