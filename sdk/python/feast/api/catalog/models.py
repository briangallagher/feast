"""
Pydantic models matching the Iceberg REST Catalog API response shapes.

Reference: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml

These models define the API contract for the Catalog API. They are intentionally
Iceberg-shaped — even for asset types that Feast or the supplemental store manage
internally — so that the API surface follows the standard regardless of backend.

Target location in Feast repo: sdk/python/feast/api/catalog/models.py
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# --- Iceberg Namespace models ---


class CreateNamespaceRequest(BaseModel):
    namespace: List[str] = Field(
        ...,
        description="Multi-level namespace identifier",
        json_schema_extra={"example": ["underwriting"]},
    )
    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Key-value properties for the namespace",
        json_schema_extra={
            "example": {"owner": "data-team", "domain": "property-casualty"}
        },
    )


class NamespaceResponse(BaseModel):
    namespace: List[str] = Field(
        ..., description="Multi-level namespace identifier"
    )
    properties: Dict[str, str] = Field(default_factory=dict)


class ListNamespacesResponse(BaseModel):
    model_config = {"populate_by_name": True}

    namespaces: List[List[str]] = Field(
        ..., description="List of namespace identifiers"
    )
    next_page_token: Optional[str] = Field(
        None,
        alias="next-page-token",
        description="Opaque token for next page, null if no more pages",
    )


# --- Iceberg Table models ---


class TableIdentifier(BaseModel):
    namespace: List[str] = Field(..., description="Namespace the table belongs to")
    name: str = Field(..., description="Table name")


class Schema(BaseModel):
    model_config = {"populate_by_name": True}

    type: str = Field(default="struct")
    schema_id: int = Field(default=0, alias="schema-id")
    fields: List[Dict[str, Any]] = Field(default_factory=list)


class TableMetadata(BaseModel):
    model_config = {"populate_by_name": True}

    format_version: int = Field(default=2, alias="format-version")
    table_uuid: str = Field(..., alias="table-uuid")
    location: str = Field(..., description="Storage location URI")
    schemas: List[Schema] = Field(default_factory=list)
    current_schema_id: int = Field(default=0, alias="current-schema-id")
    properties: Dict[str, str] = Field(default_factory=dict)


class AssetType(str, Enum):
    """Catalog asset types — extends Iceberg's table concept to non-tabular assets."""

    TABLE = "table"
    FEATURE_VIEW = "feature_view"
    DATA_SOURCE = "data_source"
    DOCUMENT_COLLECTION = "document_collection"
    VECTOR_INDEX = "vector_index"
    DATASET = "dataset"


class CreateTableRequest(BaseModel):
    model_config = {"populate_by_name": True}

    name: str = Field(..., description="Table name")
    schema_: Optional[Schema] = Field(None, alias="schema")
    location: Optional[str] = Field(
        None, description="Storage location URI (e.g., s3://, milvus://)"
    )
    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Arbitrary key-value properties",
        json_schema_extra={
            "example": {
                "asset_type": "document_collection",
                "domain": "property-casualty",
                "doc_count": "10",
            }
        },
    )


class LoadTableResult(BaseModel):
    model_config = {"populate_by_name": True}

    metadata_location: str = Field(
        ...,
        alias="metadata-location",
        description="Location of the table metadata",
    )
    metadata: TableMetadata = Field(...)
    config: Dict[str, str] = Field(default_factory=dict)


class ListTablesResponse(BaseModel):
    model_config = {"populate_by_name": True}

    identifiers: List[TableIdentifier] = Field(
        ..., description="List of table identifiers in the namespace"
    )
    next_page_token: Optional[str] = Field(
        None,
        alias="next-page-token",
        description="Opaque token for next page",
    )


# --- Error model ---


class IcebergErrorResponse(BaseModel):
    message: str = Field(..., description="Human-readable error message")
    type: str = Field(
        ...,
        description="Error type identifier",
        json_schema_extra={"example": "NoSuchNamespaceException"},
    )
    code: int = Field(..., ge=400, lt=600, description="HTTP status code")
    stack: Optional[List[str]] = Field(None, description="Stack trace (debug only)")
