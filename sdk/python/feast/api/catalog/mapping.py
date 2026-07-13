"""
Feast ↔ Iceberg translation layer.

Converts between Feast registry objects (proto dicts from grpc_call) and
Iceberg REST Catalog API response shapes (Pydantic models from models.py).

The mapping is lossy by design — Feast and Iceberg have fundamentally different
data models (see registry-vs-iceberg-catalog-api.md). This layer preserves what
maps cleanly and stores Feast-specific detail in the Iceberg `properties` bag.

Target location in Feast repo: sdk/python/feast/api/catalog/mapping.py
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from .models import (
    ListNamespacesResponse,
    ListTablesResponse,
    LoadTableResult,
    NamespaceResponse,
    Schema,
    TableIdentifier,
    TableMetadata,
)


def feast_projects_to_namespaces(
    projects_response: Dict[str, Any],
) -> ListNamespacesResponse:
    """Convert grpc_call(ListProjects) response to Iceberg ListNamespacesResponse."""
    projects = projects_response.get("projects", [])
    namespaces = []
    for proj in projects:
        name = proj.get("spec", {}).get("name", proj.get("name", ""))
        if name:
            namespaces.append([name])
    return ListNamespacesResponse(namespaces=namespaces)


def feast_project_to_namespace(
    project_response: Dict[str, Any],
) -> NamespaceResponse:
    """Convert grpc_call(GetProject) response to Iceberg NamespaceResponse."""
    spec = project_response.get("spec", project_response)
    name = spec.get("name", "")
    tags = spec.get("tags", {})
    description = spec.get("description", "")

    properties = dict(tags)
    if description:
        properties["description"] = description

    return NamespaceResponse(namespace=[name], properties=properties)


def feast_data_sources_to_table_identifiers(
    data_sources_response: Dict[str, Any],
    namespace: str,
) -> List[TableIdentifier]:
    """Convert grpc_call(ListDataSources) response to Iceberg TableIdentifiers."""
    data_sources = data_sources_response.get("dataSources", [])
    identifiers = []
    for ds in data_sources:
        name = ds.get("name", "")
        if name:
            identifiers.append(
                TableIdentifier(namespace=[namespace], name=name)
            )
    return identifiers


def feast_data_source_to_load_table_result(
    data_source: Dict[str, Any],
    namespace: str,
) -> LoadTableResult:
    """Convert a single Feast DataSource (from grpc_call) to Iceberg LoadTableResult."""
    name = data_source.get("name", "")
    ds_type = data_source.get("type", "UNKNOWN")

    # Extract location from the source-type-specific config
    location = _extract_location(data_source)

    # Feast-specific fields → Iceberg properties bag
    properties = {
        "asset_type": "data_source",
        "feast_type": str(ds_type),
    }
    tags = data_source.get("tags", {})
    properties.update(tags)

    # Build schema from Feast field mappings if present
    schema_fields = _extract_schema_fields(data_source)

    table_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"feast://{namespace}/{name}"))

    metadata = TableMetadata(
        **{
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": location,
            "schemas": [
                Schema(type="struct", **{"schema-id": 0}, fields=schema_fields)
            ],
            "current-schema-id": 0,
            "properties": properties,
        }
    )

    return LoadTableResult(
        **{
            "metadata-location": f"feast://{namespace}/{name}/metadata",
            "metadata": metadata,
            "config": {},
        }
    )


def _extract_location(data_source: Dict[str, Any]) -> str:
    """Best-effort extraction of a storage location from a Feast DataSource proto dict."""
    # Feast DataSource has type-specific fields: fileOptions, bigqueryOptions, etc.
    for key in [
        "fileOptions",
        "requestDataOptions",
        "bigqueryOptions",
        "redshiftOptions",
        "snowflakeOptions",
        "sparkOptions",
        "customOptions",
        "pushOptions",
    ]:
        opts = data_source.get(key, {})
        if opts:
            # Most have a uri, path, table, or similar field
            for loc_field in ["uri", "path", "table", "fileUrl", "query"]:
                val = opts.get(loc_field)
                if val:
                    return str(val)

    return f"feast://datasource/{data_source.get('name', 'unknown')}"


def _extract_schema_fields(data_source: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract schema fields from Feast DataSource field mappings."""
    fields = []
    field_id = 1

    # Feast stores schema in fieldMapping or schema
    for fm_key in ["fieldMapping", "schema"]:
        mapping = data_source.get(fm_key, {})
        if isinstance(mapping, dict):
            for fname, ftype in mapping.items():
                fields.append(
                    {
                        "id": field_id,
                        "name": fname,
                        "type": str(ftype),
                        "required": False,
                    }
                )
                field_id += 1

    return fields
