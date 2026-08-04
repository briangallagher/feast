"""
RHOAI Data Registry API -- generic-tables extension router.

Provides format-agnostic table registration (iceberg, parquet, csv, postgresql,
etc.) under the Iceberg REST path pattern:
  /{prefix}/namespaces/{namespace}/generic-tables/{table}

Single-project model: all assets live under CATALOG_PROJECT ("data-registry").
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, Field

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
)
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLE_ASSET_TYPE = "table"
VOLUME_ASSET_TYPE = "volume"

SYSTEM_TAGS = {
    "asset_type",
    "format",
    "location",
    "connection-ref",
    "connection_ref",
    "description",
    "content_type",
    "namespace",
    "collection",
}

# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------


class CreateTableRequest(BaseModel):
    name: str
    format: str = "iceberg"
    location: Optional[str] = None
    connection_ref: Optional[str] = None
    description: Optional[str] = None
    purpose: Optional[str] = None
    license: Optional[str] = None
    maturity: Optional[str] = None
    domain: Optional[str] = None
    pii: Optional[str] = None
    schema_fields: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Column definitions: [{name, type, nullable, description, min, max, ...}]",
    )
    properties: Optional[Dict[str, str]] = None


class AssetResponse(BaseModel):
    """Generic asset representation returned by the Data Registry API."""

    name: str
    asset_type: str
    # Table fields
    format: Optional[str] = None
    location: Optional[str] = None
    # Volume fields
    content_type: Optional[str] = None
    # Schema fields
    columns: Optional[List[Dict[str, Any]]] = None
    # Common fields
    collection: Optional[str] = None
    connection_ref: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, str]] = None
    created_at: Optional[str] = None


class AssetListResponse(BaseModel):
    assets: List[AssetResponse]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _timestamp_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt)


def _saved_dataset_to_asset(ds: SavedDataset) -> AssetResponse:
    """Convert a catalog-managed SavedDataset to a generic AssetResponse."""
    asset_type = ds.tags.get("asset_type", TABLE_ASSET_TYPE)
    created = _timestamp_iso(getattr(ds, "created_timestamp", None))

    common: Dict[str, Any] = dict(
        name=parse_display_name(ds.name),
        asset_type=asset_type,
        collection=ds.collection or DEFAULT_COLLECTION,
        connection_ref=ds.tags.get("connection-ref")
        or ds.tags.get("connection_ref")
        or None,
        description=ds.tags.get("description") or ds.tags.get("comment") or None,
        created_at=created,
    )
    common["columns"] = getattr(ds, "columns", None) or None

    properties = {
        k: v
        for k, v in ds.tags.items()
        if k not in SYSTEM_TAGS and not k.startswith("_")
    }
    common["properties"] = properties if properties else None

    if asset_type == TABLE_ASSET_TYPE:
        common["format"] = ds.tags.get("format", "iceberg")
        common["location"] = ds.tags.get("location")
    elif asset_type == VOLUME_ASSET_TYPE:
        common["location"] = ds.tags.get("location")
        common["content_type"] = ds.tags.get("content_type")

    return AssetResponse(**common)


# ---------------------------------------------------------------------------
# Generic-tables extension router (Iceberg path pattern)
# ---------------------------------------------------------------------------


def get_generic_tables_router(store: FeatureStore) -> APIRouter:
    router = APIRouter(tags=["generic-tables"])

    def _ensure() -> None:
        ensure_catalog_project(store)

    @router.get("/{prefix}/namespaces/{namespace}/generic-tables")
    def list_generic_tables(prefix: str, namespace: str) -> AssetListResponse:
        _ensure()
        rhai_ns = prefix
        collection = namespace
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags={"asset_type": TABLE_ASSET_TYPE},
            namespace=rhai_ns,
            collection=collection,
        )
        return AssetListResponse(
            assets=[_saved_dataset_to_asset(ds) for ds in datasets]
        )

    @router.post("/{prefix}/namespaces/{namespace}/generic-tables", status_code=201)
    def create_generic_table(
        prefix: str, namespace: str, body: CreateTableRequest, request: Request
    ) -> AssetResponse:
        _ensure()
        rhai_ns = prefix
        collection = namespace
        scoped = make_scoped_name(rhai_ns, collection, body.name)

        try:
            existing = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False, namespace=rhai_ns,
            )
            if existing.tags.get("asset_type") == TABLE_ASSET_TYPE and (existing.collection or "") == collection:
                raise TableAlreadyExistsException(namespace, body.name)
        except FeastObjectNotFoundException:
            pass

        location = body.location or ""

        columns = []
        if body.schema_fields:
            columns = [
                {"name": f.get("name", ""), "type": f.get("type", "string"),
                 "nullable": f.get("nullable", True), "description": f.get("description", ""),
                 "min": f.get("min"), "max": f.get("max")}
                for f in body.schema_fields
            ]

        col_meta = {}
        for col in columns:
            extras = {}
            if col.get("description"): extras["description"] = col["description"]
            if col.get("min") is not None: extras["min"] = col["min"]
            if col.get("max") is not None: extras["max"] = col["max"]
            if extras: col_meta[col["name"]] = extras

        tags = {
            "asset_type": TABLE_ASSET_TYPE,
            "format": body.format,
            "location": location,
            "connection-ref": body.connection_ref or "",
            "description": body.description or "",
            "registered_by": request.headers.get("X-User") or request.headers.get("kubeflow-userid", "unknown"),
        }
        for gov_field in ("purpose", "license", "maturity", "domain", "pii"):
            value = getattr(body, gov_field, None)
            if value:
                tags[gov_field] = value
        if col_meta:
            tags["_col_meta"] = json.dumps(col_meta)
        if body.properties:
            for k, v in body.properties.items():
                if k not in tags:
                    tags[k] = v

        ds = SavedDataset(
            name=scoped, namespace=rhai_ns, collection=collection,
            tags=tags, columns=columns,
        )
        store.registry.apply_saved_dataset(ds, project=CATALOG_PROJECT, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/{prefix}/namespaces/{namespace}/generic-tables/{table}")
    def get_generic_table(prefix: str, namespace: str, table: str) -> AssetResponse:
        _ensure()
        rhai_ns = prefix
        collection = namespace
        scoped = make_scoped_name(rhai_ns, collection, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False, namespace=rhai_ns,
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise TableNotFoundException(namespace, table)
        if (ds.collection or DEFAULT_COLLECTION) != collection:
            raise TableNotFoundException(namespace, table)
        return _saved_dataset_to_asset(ds)

    @router.delete("/{prefix}/namespaces/{namespace}/generic-tables/{table}", status_code=204)
    def delete_generic_table(prefix: str, namespace: str, table: str) -> Response:
        _ensure()
        rhai_ns = prefix
        collection = namespace
        scoped = make_scoped_name(rhai_ns, collection, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False, namespace=rhai_ns,
            )
        except FeastObjectNotFoundException:
            raise TableNotFoundException(namespace, table)
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise TableNotFoundException(namespace, table)
        if (ds.collection or DEFAULT_COLLECTION) != collection:
            raise TableNotFoundException(namespace, table)
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True, namespace=rhai_ns,
        )
        return Response(status_code=204)

    return router
