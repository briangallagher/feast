"""
RHOAI Data Catalog API — proprietary CRUD endpoints for all asset types.

Separate from the Iceberg REST handlers. Provides a user-facing API with
our own simple JSON response format for tables, volumes, databases, and
collections.

Router factory: ``get_catalog_api_router(store)`` returns an ``APIRouter``
with prefix="" (the caller sets the mount prefix).
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel, Field

from feast import FeatureStore
from feast.api.catalog.mapping import CATALOG_MANAGED_TAG
from feast.api.catalog.namespaces import DEFAULT_SCHEMA
from feast.errors import FeastObjectNotFoundException
from feast.saved_dataset import SavedDataset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLE_ASSET_TYPE = "table"
VOLUME_ASSET_TYPE = "volume"
DATABASE_ASSET_TYPE = "database"

SUPPORTED_DB_TYPES = [
    "postgresql",
    "mysql",
    "snowflake",
    "mssql",
    "oracle",
    "mongodb",
    "redis",
    "cockroachdb",
    "mariadb",
]

SYSTEM_TAGS = {
    "_catalog_managed",
    "asset_type",
    "format",
    "location",
    "connection-ref",
    "connection_ref",
    "description",
    "db_type",
    "host",
    "database",
    "schemas",
    "content_type",
    "namespace",
    "collection",
}

# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------


class CreateCollectionRequest(BaseModel):
    name: str


class CreateTableRequest(BaseModel):
    name: str
    format: str = "iceberg"
    location: Optional[str] = None
    connection_ref: Optional[str] = None
    description: Optional[str] = None
    schema_fields: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Column definitions: [{name, type, nullable, description, min, max, ...}]",
    )
    properties: Optional[Dict[str, str]] = None


class CreateVolumeRequest(BaseModel):
    name: str
    location: str
    connection_ref: Optional[str] = None
    content_type: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, str]] = None


class CreateDatabaseRequest(BaseModel):
    name: str
    db_type: str
    host: str
    database: str
    schemas: Optional[List[str]] = None
    connection_ref: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, str]] = None


class AssetResponse(BaseModel):
    """Generic asset representation returned by the Catalog API."""

    name: str
    asset_type: str
    # Table fields
    format: Optional[str] = None
    location: Optional[str] = None
    # Database fields
    db_type: Optional[str] = None
    host: Optional[str] = None
    database: Optional[str] = None
    schemas: Optional[List[str]] = None
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


class CollectionResponse(BaseModel):
    name: str
    properties: Dict[str, str] = {}


class CollectionListResponse(BaseModel):
    collections: List[CollectionResponse]


class ProjectListResponse(BaseModel):
    projects: List[str]


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

    common = dict(
        name=ds.name,
        asset_type=asset_type,
        collection=ds.namespace or DEFAULT_SCHEMA,
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
    elif asset_type == DATABASE_ASSET_TYPE:
        common["db_type"] = ds.tags.get("db_type")
        common["host"] = ds.tags.get("host")
        common["database"] = ds.tags.get("database")
        schemas_raw = ds.tags.get("schemas", "")
        common["schemas"] = (
            [s for s in schemas_raw.split(",") if s] if schemas_raw else []
        )

    return AssetResponse(**common)


def _ensure_project_exists(store: FeatureStore, project: str) -> None:
    try:
        store.registry.get_project(project, allow_cache=True)
    except FeastObjectNotFoundException:
        raise _not_found(f"Project not found: {project}")


def _not_found(message: str) -> Exception:
    """Return an HTTPException-style error as a simple exception.

    The caller should ``raise`` the returned value; the global Iceberg
    exception handler will render it, or FastAPI's default handler will
    catch HTTPException.
    """
    from fastapi import HTTPException

    return HTTPException(status_code=404, detail=message)


def _conflict(message: str) -> Exception:
    from fastapi import HTTPException

    return HTTPException(status_code=409, detail=message)


def _bad_request(message: str) -> Exception:
    from fastapi import HTTPException

    return HTTPException(status_code=400, detail=message)


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------


def get_catalog_api_router(store: FeatureStore) -> APIRouter:
    """Return an APIRouter with the RHOAI Catalog API routes.

    The router is created with ``prefix=""``; the caller sets the mount
    prefix (e.g. ``/catalog/v1``).
    """
    router = APIRouter(tags=["catalog-api"])

    # -----------------------------------------------------------------------
    # Projects
    # -----------------------------------------------------------------------

    @router.get("/projects")
    async def list_projects(request: Request) -> ProjectListResponse:
        """List all Feast projects, with optional SSAR filtering."""
        all_projects = store.registry.list_projects(allow_cache=False)
        project_names = [p.name for p in all_projects]

        ssar_enabled = (
            os.environ.get("DATACATALOG_SSAR_ENABLED", "true").lower() != "false"
        )
        if not ssar_enabled:
            return ProjectListResponse(projects=project_names)

        auth_header = request.headers.get("authorization", "")
        if not auth_header.startswith("Bearer "):
            return ProjectListResponse(projects=project_names)

        token = auth_header[7:]
        try:
            from feast.api.catalog.ssar import _check_ssar, _ensure_k8s_config

            _ensure_k8s_config()
            accessible = []
            for name in project_names:
                allowed = await _check_ssar(token, name, "namespaces", "list")
                if allowed:
                    accessible.append(name)
            return ProjectListResponse(projects=accessible)
        except Exception:
            logger.warning("SSAR filtering failed, returning all projects")
            return ProjectListResponse(projects=project_names)

    # -----------------------------------------------------------------------
    # Collections
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections")
    def list_collections(project: str) -> CollectionListResponse:
        _ensure_project_exists(store, project)
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true"},
        )
        seen: set[str] = set()
        for ds in datasets:
            seen.add(ds.namespace or DEFAULT_SCHEMA)

        # Include explicitly-created collections (may have no assets yet)
        try:
            proj = store.registry.get_project(project, allow_cache=True)
            if proj.tags:
                for tag_key in proj.tags:
                    if tag_key.startswith("_ns_meta_"):
                        ns_name = tag_key[len("_ns_meta_") :]
                        if ns_name:
                            seen.add(ns_name)
        except FeastObjectNotFoundException:
            pass

        if not seen:
            seen.add(DEFAULT_SCHEMA)

        collections = [CollectionResponse(name=n) for n in sorted(seen)]
        return CollectionListResponse(collections=collections)

    @router.post("/projects/{project}/collections", status_code=201)
    def create_collection(
        project: str, request: CreateCollectionRequest
    ) -> CollectionResponse:
        _ensure_project_exists(store, project)
        name = request.name

        # Check if already exists
        existing = set()
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true"},
        )
        for ds in datasets:
            existing.add(ds.namespace or DEFAULT_SCHEMA)

        try:
            proj = store.registry.get_project(project, allow_cache=True)
            if proj.tags:
                for tag_key in proj.tags:
                    if tag_key.startswith("_ns_meta_"):
                        ns_name = tag_key[len("_ns_meta_") :]
                        if ns_name:
                            existing.add(ns_name)
        except FeastObjectNotFoundException:
            pass

        if name in existing:
            raise _conflict(f"Collection already exists: {name}")

        # Store collection metadata in project tags
        import json

        proj = store.registry.get_project(project, allow_cache=False)
        tags = dict(proj.tags) if proj.tags else {}
        ns_meta_key = f"_ns_meta_{name}"
        tags[ns_meta_key] = json.dumps({})
        proj.tags = tags
        store.registry.apply_project(proj, commit=True)

        return CollectionResponse(name=name)

    @router.delete("/projects/{project}/collections/{collection}", status_code=204)
    def delete_collection(project: str, collection: str) -> Response:
        _ensure_project_exists(store, project)

        # Check the collection is empty
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true"},
        )
        ns_datasets = [
            ds for ds in datasets if (ds.namespace or DEFAULT_SCHEMA) == collection
        ]
        if ns_datasets:
            raise _conflict(
                f"Collection is not empty: {collection} "
                f"({len(ns_datasets)} asset(s) remain)"
            )

        # Remove _ns_meta_ tag if present
        try:
            proj = store.registry.get_project(project, allow_cache=False)
            ns_meta_key = f"_ns_meta_{collection}"
            tags = dict(proj.tags) if proj.tags else {}
            if ns_meta_key in tags:
                del tags[ns_meta_key]
                proj.tags = tags
                store.registry.apply_project(proj, commit=True)
        except FeastObjectNotFoundException:
            pass

        return Response(status_code=204)

    # -----------------------------------------------------------------------
    # Tables
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections/{collection}/tables")
    def list_tables(project: str, collection: str) -> AssetListResponse:
        _ensure_project_exists(store, project)
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": TABLE_ASSET_TYPE},
        )
        filtered = [
            ds for ds in datasets if (ds.namespace or DEFAULT_SCHEMA) == collection
        ]
        return AssetListResponse(
            assets=[_saved_dataset_to_asset(ds) for ds in filtered]
        )

    @router.post("/projects/{project}/collections/{collection}/tables", status_code=201)
    def create_table(
        project: str, collection: str, request: CreateTableRequest
    ) -> AssetResponse:
        _ensure_project_exists(store, project)

        # Check for duplicate
        try:
            existing = store.registry.get_saved_dataset(
                request.name,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
            if existing.tags.get("asset_type") == TABLE_ASSET_TYPE:
                raise _conflict(f"Table already exists: {collection}.{request.name}")
        except FeastObjectNotFoundException:
            pass

        location = (
            request.location or f"feast://{project}/{collection}/tables/{request.name}"
        )

        columns = []
        if request.schema_fields:
            columns = [
                {
                    "name": f.get("name", ""),
                    "type": f.get("type", "string"),
                    "nullable": f.get("nullable", True),
                    "description": f.get("description", ""),
                    "min": f.get("min"),
                    "max": f.get("max"),
                }
                for f in request.schema_fields
            ]

        tags = {
            CATALOG_MANAGED_TAG: "true",
            "asset_type": TABLE_ASSET_TYPE,
            "format": request.format,
            "location": location,
            "connection-ref": request.connection_ref or "",
            "description": request.description or "",
        }
        if request.properties:
            for k, v in request.properties.items():
                if k not in tags:  # don't overwrite system tags
                    tags[k] = v

        ds = SavedDataset(
            name=request.name,
            tags=tags,
            namespace=collection,
            columns=columns,
        )
        store.registry.apply_saved_dataset(ds, project=project, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/projects/{project}/collections/{collection}/tables/{table}")
    def get_table(project: str, collection: str, table: str) -> AssetResponse:
        _ensure_project_exists(store, project)
        try:
            ds = store.registry.get_saved_dataset(
                table,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Table not found: {collection}.{table}")
        if ds.tags.get(CATALOG_MANAGED_TAG) != "true":
            raise _not_found(f"Table not found: {collection}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {collection}.{table}")
        return _saved_dataset_to_asset(ds)

    @router.delete(
        "/projects/{project}/collections/{collection}/tables/{table}",
        status_code=204,
    )
    def delete_table(project: str, collection: str, table: str) -> Response:
        _ensure_project_exists(store, project)
        try:
            ds = store.registry.get_saved_dataset(
                table,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Table not found: {collection}.{table}")
        if ds.tags.get(CATALOG_MANAGED_TAG) != "true":
            raise _not_found(f"Table not found: {collection}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {collection}.{table}")
        store.registry.delete_saved_dataset(
            table, project=project, commit=True, namespace=collection
        )
        return Response(status_code=204)

    # -----------------------------------------------------------------------
    # Volumes
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections/{collection}/volumes")
    def list_volumes(project: str, collection: str) -> AssetListResponse:
        _ensure_project_exists(store, project)
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": VOLUME_ASSET_TYPE},
        )
        filtered = [
            ds for ds in datasets if (ds.namespace or DEFAULT_SCHEMA) == collection
        ]
        return AssetListResponse(
            assets=[_saved_dataset_to_asset(ds) for ds in filtered]
        )

    @router.post(
        "/projects/{project}/collections/{collection}/volumes", status_code=201
    )
    def create_volume(
        project: str, collection: str, request: CreateVolumeRequest
    ) -> AssetResponse:
        _ensure_project_exists(store, project)

        try:
            existing = store.registry.get_saved_dataset(
                request.name,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
            if existing.tags.get("asset_type") == VOLUME_ASSET_TYPE:
                raise _conflict(f"Volume already exists: {collection}.{request.name}")
        except FeastObjectNotFoundException:
            pass

        tags = {
            CATALOG_MANAGED_TAG: "true",
            "asset_type": VOLUME_ASSET_TYPE,
            "location": request.location,
            "content_type": request.content_type or "",
            "connection-ref": request.connection_ref or "",
            "description": request.description or "",
        }
        if request.properties:
            for k, v in request.properties.items():
                if k not in tags:  # don't overwrite system tags
                    tags[k] = v

        ds = SavedDataset(
            name=request.name,
            tags=tags,
            namespace=collection,
        )
        store.registry.apply_saved_dataset(ds, project=project, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/projects/{project}/collections/{collection}/volumes/{volume}")
    def get_volume(project: str, collection: str, volume: str) -> AssetResponse:
        _ensure_project_exists(store, project)
        try:
            ds = store.registry.get_saved_dataset(
                volume,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        if ds.tags.get(CATALOG_MANAGED_TAG) != "true":
            raise _not_found(f"Volume not found: {collection}.{volume}")
        if ds.tags.get("asset_type") != VOLUME_ASSET_TYPE:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        return _saved_dataset_to_asset(ds)

    @router.delete(
        "/projects/{project}/collections/{collection}/volumes/{volume}",
        status_code=204,
    )
    def delete_volume(project: str, collection: str, volume: str) -> Response:
        _ensure_project_exists(store, project)
        try:
            ds = store.registry.get_saved_dataset(
                volume,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        if ds.tags.get(CATALOG_MANAGED_TAG) != "true":
            raise _not_found(f"Volume not found: {collection}.{volume}")
        if ds.tags.get("asset_type") != VOLUME_ASSET_TYPE:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        store.registry.delete_saved_dataset(
            volume, project=project, commit=True, namespace=collection
        )
        return Response(status_code=204)

    # -----------------------------------------------------------------------
    # Databases
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections/{collection}/databases")
    def list_databases(project: str, collection: str) -> AssetListResponse:
        _ensure_project_exists(store, project)
        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": DATABASE_ASSET_TYPE},
        )
        filtered = [
            ds for ds in datasets if (ds.namespace or DEFAULT_SCHEMA) == collection
        ]
        return AssetListResponse(
            assets=[_saved_dataset_to_asset(ds) for ds in filtered]
        )

    @router.post(
        "/projects/{project}/collections/{collection}/databases",
        status_code=201,
    )
    def create_database(
        project: str, collection: str, request: CreateDatabaseRequest
    ) -> AssetResponse:
        _ensure_project_exists(store, project)

        if request.db_type not in SUPPORTED_DB_TYPES:
            raise _bad_request(
                f"Unsupported database type: {request.db_type}. "
                f"Supported: {', '.join(SUPPORTED_DB_TYPES)}"
            )

        try:
            existing = store.registry.get_saved_dataset(
                request.name,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
            if existing.tags.get("asset_type") == DATABASE_ASSET_TYPE:
                raise _conflict(f"Database already exists: {collection}.{request.name}")
        except FeastObjectNotFoundException:
            pass

        tags = {
            CATALOG_MANAGED_TAG: "true",
            "asset_type": DATABASE_ASSET_TYPE,
            "db_type": request.db_type,
            "host": request.host,
            "database": request.database,
            "schemas": ",".join(request.schemas) if request.schemas else "",
            "connection-ref": request.connection_ref or "",
            "description": request.description or "",
        }
        if request.properties:
            for k, v in request.properties.items():
                if k not in tags:  # don't overwrite system tags
                    tags[k] = v

        ds = SavedDataset(
            name=request.name,
            tags=tags,
            namespace=collection,
        )
        store.registry.apply_saved_dataset(ds, project=project, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/projects/{project}/collections/{collection}/databases/{database}")
    def get_database(project: str, collection: str, database: str) -> AssetResponse:
        _ensure_project_exists(store, project)
        try:
            ds = store.registry.get_saved_dataset(
                database,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Database not found: {collection}.{database}")
        if ds.tags.get(CATALOG_MANAGED_TAG) != "true":
            raise _not_found(f"Database not found: {collection}.{database}")
        if ds.tags.get("asset_type") != DATABASE_ASSET_TYPE:
            raise _not_found(f"Database not found: {collection}.{database}")
        return _saved_dataset_to_asset(ds)

    @router.delete(
        "/projects/{project}/collections/{collection}/databases/{database}",
        status_code=204,
    )
    def delete_database(project: str, collection: str, database: str) -> Response:
        _ensure_project_exists(store, project)
        try:
            ds = store.registry.get_saved_dataset(
                database,
                project=project,
                allow_cache=False,
                namespace=collection,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Database not found: {collection}.{database}")
        if ds.tags.get(CATALOG_MANAGED_TAG) != "true":
            raise _not_found(f"Database not found: {collection}.{database}")
        if ds.tags.get("asset_type") != DATABASE_ASSET_TYPE:
            raise _not_found(f"Database not found: {collection}.{database}")
        store.registry.delete_saved_dataset(
            database, project=project, commit=True, namespace=collection
        )
        return Response(status_code=204)

    # -----------------------------------------------------------------------
    # Search
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/search")
    def search_project(
        project: str,
        q: str = Query(
            default="",
            description="Text query (matches name, description, tags). Empty returns all.",
        ),
        asset_type: Optional[str] = Query(
            default=None,
            description="Filter by asset type: table, volume, database.",
        ),
        collection: Optional[str] = Query(
            default=None,
            description="Restrict search to a single collection.",
        ),
    ) -> AssetListResponse:
        _ensure_project_exists(store, project)

        tag_filter: Dict[str, str] = {CATALOG_MANAGED_TAG: "true"}
        if asset_type:
            tag_filter["asset_type"] = asset_type

        datasets = store.registry.list_saved_datasets(
            project=project,
            allow_cache=False,
            tags=tag_filter,
        )

        results: List[SavedDataset] = []
        query_lower = q.lower()

        for ds in datasets:
            if collection and (ds.namespace or DEFAULT_SCHEMA) != collection:
                continue
            if query_lower:
                name_match = query_lower in ds.name.lower()
                desc = ds.tags.get("description") or ds.tags.get("comment") or ""
                desc_match = query_lower in desc.lower()
                tag_match = any(
                    query_lower in v.lower()
                    for k, v in ds.tags.items()
                    if k != CATALOG_MANAGED_TAG
                )
                if not (name_match or desc_match or tag_match):
                    continue
            results.append(ds)

        return AssetListResponse(assets=[_saved_dataset_to_asset(ds) for ds in results])

    @router.get("/search")
    async def search_cross_project(
        request: Request,
        q: str = Query(
            default="",
            description="Text query. Empty returns all assets across projects.",
        ),
        asset_type: Optional[str] = Query(
            default=None,
            description="Filter by asset type: table, volume, database.",
        ),
        project: Optional[str] = Query(
            default=None,
            description="Restrict to a single project.",
        ),
    ) -> AssetListResponse:
        all_projects = store.registry.list_projects(allow_cache=False)
        if project:
            project_names = [p.name for p in all_projects if p.name == project]
        else:
            project_names = [p.name for p in all_projects]

        all_assets: List[AssetResponse] = []
        query_lower = q.lower()

        for proj_name in project_names:
            tag_filter: Dict[str, str] = {CATALOG_MANAGED_TAG: "true"}
            if asset_type:
                tag_filter["asset_type"] = asset_type

            try:
                datasets = store.registry.list_saved_datasets(
                    project=proj_name,
                    allow_cache=False,
                    tags=tag_filter,
                )
            except Exception:
                logger.warning("Failed to list datasets for project %s", proj_name)
                continue

            for ds in datasets:
                if query_lower:
                    name_match = query_lower in ds.name.lower()
                    desc = ds.tags.get("description") or ds.tags.get("comment") or ""
                    desc_match = query_lower in desc.lower()
                    tag_match = any(
                        query_lower in v.lower()
                        for k, v in ds.tags.items()
                        if k != CATALOG_MANAGED_TAG
                    )
                    if not (name_match or desc_match or tag_match):
                        continue
                all_assets.append(_saved_dataset_to_asset(ds))

        return AssetListResponse(assets=all_assets)

    return router
