"""
RHOAI Data Catalog API -- proprietary CRUD endpoints for all asset types.

Single-project model: all assets live under CATALOG_PROJECT ("data-registry").
RHAI namespaces (K8s namespaces) are stored in SavedDataset.namespace;
collections in SavedDataset.collection.  DB names are scoped:
  "insurance-demo/claims/auto-claims"

Router factory: ``get_catalog_api_router(store)`` returns an ``APIRouter``
with prefix="" (the caller sets the mount prefix).
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel, Field

from feast import FeatureStore
from feast.api.catalog.mapping import (
    CATALOG_PROJECT,
    DEFAULT_COLLECTION,
    ensure_catalog_project,
    list_collections_for_ns,
    list_rhai_namespaces,
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
    "asset_type",
    "format",
    "location",
    "connection-ref",
    "connection_ref",
    "description",
    "owner",
    "registered_by",
    "updated_by",
    "db_type",
    "host",
    "database",
    "schemas",
    "content_type",
    "namespace",
    "collection",
    "_tags",
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
    purpose: Optional[str] = None
    license: Optional[str] = None
    maturity: Optional[str] = None
    domain: Optional[str] = None
    pii: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None
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
    tags: Optional[List[str]] = None
    properties: Optional[Dict[str, str]] = None


class CreateDatabaseRequest(BaseModel):
    name: str
    db_type: str
    host: str
    database: str
    schemas: Optional[List[str]] = None
    connection_ref: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    properties: Optional[Dict[str, str]] = None


class UpdateGenericTableRequest(BaseModel):
    description: Optional[str] = None
    format: Optional[str] = None
    location: Optional[str] = None
    connection_ref: Optional[str] = None
    purpose: Optional[str] = None
    license: Optional[str] = None
    maturity: Optional[str] = None
    domain: Optional[str] = None
    pii: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None
    schema_fields: Optional[List[Dict[str, Any]]] = None
    properties: Optional[Dict[str, str]] = None


class AssetResponse(BaseModel):
    """Generic asset representation returned by the Catalog API."""

    name: str
    asset_type: str
    uuid: Optional[str] = None
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
    owner: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    properties: Optional[Dict[str, str]] = None
    registered_by: Optional[str] = None
    updated_by: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


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
    updated = _timestamp_iso(getattr(ds, "last_updated_timestamp", None))

    raw_tags = ds.tags.get("_tags", "")
    asset_tags = [t for t in raw_tags.split(",") if t] if raw_tags else None

    common: Dict[str, Any] = dict(
        name=parse_display_name(ds.name),
        asset_type=asset_type,
        uuid=ds.tags.get("_asset_uuid") or None,
        collection=ds.collection or DEFAULT_COLLECTION,
        connection_ref=ds.tags.get("connection-ref")
        or ds.tags.get("connection_ref")
        or None,
        description=ds.tags.get("description") or ds.tags.get("comment") or None,
        owner=ds.tags.get("owner") or None,
        tags=asset_tags,
        registered_by=ds.tags.get("registered_by") or None,
        updated_by=ds.tags.get("updated_by") or None,
        created_at=created,
        updated_at=updated,
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


def _not_found(message: str) -> Exception:
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

    URL convention (single-project model):
      /projects/{project}  -- {project} = RHAI namespace (K8s namespace)
      /projects/{project}/collections/{collection}  -- collection within the RHAI ns
      /projects/{project}/collections/{collection}/tables/{table}  -- display name

    All registry operations use project=CATALOG_PROJECT.
    DB name = make_scoped_name(project, collection, display_name).
    """
    router = APIRouter(tags=["catalog-api"])

    # -----------------------------------------------------------------------
    # Projects (= RHAI namespaces)
    # -----------------------------------------------------------------------

    @router.get("/projects")
    async def list_projects() -> ProjectListResponse:
        """List RHAI namespaces.

        Authorization is handled by kube-rbac-proxy before requests reach
        this endpoint, so we simply return all known namespaces.
        """
        ensure_catalog_project(store)
        namespace_names = sorted(list_rhai_namespaces(store))
        return ProjectListResponse(projects=namespace_names)

    # -----------------------------------------------------------------------
    # Collections
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections")
    def list_collections(project: str) -> CollectionListResponse:
        ensure_catalog_project(store)
        collections = list_collections_for_ns(store, project)
        if not collections:
            collections.add(DEFAULT_COLLECTION)

        result = [CollectionResponse(name=n) for n in sorted(collections)]
        return CollectionListResponse(collections=result)

    @router.post("/projects/{project}/collections", status_code=201)
    def create_collection(
        project: str, request: CreateCollectionRequest
    ) -> CollectionResponse:
        ensure_catalog_project(store)
        name = request.name

        existing = list_collections_for_ns(store, project)
        if name in existing:
            raise _conflict(f"Collection already exists: {name}")

        # Store _ns_{project}_{collection} tag on CATALOG_PROJECT
        proj = store.registry.get_project(CATALOG_PROJECT, allow_cache=False)
        tags = dict(proj.tags) if proj.tags else {}
        ns_tag_key = f"_ns_{project}_{name}"
        tags[ns_tag_key] = json.dumps({})
        proj.tags = tags
        store.registry.apply_project(proj, commit=True)

        return CollectionResponse(name=name)

    @router.delete("/projects/{project}/collections/{collection}", status_code=204)
    def delete_collection(project: str, collection: str) -> Response:
        ensure_catalog_project(store)

        # Check the collection is empty
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            namespace=project,
        )
        ns_datasets = [
            ds for ds in datasets if (ds.collection or DEFAULT_COLLECTION) == collection
        ]
        if ns_datasets:
            raise _conflict(
                f"Collection is not empty: {collection} "
                f"({len(ns_datasets)} asset(s) remain)"
            )

        # Remove _ns_{project}_{collection} tag from CATALOG_PROJECT
        try:
            proj = store.registry.get_project(CATALOG_PROJECT, allow_cache=False)
            ns_tag_key = f"_ns_{project}_{collection}"
            tags = dict(proj.tags) if proj.tags else {}
            if ns_tag_key in tags:
                del tags[ns_tag_key]
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
        ensure_catalog_project(store)
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags={"asset_type": TABLE_ASSET_TYPE},
            namespace=project,
        )
        filtered = [
            ds for ds in datasets if (ds.collection or DEFAULT_COLLECTION) == collection
        ]
        return AssetListResponse(
            assets=[_saved_dataset_to_asset(ds) for ds in filtered]
        )

    @router.post("/projects/{project}/collections/{collection}/tables", status_code=201)
    def create_table(
        project: str, collection: str, request: CreateTableRequest
    ) -> AssetResponse:
        ensure_catalog_project(store)

        scoped = make_scoped_name(project, collection, request.name)
        try:
            existing = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
            if existing.tags.get("asset_type") == TABLE_ASSET_TYPE:
                raise _conflict(f"Table already exists: {collection}.{request.name}")
        except FeastObjectNotFoundException:
            pass

        location = (
            request.location or ""
        )

        columns: list[dict] = []
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

        tags: Dict[str, str] = {
            "asset_type": TABLE_ASSET_TYPE,
            "format": request.format,
            "location": location,
            "connection-ref": request.connection_ref or "",
            "description": request.description or "",
        }
        if request.tags:
            tags["_tags"] = ",".join(request.tags)
        if request.properties:
            for k, v in request.properties.items():
                if k not in tags:
                    tags[k] = v

        ds = SavedDataset(
            name=scoped,
            tags=tags,
            namespace=project,
            collection=collection,
            columns=columns,
        )
        store.registry.apply_saved_dataset(ds, project=CATALOG_PROJECT, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/projects/{project}/collections/{collection}/tables/{table}")
    def get_table(project: str, collection: str, table: str) -> AssetResponse:
        ensure_catalog_project(store)
        scoped = make_scoped_name(project, collection, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Table not found: {collection}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {collection}.{table}")
        return _saved_dataset_to_asset(ds)

    @router.delete(
        "/projects/{project}/collections/{collection}/tables/{table}",
        status_code=204,
    )
    def delete_table(project: str, collection: str, table: str) -> Response:
        ensure_catalog_project(store)
        scoped = make_scoped_name(project, collection, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Table not found: {collection}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {collection}.{table}")
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True
        )
        return Response(status_code=204)

    # -----------------------------------------------------------------------
    # Volumes
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections/{collection}/volumes")
    def list_volumes(project: str, collection: str) -> AssetListResponse:
        ensure_catalog_project(store)
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags={"asset_type": VOLUME_ASSET_TYPE},
            namespace=project,
        )
        filtered = [
            ds for ds in datasets if (ds.collection or DEFAULT_COLLECTION) == collection
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
        ensure_catalog_project(store)

        scoped = make_scoped_name(project, collection, request.name)
        try:
            existing = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
            if existing.tags.get("asset_type") == VOLUME_ASSET_TYPE:
                raise _conflict(f"Volume already exists: {collection}.{request.name}")
        except FeastObjectNotFoundException:
            pass

        tags: Dict[str, str] = {
            "asset_type": VOLUME_ASSET_TYPE,
            "location": request.location,
            "content_type": request.content_type or "",
            "connection-ref": request.connection_ref or "",
            "description": request.description or "",
        }
        if request.tags:
            tags["_tags"] = ",".join(request.tags)
        if request.properties:
            for k, v in request.properties.items():
                if k not in tags:
                    tags[k] = v

        ds = SavedDataset(
            name=scoped,
            tags=tags,
            namespace=project,
            collection=collection,
        )
        store.registry.apply_saved_dataset(ds, project=CATALOG_PROJECT, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/projects/{project}/collections/{collection}/volumes/{volume}")
    def get_volume(project: str, collection: str, volume: str) -> AssetResponse:
        ensure_catalog_project(store)
        scoped = make_scoped_name(project, collection, volume)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        if ds.tags.get("asset_type") != VOLUME_ASSET_TYPE:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        return _saved_dataset_to_asset(ds)

    @router.delete(
        "/projects/{project}/collections/{collection}/volumes/{volume}",
        status_code=204,
    )
    def delete_volume(project: str, collection: str, volume: str) -> Response:
        ensure_catalog_project(store)
        scoped = make_scoped_name(project, collection, volume)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        if ds.tags.get("asset_type") != VOLUME_ASSET_TYPE:
            raise _not_found(f"Volume not found: {collection}.{volume}")
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True
        )
        return Response(status_code=204)

    # -----------------------------------------------------------------------
    # Databases
    # -----------------------------------------------------------------------

    @router.get("/projects/{project}/collections/{collection}/databases")
    def list_databases(project: str, collection: str) -> AssetListResponse:
        ensure_catalog_project(store)
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags={"asset_type": DATABASE_ASSET_TYPE},
            namespace=project,
        )
        filtered = [
            ds for ds in datasets if (ds.collection or DEFAULT_COLLECTION) == collection
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
        ensure_catalog_project(store)

        if request.db_type not in SUPPORTED_DB_TYPES:
            raise _bad_request(
                f"Unsupported database type: {request.db_type}. "
                f"Supported: {', '.join(SUPPORTED_DB_TYPES)}"
            )

        scoped = make_scoped_name(project, collection, request.name)
        try:
            existing = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
            if existing.tags.get("asset_type") == DATABASE_ASSET_TYPE:
                raise _conflict(f"Database already exists: {collection}.{request.name}")
        except FeastObjectNotFoundException:
            pass

        tags: Dict[str, str] = {
            "asset_type": DATABASE_ASSET_TYPE,
            "db_type": request.db_type,
            "host": request.host,
            "database": request.database,
            "schemas": ",".join(request.schemas) if request.schemas else "",
            "connection-ref": request.connection_ref or "",
            "description": request.description or "",
        }
        if request.tags:
            tags["_tags"] = ",".join(request.tags)
        if request.properties:
            for k, v in request.properties.items():
                if k not in tags:
                    tags[k] = v

        ds = SavedDataset(
            name=scoped,
            tags=tags,
            namespace=project,
            collection=collection,
        )
        store.registry.apply_saved_dataset(ds, project=CATALOG_PROJECT, commit=True)
        return _saved_dataset_to_asset(ds)

    @router.get("/projects/{project}/collections/{collection}/databases/{database}")
    def get_database(project: str, collection: str, database: str) -> AssetResponse:
        ensure_catalog_project(store)
        scoped = make_scoped_name(project, collection, database)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Database not found: {collection}.{database}")
        if ds.tags.get("asset_type") != DATABASE_ASSET_TYPE:
            raise _not_found(f"Database not found: {collection}.{database}")
        return _saved_dataset_to_asset(ds)

    @router.delete(
        "/projects/{project}/collections/{collection}/databases/{database}",
        status_code=204,
    )
    def delete_database(project: str, collection: str, database: str) -> Response:
        ensure_catalog_project(store)
        scoped = make_scoped_name(project, collection, database)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Database not found: {collection}.{database}")
        if ds.tags.get("asset_type") != DATABASE_ASSET_TYPE:
            raise _not_found(f"Database not found: {collection}.{database}")
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True
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
        tag: Optional[str] = Query(
            default=None,
            description="Filter by tag.",
        ),
    ) -> AssetListResponse:
        ensure_catalog_project(store)

        tag_filter: Dict[str, str] = {}
        if asset_type:
            tag_filter["asset_type"] = asset_type

        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            tags=tag_filter if tag_filter else None,
            namespace=project,
        )

        results: List[SavedDataset] = []
        query_lower = q.lower()

        for ds in datasets:
            if collection and (ds.collection or DEFAULT_COLLECTION) != collection:
                continue
            if tag:
                asset_tags = [t for t in ds.tags.get("_tags", "").split(",") if t]
                if tag not in asset_tags:
                    continue
            if query_lower:
                display = parse_display_name(ds.name)
                name_match = query_lower in display.lower()
                desc = ds.tags.get("description") or ds.tags.get("comment") or ""
                desc_match = query_lower in desc.lower()
                tag_match = any(
                    query_lower in v.lower()
                    for k, v in ds.tags.items()
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
            description="Text query. Empty returns all assets across namespaces.",
        ),
        asset_type: Optional[str] = Query(
            default=None,
            description="Filter by asset type: table, volume, database.",
        ),
        project: Optional[str] = Query(
            default=None,
            description="Restrict to a single RHAI namespace.",
        ),
    ) -> AssetListResponse:
        ensure_catalog_project(store)
        all_ns = sorted(list_rhai_namespaces(store))
        if project:
            ns_names = [n for n in all_ns if n == project]
        else:
            ns_names = all_ns

        all_assets: List[AssetResponse] = []
        query_lower = q.lower()

        for ns_name in ns_names:
            tag_filter: Dict[str, str] = {}
            if asset_type:
                tag_filter["asset_type"] = asset_type

            try:
                datasets = store.registry.list_saved_datasets(
                    project=CATALOG_PROJECT,
                    allow_cache=False,
                    tags=tag_filter if tag_filter else None,
                    namespace=ns_name,
                )
            except Exception:
                logger.warning("Failed to list datasets for namespace %s", ns_name)
                continue

            for ds in datasets:
                if query_lower:
                    display = parse_display_name(ds.name)
                    name_match = query_lower in display.lower()
                    desc = ds.tags.get("description") or ds.tags.get("comment") or ""
                    desc_match = query_lower in desc.lower()
                    tag_match = any(
                        query_lower in v.lower()
                        for k, v in ds.tags.items()
                    )
                    if not (name_match or desc_match or tag_match):
                        continue
                all_assets.append(_saved_dataset_to_asset(ds))

        return AssetListResponse(assets=all_assets)

    # -----------------------------------------------------------------------
    # Tags
    # -----------------------------------------------------------------------

    class TagListResponse(BaseModel):
        tags: List[str]

    @router.get("/projects/{project}/tags")
    def list_tags(project: str) -> TagListResponse:
        ensure_catalog_project(store)
        datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            namespace=project,
        )
        all_tags: set[str] = set()
        for ds in datasets:
            raw = ds.tags.get("_tags", "")
            if raw:
                all_tags.update(t for t in raw.split(",") if t)
        return TagListResponse(tags=sorted(all_tags))

    return router


# ---------------------------------------------------------------------------
# Generic-tables extension router (Iceberg path pattern)
# ---------------------------------------------------------------------------


def get_generic_tables_router(store: FeatureStore) -> APIRouter:
    router = APIRouter(tags=["generic-tables"])

    def _ensure() -> None:
        ensure_catalog_project(store)

    @router.get("/{prefix}/namespaces/{namespace}/generic-tables")
    def list_generic_tables(
        prefix: str,
        namespace: str,
        tag: Optional[str] = Query(default=None, description="Filter by tag."),
    ) -> AssetListResponse:
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
        if tag:
            datasets = [
                ds for ds in datasets
                if tag in [t for t in ds.tags.get("_tags", "").split(",") if t]
            ]
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
                raise _conflict(f"Table already exists: {namespace}.{body.name}")
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
            "_asset_uuid": str(uuid.uuid4()),
            "format": body.format,
            "location": location,
            "connection-ref": body.connection_ref or "",
            "description": body.description or "",
            "registered_by": request.headers.get("X-User") or request.headers.get("kubeflow-userid", "unknown"),
        }
        if body.tags:
            tags["_tags"] = ",".join(body.tags)
        if body.purpose:
            tags["purpose"] = body.purpose
        if body.license:
            tags["license"] = body.license
        if body.maturity:
            tags["maturity"] = body.maturity
        if body.domain:
            tags["domain"] = body.domain
        if body.pii:
            tags["pii"] = body.pii
        if body.owner:
            tags["owner"] = body.owner
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
            raise _not_found(f"Table not found: {namespace}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {namespace}.{table}")
        if (ds.collection or DEFAULT_COLLECTION) != collection:
            raise _not_found(f"Table not found: {namespace}.{table}")
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
            raise _not_found(f"Table not found: {namespace}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {namespace}.{table}")
        if (ds.collection or DEFAULT_COLLECTION) != collection:
            raise _not_found(f"Table not found: {namespace}.{table}")
        store.registry.delete_saved_dataset(
            scoped, project=CATALOG_PROJECT, commit=True, namespace=rhai_ns,
        )
        return Response(status_code=204)

    @router.patch("/{prefix}/namespaces/{namespace}/generic-tables/{table}")
    def update_generic_table(
        prefix: str, namespace: str, table: str, body: UpdateGenericTableRequest, request: Request
    ) -> AssetResponse:
        _ensure()
        rhai_ns = prefix
        collection = namespace
        scoped = make_scoped_name(rhai_ns, collection, table)
        try:
            ds = store.registry.get_saved_dataset(
                scoped, project=CATALOG_PROJECT, allow_cache=False, namespace=rhai_ns,
            )
        except FeastObjectNotFoundException:
            raise _not_found(f"Table not found: {namespace}.{table}")
        if ds.tags.get("asset_type") != TABLE_ASSET_TYPE:
            raise _not_found(f"Table not found: {namespace}.{table}")
        if (ds.collection or DEFAULT_COLLECTION) != collection:
            raise _not_found(f"Table not found: {namespace}.{table}")

        tags = dict(ds.tags)
        if body.description is not None:
            tags["description"] = body.description
        if body.format is not None:
            tags["format"] = body.format
        if body.location is not None:
            tags["location"] = body.location
        if body.connection_ref is not None:
            tags["connection-ref"] = body.connection_ref
        if body.purpose is not None:
            tags["purpose"] = body.purpose
        if body.license is not None:
            tags["license"] = body.license
        if body.maturity is not None:
            tags["maturity"] = body.maturity
        if body.domain is not None:
            tags["domain"] = body.domain
        if body.pii is not None:
            tags["pii"] = body.pii
        if body.owner is not None:
            tags["owner"] = body.owner
        if body.tags is not None:
            tags["_tags"] = ",".join(body.tags)
        tags["updated_by"] = request.headers.get("X-User") or request.headers.get("kubeflow-userid", "unknown")
        if body.properties is not None:
            for k, v in body.properties.items():
                if k not in SYSTEM_TAGS and not k.startswith("_"):
                    tags[k] = v

        columns = ds.columns
        if body.schema_fields is not None:
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
            if col_meta:
                tags["_col_meta"] = json.dumps(col_meta)

        updated = SavedDataset(
            name=scoped, namespace=rhai_ns, collection=collection,
            tags=tags, columns=columns,
        )
        store.registry.apply_saved_dataset(updated, project=CATALOG_PROJECT, commit=True)
        return _saved_dataset_to_asset(updated)

    return router
