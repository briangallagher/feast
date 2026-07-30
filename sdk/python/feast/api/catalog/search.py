"""
Multi-level catalog search with cross-namespace support, collection discovery,
property filtering, relevance scoring, and pagination.

Single-project model: all assets live under CATALOG_PROJECT ("data-registry").
RHAI namespaces are stored in SavedDataset.namespace; collections in
SavedDataset.collection.  Scoped names encode both:
  "insurance-demo/claims/auto-claims"

Three search levels:
  GET /v1/search              -- Level 1: cross-namespace (all accessible RHAI namespaces)
  GET /v1/{prefix}/search     -- Level 2: single RHAI namespace (all collections within)
  GET /v1/{prefix}/search?namespace=X -- Level 3: single collection (tables+volumes only)

Authorization is handled by kube-rbac-proxy before requests reach this code.

Extension of the Iceberg REST Catalog API (which has no search endpoint).
Searches catalog-managed SavedDatasets stored in the Feast registry.
"""

import json
import logging
from typing import List, Optional

from fastapi import APIRouter, Query

from feast import FeatureStore
from feast.api.catalog.mapping import (
    CATALOG_MANAGED_TAG,
    CATALOG_PROJECT,
    DEFAULT_COLLECTION,
    list_collections_for_ns,
    parse_display_name,
)
from feast.api.catalog.models import SearchResponse, SearchResult

logger = logging.getLogger(__name__)


def _compute_match_score(query: str, name: str, description: str, tags: dict) -> int:
    """Score a catalog asset against a search query.

    Scoring tiers (highest match wins):
      100 -- exact name match (case-insensitive)
       90 -- query is a substring of the name
       80 -- query is a substring of the description
       60 -- query matches a property/tag value
       40 -- fuzzy match (>=75% character overlap with name)
        0 -- no match
    """
    q = query.lower()

    if q == name.lower():
        return 100

    if q in name.lower():
        return 90

    if description and q in description.lower():
        return 80

    for key, val in tags.items():
        if key == CATALOG_MANAGED_TAG:
            continue
        if q in str(val).lower() or q in key.lower():
            return 60

    if len(q) >= 3 and _fuzzy_overlap(q, name.lower()) >= 0.75:
        return 40

    return 0


def _fuzzy_overlap(a: str, b: str) -> float:
    """Character-level overlap ratio between two strings."""
    if not a or not b:
        return 0.0
    set_a = set(a)
    set_b = set(b)
    intersection = set_a & set_b
    return len(intersection) / max(len(set_a), len(set_b))


def _matches_property_filters(tags: dict, filters: List[str]) -> bool:
    """Check if all property filters match the asset's tags.

    Each filter is 'key:value' -- both are case-insensitive substring matches.
    """
    for f in filters:
        if ":" not in f:
            continue
        key, value = f.split(":", 1)
        matched = False
        for tag_key, tag_val in tags.items():
            if tag_key == CATALOG_MANAGED_TAG:
                continue
            if key.lower() in tag_key.lower() and value.lower() in str(tag_val).lower():
                matched = True
                break
        if not matched:
            return False
    return True


def _get_collection_metadata(
    project_tags: dict, rhai_ns: str, collection_name: str
) -> dict:
    """Extract metadata for a collection from _ns_{rhai_ns}_{collection} tags."""
    meta_key = f"_ns_{rhai_ns}_{collection_name}"
    raw = project_tags.get(meta_key)
    if raw:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def _search_rhai_namespace(
    store: FeatureStore,
    rhai_ns: str,
    query: str,
    effective_collections: List[str],
    effective_type: Optional[str],
    properties: Optional[List[str]],
    include_collections: bool,
) -> List[tuple]:
    """Search within a single RHAI namespace, returning scored (score, SearchResult) tuples."""
    scored_results: list[tuple[int, SearchResult]] = []

    try:
        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=False)
    except Exception:
        return scored_results

    project_tags = dict(project.tags) if project.tags else {}

    # Search collections as entities -- Level 1 and 2 only
    if include_collections and (
        effective_type is None or effective_type == "collection"
    ):
        collections = list_collections_for_ns(store, rhai_ns)

        for coll_name in collections:
            if effective_collections and coll_name not in effective_collections:
                continue

            coll_meta = _get_collection_metadata(project_tags, rhai_ns, coll_name)
            coll_description = coll_meta.get("description", "")

            if properties and not _matches_property_filters(coll_meta, properties):
                continue

            if query:
                score = _compute_match_score(
                    query, coll_name, coll_description, coll_meta
                )
                if score == 0:
                    continue
            else:
                score = 50

            scored_results.append(
                (
                    score,
                    SearchResult(
                        type="collection",
                        namespace=[coll_name],
                        name=coll_name,
                        description=coll_description or None,
                        properties={
                            k: v for k, v in coll_meta.items() if not k.startswith("_")
                        },
                        score=score,
                        project=rhai_ns,
                    ),
                )
            )
        datasets = None
    else:
        datasets = None

    # Search tables and volumes
    if effective_type is None or effective_type in (
        "table",
        "volume",
        "iceberg_table",
        "document_collection",
        "vector_index",
        "dataset",
    ):
        if datasets is None:
            datasets = store.registry.list_saved_datasets(
                project=CATALOG_PROJECT,
                allow_cache=False,
                tags={CATALOG_MANAGED_TAG: "true"},
                namespace=rhai_ns,
            )

        for ds in datasets:
            ds_asset_type = ds.tags.get("asset_type", "table")
            description = ds.tags.get("comment") or ds.tags.get("description")
            ds_collection = ds.collection or DEFAULT_COLLECTION
            display_name = parse_display_name(ds.name)

            if effective_collections and ds_collection not in effective_collections:
                continue

            if effective_type and ds_asset_type != effective_type:
                continue

            if properties and not _matches_property_filters(ds.tags, properties):
                continue

            if query:
                score = _compute_match_score(
                    query, display_name, description or "", ds.tags
                )
                if score == 0:
                    continue
            else:
                score = 50

            ns = [ds_collection]
            props = {
                k: v
                for k, v in ds.tags.items()
                if k not in (CATALOG_MANAGED_TAG, "asset_type")
            }

            scored_results.append(
                (
                    score,
                    SearchResult(
                        type=ds_asset_type,
                        namespace=ns,
                        name=display_name,
                        description=description,
                        properties=props,
                        score=score,
                        project=rhai_ns,
                    ),
                )
            )

    return scored_results


def get_search_router(store: FeatureStore) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-search"])

    @router.get("/{prefix}/search")
    def search_catalog(
        prefix: str,
        query: str = Query(
            default="",
            description="Text search query (matches name, description, and property values). Empty string returns all assets.",
        ),
        namespace: Optional[str] = Query(
            default=None,
            description="Restrict search to a single collection name.",
        ),
        namespaces: Optional[List[str]] = Query(
            default=None,
            description="Restrict search to these collection names (searches all if omitted).",
        ),
        properties: Optional[List[str]] = Query(
            default=None,
            description="Property filters as 'key:value' pairs. All must match. Example: properties=domain:flood&properties=format:iceberg",
        ),
        type_filter: Optional[str] = Query(
            default=None,
            alias="type",
            description="Filter by asset type: table, volume, collection, iceberg_table, document_collection, vector_index, dataset.",
        ),
        asset_type: Optional[str] = Query(
            default=None,
            description="Filter by asset type (alternative to 'type' parameter).",
        ),
        sort_by: str = Query(
            default="score",
            description="Sort results by 'score' (relevance, descending) or 'name' (alphabetical).",
        ),
        page: int = Query(default=1, ge=1, description="Page number (1-indexed)."),
        page_size: Optional[int] = Query(
            default=None,
            ge=1,
            le=500,
            description="Results per page.",
        ),
        limit: int = Query(
            default=50,
            ge=1,
            le=500,
            description="Results per page (default when page_size not specified).",
        ),
    ) -> SearchResponse:
        """Search catalog assets within a single RHAI namespace (Level 2/3).

        Level 2: search all collections, tables, and volumes within the namespace.
        Level 3: add ?namespace=X to restrict to a single collection (tables+volumes only).
        """
        effective_collections = list(namespaces or [])
        if namespace and namespace not in effective_collections:
            effective_collections.append(namespace)

        effective_type = type_filter or asset_type
        effective_limit = page_size if page_size is not None else limit

        # Level 3: if collection filter is set, don't include collections in results
        include_collections = len(effective_collections) == 0

        scored_results = _search_rhai_namespace(
            store,
            prefix,
            query,
            effective_collections,
            effective_type,
            properties,
            include_collections,
        )

        if sort_by == "name":
            scored_results.sort(key=lambda x: x[1].name)
        else:
            scored_results.sort(key=lambda x: x[0], reverse=True)

        total = len(scored_results)
        start = (page - 1) * effective_limit
        page_results = [r for _, r in scored_results[start : start + effective_limit]]

        return SearchResponse(
            query=query,
            results=page_results,
            total=total,
            page=page,
            limit=effective_limit,
        )

    return router
