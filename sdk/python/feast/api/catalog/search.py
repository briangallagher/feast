"""
Enhanced catalog search with property filtering, relevance scoring, and pagination.

Extension of the Iceberg REST Catalog API (which has no search endpoint).
Searches catalog-managed SavedDatasets stored in the Feast registry.
No database changes required — reads existing SavedDataset tags.
"""

from typing import List, Optional

from fastapi import APIRouter, Query

from feast import FeatureStore
from feast.api.catalog.mapping import CATALOG_MANAGED_TAG
from feast.api.catalog.models import SearchResponse, SearchResult
from feast.api.catalog.namespaces import DEFAULT_SCHEMA


def _compute_match_score(query: str, name: str, description: str, tags: dict) -> int:
    """Score a catalog asset against a search query.

    Scoring tiers (highest match wins):
      100 — exact name match (case-insensitive)
       90 — query is a substring of the name
       80 — query is a substring of the description
       60 — query matches a property/tag value
       40 — fuzzy match (≥75% character overlap with name)
        0 — no match
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

    Each filter is 'key:value' — both are case-insensitive substring matches.
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
            description="Restrict search to a single namespace name.",
        ),
        namespaces: Optional[List[str]] = Query(
            default=None,
            description="Restrict search to these namespace names (searches all if omitted).",
        ),
        properties: Optional[List[str]] = Query(
            default=None,
            description="Property filters as 'key:value' pairs. All must match. Example: properties=domain:flood&properties=format:iceberg",
        ),
        type_filter: Optional[str] = Query(
            default=None,
            alias="type",
            description="Filter by asset type: table, volume, iceberg_table, document_collection, vector_index, dataset.",
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
            default=50, ge=1, le=500, description="Results per page (default when page_size not specified)."
        ),
    ) -> SearchResponse:
        """Search catalog assets with property filtering, relevance scoring, and pagination.

        This is an RHOAI extension to the Iceberg REST Catalog API (the spec has no
        search endpoint). Searches catalog-managed SavedDatasets in the Feast registry
        using existing tags — no database changes required.

        Features beyond the basic substring search:
        - **Property filtering**: `?properties=domain:flood` filters by tag values
        - **Type filtering**: `?type=volume` or `?asset_type=volume` shows only volumes
        - **Namespace scoping**: `?namespace=underwriting` or `?namespaces=underwriting`
        - **Relevance scoring**: results ranked by match quality (exact > substring > property > fuzzy)
        - **Pagination**: `?page=1&page_size=10` (or `?limit=10`) for large catalogs
        - **Empty query**: `?query=` returns all assets (useful with property filters)
        """
        effective_namespaces = list(namespaces or [])
        if namespace and namespace not in effective_namespaces:
            effective_namespaces.append(namespace)

        effective_type = type_filter or asset_type

        effective_limit = page_size if page_size is not None else limit

        try:
            store.registry.get_project(prefix, allow_cache=False)
        except Exception:
            return SearchResponse(
                query=query, results=[], total=0, page=page, limit=effective_limit
            )

        datasets = store.registry.list_saved_datasets(
            project=prefix,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true"},
        )

        scored_results: list[tuple[int, SearchResult]] = []

        for ds in datasets:
            ds_asset_type = ds.tags.get("asset_type", "table")
            description = ds.tags.get("comment") or ds.tags.get("description")
            ds_ns = ds.namespace or DEFAULT_SCHEMA

            if effective_namespaces and ds_ns not in effective_namespaces:
                continue

            if effective_type and ds_asset_type != effective_type:
                continue

            if properties and not _matches_property_filters(ds.tags, properties):
                continue

            if query:
                score = _compute_match_score(query, ds.name, description or "", ds.tags)
                if score == 0:
                    continue
            else:
                score = 50

            ns = [ds_ns]
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
                        name=ds.name,
                        description=description,
                        properties=props,
                        score=score,
                    ),
                )
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
