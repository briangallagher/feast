"""Tests for multi-level catalog search (Level 1/2/3).

Tests the three search levels defined in design doc 06 §11:
  Level 1: GET /v1/search — cross-project
  Level 2: GET /v1/{prefix}/search — single project
  Level 3: GET /v1/{prefix}/search?namespace=X — single collection
"""

import json
import os
import tempfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from feast import FeatureStore
from feast.api.catalog import add_catalog_routes
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.project import Project
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset


@pytest.fixture
def catalog_test_app():
    """Multi-project catalog fixture with collections, tables, and volumes.

    Creates three projects (namespaces):
      - ns_underwriting: collections [underwriting, claims]
      - ns_actuarial: collections [models, datasets]
      - ns_regulatory: collections [compliance]

    Each collection has tables and/or volumes with searchable properties.
    """
    os.environ["DATACATALOG_SSAR_ENABLED"] = "false"

    tmp_dir = tempfile.TemporaryDirectory()
    registry_path = os.path.join(tmp_dir.name, "registry.db")
    dummy_path = os.path.join(tmp_dir.name, "dummy.parquet")

    import pandas as pd
    pd.DataFrame({"x": [1]}).to_parquet(dummy_path)

    config = {
        "registry": registry_path,
        "project": "ns_underwriting",
        "provider": "local",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": ":memory:"},
    }
    store = FeatureStore(config=RepoConfig.model_validate(config))

    # --- Project 1: ns_underwriting ---
    proj1 = Project(
        name="ns_underwriting",
        description="Underwriting namespace",
        tags={
            "domain": "insurance",
            "_ns_meta_underwriting": json.dumps({
                "description": "Property & casualty underwriting data",
                "domain": "underwriting",
                "LOB": "property-casualty",
            }),
            "_ns_meta_claims": json.dumps({
                "description": "Claims processing and analytics",
                "domain": "claims",
                "LOB": "property-casualty",
            }),
        },
    )
    store.registry.apply_project(proj1, commit=True)

    # Tables in 'underwriting' collection
    storage = SavedDatasetFileStorage(path=dummy_path)

    datasets_p1 = [
        SavedDataset(
            name="regulatory_bulletins",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "Regulatory bulletins from state DOIs",
                "domain": "regulatory",
                "LOB": "property-casualty",
                "format": "iceberg",
                "connection-ref": "dataconnection-minio-underwriting",
            },
        ),
        SavedDataset(
            name="underwriting_guidelines",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "Internal underwriting guidelines and rules",
                "domain": "underwriting",
                "LOB": "property-casualty",
                "format": "iceberg",
                "owner": "uw-team",
            },
        ),
        SavedDataset(
            name="claims_analysis_2026",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "Claims analysis aggregated data for 2026",
                "domain": "claims",
                "LOB": "property-casualty",
                "format": "iceberg",
                "pii": "true",
                "tier": "silver",
                "owner": "claims-analytics-team",
            },
        ),
        SavedDataset(
            name="regulatory_bulletin_docs",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "volume",
                "comment": "Raw regulatory bulletin PDFs",
                "domain": "regulatory",
                "LOB": "property-casualty",
                "doc_count": "5",
            },
        ),
        SavedDataset(
            name="iso_forms",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "volume",
                "comment": "ISO standard forms library",
                "domain": "underwriting",
                "LOB": "property-casualty",
                "doc_count": "12",
            },
        ),
    ]

    # Set namespace (collection) on datasets
    datasets_p1[0].namespace = "underwriting"
    datasets_p1[1].namespace = "underwriting"
    datasets_p1[2].namespace = "claims"
    datasets_p1[3].namespace = "underwriting"
    datasets_p1[4].namespace = "underwriting"

    for ds in datasets_p1:
        store.registry.apply_saved_dataset(ds, "ns_underwriting", commit=False)
    store.registry.commit()

    # --- Project 2: ns_actuarial ---
    proj2 = Project(
        name="ns_actuarial",
        description="Actuarial namespace",
        tags={
            "domain": "actuarial",
            "_ns_meta_models": json.dumps({
                "description": "Actuarial pricing and risk models",
                "domain": "actuarial",
                "team": "pricing-team",
            }),
            "_ns_meta_datasets": json.dumps({
                "description": "Training and evaluation datasets",
                "domain": "actuarial",
                "team": "data-science",
            }),
        },
    )
    store.registry.apply_project(proj2, commit=True)

    datasets_p2 = [
        SavedDataset(
            name="flood_risk_scores",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "Flood risk scores by postal code",
                "domain": "risk",
                "model_version": "v3.2",
                "format": "iceberg",
            },
        ),
        SavedDataset(
            name="hurricane_exposure",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "Hurricane exposure analysis for coastal properties",
                "domain": "catastrophe",
                "format": "parquet",
            },
        ),
        SavedDataset(
            name="training_data_2025",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "Model training dataset for 2025 pricing cycle",
                "domain": "actuarial",
                "purpose": "training",
                "row_count": "1500000",
            },
        ),
    ]

    datasets_p2[0].namespace = "models"
    datasets_p2[1].namespace = "models"
    datasets_p2[2].namespace = "datasets"

    for ds in datasets_p2:
        store.registry.apply_saved_dataset(ds, "ns_actuarial", commit=False)
    store.registry.commit()

    # --- Project 3: ns_regulatory ---
    proj3 = Project(
        name="ns_regulatory",
        description="Regulatory compliance namespace",
        tags={
            "domain": "regulatory",
            "_ns_meta_compliance": json.dumps({
                "description": "Regulatory compliance data and filings",
                "domain": "regulatory",
                "retention_years": "7",
            }),
        },
    )
    store.registry.apply_project(proj3, commit=True)

    datasets_p3 = [
        SavedDataset(
            name="state_filings",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "table",
                "comment": "State regulatory filings tracker",
                "domain": "regulatory",
                "format": "iceberg",
            },
        ),
        SavedDataset(
            name="compliance_docs",
            features=[],
            join_keys=[],
            storage=storage,
            tags={
                "_catalog_managed": "true",
                "asset_type": "volume",
                "comment": "Compliance documentation archive",
                "domain": "regulatory",
                "doc_count": "230",
            },
        ),
    ]

    datasets_p3[0].namespace = "compliance"
    datasets_p3[1].namespace = "compliance"

    for ds in datasets_p3:
        store.registry.apply_saved_dataset(ds, "ns_regulatory", commit=False)
    store.registry.commit()

    # Build FastAPI app with catalog routes
    app = FastAPI()
    add_catalog_routes(app, store)
    client = TestClient(app)

    yield client, store

    tmp_dir.cleanup()
    os.environ.pop("DATACATALOG_SSAR_ENABLED", None)


# ===========================================================================
# Level 2: Single-project search (GET /v1/{prefix}/search)
# ===========================================================================


class TestLevel2SingleProjectSearch:
    """Level 2: search within a single project."""

    def test_search_returns_tables_and_volumes(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=")
        assert resp.status_code == 200
        data = resp.json()

        types = {r["type"] for r in data["results"]}
        assert "table" in types
        assert "volume" in types

    def test_search_returns_collections(self, catalog_test_app):
        """Level 2 should include collections as searchable entities."""
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=")
        assert resp.status_code == 200
        data = resp.json()

        collection_results = [r for r in data["results"] if r["type"] == "collection"]
        assert len(collection_results) >= 2
        names = {r["name"] for r in collection_results}
        assert "underwriting" in names
        assert "claims" in names

    def test_search_collection_has_metadata(self, catalog_test_app):
        """Collection results should include metadata from _ns_meta_ tags."""
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=underwriting")
        data = resp.json()

        collections = [r for r in data["results"] if r["type"] == "collection"]
        assert len(collections) >= 1

        uw_coll = next(c for c in collections if c["name"] == "underwriting")
        assert uw_coll["description"] == "Property & casualty underwriting data"
        assert uw_coll["properties"]["domain"] == "underwriting"
        assert uw_coll["project"] == "ns_underwriting"

    def test_search_by_name_substring(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=regulatory")
        data = resp.json()

        names = {r["name"] for r in data["results"]}
        assert "regulatory_bulletins" in names
        assert "regulatory_bulletin_docs" in names

    def test_search_by_tag_value(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=silver")
        data = resp.json()

        results = data["results"]
        assert len(results) >= 1
        assert any(r["name"] == "claims_analysis_2026" for r in results)

    def test_search_by_description(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=state DOIs")
        data = resp.json()

        results = data["results"]
        assert any(r["name"] == "regulatory_bulletins" for r in results)

    def test_search_property_filter(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&properties=domain:regulatory")
        data = resp.json()

        for r in data["results"]:
            if r["type"] != "collection":
                assert "regulatory" in r["properties"].get("domain", "").lower()

    def test_search_type_filter_table(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&type=table")
        data = resp.json()

        for r in data["results"]:
            assert r["type"] == "table"

    def test_search_type_filter_volume(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&type=volume")
        data = resp.json()

        for r in data["results"]:
            assert r["type"] == "volume"

    def test_search_type_filter_collection(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&type=collection")
        data = resp.json()

        for r in data["results"]:
            assert r["type"] == "collection"
        assert len(data["results"]) >= 2

    def test_search_pagination(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&page_size=2&page=1")
        data = resp.json()

        assert len(data["results"]) <= 2
        assert data["total"] > 2
        assert data["page"] == 1
        assert data["limit"] == 2

        resp2 = client.get("/v1/ns_underwriting/search?query=&page_size=2&page=2")
        data2 = resp2.json()
        assert data2["page"] == 2
        # Results on page 2 should be different from page 1
        names_p1 = {r["name"] for r in data["results"]}
        names_p2 = {r["name"] for r in data2["results"]}
        assert names_p1.isdisjoint(names_p2)

    def test_search_sort_by_name(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&sort_by=name")
        data = resp.json()

        names = [r["name"] for r in data["results"]]
        assert names == sorted(names)

    def test_search_sort_by_score(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=underwriting&sort_by=score")
        data = resp.json()

        scores = [r["score"] for r in data["results"]]
        assert scores == sorted(scores, reverse=True)

    def test_search_empty_query_returns_all(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=")
        data = resp.json()

        # 5 datasets + 2 collections = 7
        assert data["total"] == 7

    def test_search_no_results(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=xyznonexistent999")
        data = resp.json()

        assert data["total"] == 0
        assert data["results"] == []

    def test_search_nonexistent_project(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/nonexistent_project/search?query=test")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0

    def test_search_result_has_project_field(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=")
        data = resp.json()

        for r in data["results"]:
            assert r["project"] == "ns_underwriting"

    def test_search_fuzzy_match(self, catalog_test_app):
        """Typo should still find results via fuzzy matching."""
        client, _ = catalog_test_app
        # "osi_forms" is NOT a substring of "iso_forms" but has identical character set
        # {o,s,i,_,f,r,m} vs {i,s,o,_,f,r,m} → 7/7 = 1.0 overlap ≥ 0.75 threshold
        resp = client.get("/v1/ns_underwriting/search?query=osi_forms")
        data = resp.json()

        assert data["total"] > 0
        fuzzy_result = next(r for r in data["results"] if r["name"] == "iso_forms")
        assert fuzzy_result["score"] == 40


# ===========================================================================
# Level 3: Single-collection search (GET /v1/{prefix}/search?namespace=X)
# ===========================================================================


class TestLevel3SingleCollectionSearch:
    """Level 3: search restricted to a single collection."""

    def test_namespace_filter_restricts_results(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&namespace=claims")
        data = resp.json()

        for r in data["results"]:
            assert r["namespace"] == ["claims"]

    def test_namespace_filter_excludes_collections(self, catalog_test_app):
        """At Level 3, collections should NOT appear in results."""
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&namespace=underwriting")
        data = resp.json()

        types = {r["type"] for r in data["results"]}
        assert "collection" not in types

    def test_namespace_filter_only_matching_assets(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&namespace=claims")
        data = resp.json()

        # Only claims_analysis_2026 is in the 'claims' namespace
        assert data["total"] == 1
        assert data["results"][0]["name"] == "claims_analysis_2026"

    def test_namespace_filter_with_query(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=regulatory&namespace=underwriting")
        data = resp.json()

        for r in data["results"]:
            assert r["namespace"] == ["underwriting"]
            assert "regulatory" in r["name"].lower() or "regulatory" in (r.get("description") or "").lower() or any("regulatory" in str(v).lower() for v in r["properties"].values())

    def test_namespace_filter_nonexistent_collection(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=&namespace=nonexistent")
        data = resp.json()

        assert data["total"] == 0
        assert data["results"] == []


# ===========================================================================
# Level 1: Cross-project search (GET /v1/search)
# ===========================================================================


class TestLevel1CrossProjectSearch:
    """Level 1: search across all projects."""

    def test_cross_project_search_returns_results_from_all(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=")
        assert resp.status_code == 200
        data = resp.json()

        projects_found = {r["project"] for r in data["results"]}
        assert "ns_underwriting" in projects_found
        assert "ns_actuarial" in projects_found
        assert "ns_regulatory" in projects_found

    def test_cross_project_search_includes_collections(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=")
        data = resp.json()

        collections = [r for r in data["results"] if r["type"] == "collection"]
        collection_names = {r["name"] for r in collections}
        assert "underwriting" in collection_names
        assert "claims" in collection_names
        assert "models" in collection_names
        assert "datasets" in collection_names
        assert "compliance" in collection_names

    def test_cross_project_search_total_count(self, catalog_test_app):
        """All assets across all projects should be returned."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=")
        data = resp.json()

        # ns_underwriting: 5 assets + 2 collections = 7
        # ns_actuarial: 3 assets + 2 collections = 5
        # ns_regulatory: 2 assets + 1 collection = 3
        # Total = 15
        assert data["total"] == 15

    def test_cross_project_search_by_keyword(self, catalog_test_app):
        """Search for 'flood' should find flood_risk_scores in ns_actuarial."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=flood")
        data = resp.json()

        assert data["total"] >= 1
        names = {r["name"] for r in data["results"]}
        assert "flood_risk_scores" in names

    def test_cross_project_search_by_domain_tag(self, catalog_test_app):
        """Search for 'regulatory' finds assets across multiple projects."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=regulatory")
        data = resp.json()

        projects = {r["project"] for r in data["results"]}
        # Should find in ns_underwriting (regulatory_bulletins, regulatory_bulletin_docs)
        # and ns_regulatory (state_filings has domain=regulatory, compliance collection has domain=regulatory)
        assert "ns_underwriting" in projects
        assert "ns_regulatory" in projects

    def test_cross_project_filter_by_projects_param(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&projects=ns_actuarial")
        data = resp.json()

        for r in data["results"]:
            assert r["project"] == "ns_actuarial"

    def test_cross_project_filter_multiple_projects(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&projects=ns_actuarial&projects=ns_regulatory")
        data = resp.json()

        projects_found = {r["project"] for r in data["results"]}
        assert projects_found <= {"ns_actuarial", "ns_regulatory"}
        assert "ns_underwriting" not in projects_found

    def test_cross_project_filter_nonexistent_project(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&projects=nonexistent")
        data = resp.json()

        assert data["total"] == 0
        assert data["results"] == []

    def test_cross_project_type_filter(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&type=volume")
        data = resp.json()

        for r in data["results"]:
            assert r["type"] == "volume"
        names = {r["name"] for r in data["results"]}
        assert "regulatory_bulletin_docs" in names
        assert "iso_forms" in names
        assert "compliance_docs" in names

    def test_cross_project_property_filter(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&properties=format:iceberg")
        data = resp.json()

        for r in data["results"]:
            if r["type"] != "collection":
                assert "iceberg" in r["properties"].get("format", "").lower()

    def test_cross_project_namespace_filter(self, catalog_test_app):
        """Namespace filter restricts to a collection name across all projects."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&namespace=models")
        data = resp.json()

        # Only assets in the 'models' collection of ns_actuarial
        for r in data["results"]:
            assert r["namespace"] == ["models"]
            assert r["project"] == "ns_actuarial"

    def test_cross_project_pagination(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&page_size=5&page=1")
        data = resp.json()

        assert len(data["results"]) == 5
        assert data["total"] == 15

        resp2 = client.get("/v1/search?query=&page_size=5&page=2")
        data2 = resp2.json()
        assert len(data2["results"]) == 5

        resp3 = client.get("/v1/search?query=&page_size=5&page=3")
        data3 = resp3.json()
        assert len(data3["results"]) == 5

        # All pages should have distinct results
        all_names = (
            [r["name"] for r in data["results"]]
            + [r["name"] for r in data2["results"]]
            + [r["name"] for r in data3["results"]]
        )
        # Some names may repeat across projects (e.g., collection names)
        # but the (name, project) pair should be unique
        all_pairs = [
            (r["name"], r["project"]) for r in data["results"]
        ] + [
            (r["name"], r["project"]) for r in data2["results"]
        ] + [
            (r["name"], r["project"]) for r in data3["results"]
        ]
        assert len(all_pairs) == len(set(all_pairs))

    def test_cross_project_sort_by_score(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=flood&sort_by=score")
        data = resp.json()

        scores = [r["score"] for r in data["results"]]
        assert scores == sorted(scores, reverse=True)

    def test_cross_project_sort_by_name(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&sort_by=name")
        data = resp.json()

        names = [r["name"] for r in data["results"]]
        assert names == sorted(names)

    def test_cross_project_collection_search_by_name(self, catalog_test_app):
        """Searching for 'claims' should return the 'claims' collection."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=claims")
        data = resp.json()

        collection_results = [r for r in data["results"] if r["type"] == "collection"]
        assert any(r["name"] == "claims" for r in collection_results)

    def test_cross_project_collection_search_by_property(self, catalog_test_app):
        """Searching for a collection property value should find it."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=pricing-team")
        data = resp.json()

        # The 'models' collection has team=pricing-team
        assert any(
            r["name"] == "models" and r["type"] == "collection"
            for r in data["results"]
        )


# ===========================================================================
# Scoring and ranking tests
# ===========================================================================


class TestSearchScoring:
    """Verify scoring tiers work correctly across all levels."""

    def test_exact_name_match_scores_100(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=iso_forms")
        data = resp.json()

        exact = next(r for r in data["results"] if r["name"] == "iso_forms")
        assert exact["score"] == 100

    def test_name_substring_scores_90(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=iso")
        data = resp.json()

        iso_result = next(r for r in data["results"] if r["name"] == "iso_forms")
        assert iso_result["score"] == 90

    def test_description_match_scores_80(self, catalog_test_app):
        client, _ = catalog_test_app
        # "state DOIs" appears in regulatory_bulletins description but not name or tags
        resp = client.get("/v1/ns_underwriting/search?query=DOIs")
        data = resp.json()

        bulletin = next(r for r in data["results"] if r["name"] == "regulatory_bulletins")
        assert bulletin["score"] == 80

    def test_tag_value_match_scores_60(self, catalog_test_app):
        client, _ = catalog_test_app
        # "silver" matches tier=silver tag on claims_analysis_2026
        resp = client.get("/v1/ns_underwriting/search?query=silver")
        data = resp.json()

        claims = next(r for r in data["results"] if r["name"] == "claims_analysis_2026")
        assert claims["score"] == 60

    def test_collection_exact_name_scores_100(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=claims")
        data = resp.json()

        coll = next(
            r for r in data["results"]
            if r["type"] == "collection" and r["name"] == "claims"
        )
        assert coll["score"] == 100

    def test_results_sorted_by_score_descending(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/ns_underwriting/search?query=regulatory")
        data = resp.json()

        scores = [r["score"] for r in data["results"]]
        assert scores == sorted(scores, reverse=True)


# ===========================================================================
# Edge cases
# ===========================================================================


class TestSearchEdgeCases:
    """Edge cases and error handling."""

    def test_empty_query_string(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0

    def test_page_beyond_results(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&page=999&page_size=10")
        assert resp.status_code == 200
        data = resp.json()
        assert data["results"] == []
        assert data["total"] > 0

    def test_page_size_limit_respected(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&page_size=3")
        data = resp.json()
        assert len(data["results"]) == 3

    def test_special_characters_in_query(self, catalog_test_app):
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=property-casualty")
        assert resp.status_code == 200

    def test_case_insensitive_search(self, catalog_test_app):
        client, _ = catalog_test_app
        resp_lower = client.get("/v1/ns_underwriting/search?query=regulatory")
        resp_upper = client.get("/v1/ns_underwriting/search?query=REGULATORY")

        data_lower = resp_lower.json()
        data_upper = resp_upper.json()

        assert data_lower["total"] == data_upper["total"]

    def test_cross_project_with_namespace_excludes_collections(self, catalog_test_app):
        """When namespace is specified in cross-project, no collections returned."""
        client, _ = catalog_test_app
        resp = client.get("/v1/search?query=&namespace=compliance")
        data = resp.json()

        types = {r["type"] for r in data["results"]}
        assert "collection" not in types
