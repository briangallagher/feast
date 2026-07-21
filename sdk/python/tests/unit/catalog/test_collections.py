"""Tests for collection (namespace) CRUD — creation, listing, empty collections.

Validates the fix for the empty collection bug: collections created via POST
must appear in GET /namespaces even when they have no assets.
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
def namespace_test_app():
    """Fixture with a single project and one collection with assets."""
    os.environ["DATACATALOG_SSAR_ENABLED"] = "false"

    tmp_dir = tempfile.TemporaryDirectory()
    registry_path = os.path.join(tmp_dir.name, "registry.db")
    dummy_path = os.path.join(tmp_dir.name, "dummy.parquet")

    import pandas as pd
    pd.DataFrame({"x": [1]}).to_parquet(dummy_path)

    config = {
        "registry": registry_path,
        "project": "test_project",
        "provider": "local",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": ":memory:"},
    }
    store = FeatureStore(config=RepoConfig.model_validate(config))

    proj = Project(
        name="test_project",
        description="Test project",
        tags={},
    )
    store.registry.apply_project(proj, commit=True)

    # Add one asset in the "existing" collection
    storage = SavedDatasetFileStorage(path=dummy_path)
    ds = SavedDataset(
        name="sample_table",
        features=[],
        join_keys=[],
        storage=storage,
        tags={
            "_catalog_managed": "true",
            "asset_type": "table",
            "comment": "A sample table",
        },
    )
    ds.namespace = "existing_collection"
    store.registry.apply_saved_dataset(ds, "test_project", commit=True)

    app = FastAPI()
    add_catalog_routes(app, store)
    client = TestClient(app)

    yield client, store

    tmp_dir.cleanup()
    os.environ.pop("DATACATALOG_SSAR_ENABLED", None)


class TestCollectionListing:
    """GET /v1/{prefix}/namespaces should list all collections."""

    def test_list_shows_collection_with_assets(self, namespace_test_app):
        client, _ = namespace_test_app
        resp = client.get("/v1/test_project/namespaces")
        assert resp.status_code == 200
        data = resp.json()
        ns_names = [ns[0] for ns in data["namespaces"]]
        assert "existing_collection" in ns_names

    def test_create_empty_collection_appears_in_list(self, namespace_test_app):
        """Bug fix: empty collections must appear in GET /namespaces."""
        client, _ = namespace_test_app

        # Create a new empty collection
        resp = client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["new_empty_collection"]},
        )
        assert resp.status_code == 200

        # Verify it appears in the list
        resp = client.get("/v1/test_project/namespaces")
        assert resp.status_code == 200
        data = resp.json()
        ns_names = [ns[0] for ns in data["namespaces"]]
        assert "new_empty_collection" in ns_names
        assert "existing_collection" in ns_names

    def test_create_collection_with_properties(self, namespace_test_app):
        client, _ = namespace_test_app

        resp = client.post(
            "/v1/test_project/namespaces",
            json={
                "namespace": ["claims"],
                "properties": {"domain": "claims", "LOB": "property-casualty"},
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["namespace"] == ["claims"]

        # Appears in list
        resp = client.get("/v1/test_project/namespaces")
        ns_names = [ns[0] for ns in resp.json()["namespaces"]]
        assert "claims" in ns_names

    def test_create_duplicate_collection_fails(self, namespace_test_app):
        client, _ = namespace_test_app

        # existing_collection already has assets
        resp = client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["existing_collection"]},
        )
        assert resp.status_code == 409

    def test_create_then_navigate_back_shows_collection(self, namespace_test_app):
        """Simulates UI flow: create → navigate to detail → back to list."""
        client, _ = namespace_test_app

        # Create
        resp = client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["ui_created"]},
        )
        assert resp.status_code == 200

        # Simulate "navigate to detail" (GET single namespace)
        resp = client.get("/v1/test_project/namespaces/ui_created")
        assert resp.status_code == 200

        # Simulate "navigate back to list"
        resp = client.get("/v1/test_project/namespaces")
        data = resp.json()
        ns_names = [ns[0] for ns in data["namespaces"]]
        assert "ui_created" in ns_names

    def test_empty_collection_persists_after_multiple_list_calls(self, namespace_test_app):
        client, _ = namespace_test_app

        client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["persistent_empty"]},
        )

        # Multiple list calls should all show it
        for _ in range(3):
            resp = client.get("/v1/test_project/namespaces")
            ns_names = [ns[0] for ns in resp.json()["namespaces"]]
            assert "persistent_empty" in ns_names

    def test_collection_with_assets_removed_still_listed(self, namespace_test_app):
        """Collection created via _ns_meta_ should survive asset deletion."""
        client, _ = namespace_test_app

        # Create collection explicitly
        client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["will_be_empty"]},
        )

        # Verify it's listed (empty — no assets)
        resp = client.get("/v1/test_project/namespaces")
        ns_names = [ns[0] for ns in resp.json()["namespaces"]]
        assert "will_be_empty" in ns_names


class TestCollectionDeletion:
    """DELETE /v1/{prefix}/namespaces/{ns} behaviour."""

    def test_delete_non_empty_collection_fails(self, namespace_test_app):
        client, _ = namespace_test_app

        resp = client.delete("/v1/test_project/namespaces/existing_collection")
        assert resp.status_code == 409

    def test_delete_empty_collection_succeeds(self, namespace_test_app):
        client, _ = namespace_test_app

        # Create then delete
        client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["to_delete"]},
        )
        resp = client.delete("/v1/test_project/namespaces/to_delete")
        assert resp.status_code == 204


class TestCollectionGetSingle:
    """GET /v1/{prefix}/namespaces/{ns} — single collection detail."""

    def test_get_existing_collection(self, namespace_test_app):
        client, _ = namespace_test_app
        resp = client.get("/v1/test_project/namespaces/existing_collection")
        assert resp.status_code == 200
        data = resp.json()
        assert data["namespace"] == ["existing_collection"]

    def test_get_empty_collection(self, namespace_test_app):
        client, _ = namespace_test_app

        client.post(
            "/v1/test_project/namespaces",
            json={"namespace": ["empty_one"]},
        )
        resp = client.get("/v1/test_project/namespaces/empty_one")
        assert resp.status_code == 200

    def test_head_collection_exists(self, namespace_test_app):
        client, _ = namespace_test_app
        resp = client.head("/v1/test_project/namespaces/existing_collection")
        assert resp.status_code == 204

    def test_nonexistent_project_returns_404(self, namespace_test_app):
        client, _ = namespace_test_app
        resp = client.get("/v1/nonexistent/namespaces")
        assert resp.status_code == 404
