"""Integration tests for the Catalog API using FastAPI TestClient.

Tests the full request → route → handler → response chain with a mock
Feast registry. No real K8s cluster or S3 needed.

Covers: CRUD lifecycle, spec compliance, error handling, UpdateTable actions,
namespace operations, search, and credential resolution.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ["DATACATALOG_SSAR_ENABLED"] = "false"

from feast.api.catalog import add_catalog_routes
from feast.api.catalog.mapping import CATALOG_MANAGED_TAG


class MockSavedDataset:
    def __init__(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        namespace: str = "default",
        columns: Optional[List[Dict[str, Any]]] = None,
        data_source_ref: str = "",
    ):
        self.name = name
        self.tags = tags or {}
        self.namespace = namespace
        self.columns = columns or []
        self.data_source_ref = data_source_ref
        self.created_timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.last_updated_timestamp = datetime(2024, 6, 1, tzinfo=timezone.utc)


class MockProject:
    def __init__(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ):
        self.name = name
        self.tags = tags or {}
        self.description = description
        self.owner = owner
        self.created_timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.last_updated_timestamp = datetime(2024, 6, 1, tzinfo=timezone.utc)


class FeastObjectNotFoundException(Exception):
    pass


class MockRegistry:
    """In-memory mock of the Feast registry for testing."""

    def __init__(self):
        self._projects: Dict[str, MockProject] = {}
        self._datasets: Dict[str, List[MockSavedDataset]] = {}

    def add_project(self, project: MockProject):
        self._projects[project.name] = project
        if project.name not in self._datasets:
            self._datasets[project.name] = []

    def get_project(self, name, allow_cache=True):
        if name not in self._projects:
            raise FeastObjectNotFoundException(f"Project not found: {name}")
        return self._projects[name]

    def list_projects(self, allow_cache=True):
        return list(self._projects.values())

    def apply_project(self, project, commit=True):
        self._projects[project.name] = project
        if project.name not in self._datasets:
            self._datasets[project.name] = []

    def delete_project(self, name, commit=True):
        self._projects.pop(name, None)
        self._datasets.pop(name, None)

    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = True,
        tags: Optional[Dict[str, str]] = None,
        namespace: Optional[str] = None,
    ) -> List[MockSavedDataset]:
        datasets = self._datasets.get(project, [])
        results = []
        for ds in datasets:
            if tags:
                match = all(ds.tags.get(k) == v for k, v in tags.items())
                if not match:
                    continue
            if namespace is not None and ds.namespace != namespace:
                continue
            results.append(ds)
        return results

    def get_saved_dataset(
        self,
        name: str,
        project: str,
        allow_cache: bool = True,
        namespace: Optional[str] = None,
    ):
        datasets = self._datasets.get(project, [])
        for ds in datasets:
            if ds.name == name:
                if namespace is not None and ds.namespace != namespace:
                    continue
                return ds
        raise FeastObjectNotFoundException(f"SavedDataset not found: {name}")

    def apply_saved_dataset(self, ds, project: str, commit: bool = True):
        if project not in self._datasets:
            self._datasets[project] = []
        existing = [
            i
            for i, d in enumerate(self._datasets[project])
            if d.name == ds.name and d.namespace == ds.namespace
        ]
        if existing:
            self._datasets[project][existing[0]] = ds
        else:
            self._datasets[project].append(ds)

    def delete_saved_dataset(
        self,
        name: str,
        project: str,
        commit: bool = True,
        namespace: Optional[str] = None,
    ):
        if project in self._datasets:
            self._datasets[project] = [
                ds
                for ds in self._datasets[project]
                if not (
                    ds.name == name
                    and (namespace is None or ds.namespace == namespace)
                )
            ]


@pytest.fixture
def mock_registry():
    registry = MockRegistry()
    registry.add_project(MockProject("underwriting", description="Underwriting team"))
    registry.add_project(MockProject("analytics", description="Analytics team"))
    return registry


@pytest.fixture
def mock_store(mock_registry):
    store = MagicMock()
    store.registry = mock_registry
    return store


@pytest.fixture
def client(mock_store):
    app = FastAPI()
    with patch("feast.api.catalog.credentials.create_vender_from_env", return_value=None), \
         patch("feast.api.catalog.metadata_reader.create_reader_from_env", return_value=None), \
         patch("feast.errors.FeastObjectNotFoundException", FeastObjectNotFoundException):
        import feast.errors
        feast.errors.FeastObjectNotFoundException = FeastObjectNotFoundException
        add_catalog_routes(app, mock_store)
    return TestClient(app)


@pytest.fixture
def seeded_client(mock_store, mock_registry):
    table = MockSavedDataset(
        name="events",
        tags={
            CATALOG_MANAGED_TAG: "true",
            "asset_type": "table",
            "location": "s3://bucket/events/",
            "format": "parquet",
            "domain": "analytics",
            "description": "User event stream",
        },
        namespace="default",
        columns=[
            {"name": "user_id", "type": "INT64", "nullable": False},
            {"name": "event_type", "type": "STRING", "nullable": True},
        ],
    )
    mock_registry._datasets["underwriting"].append(table)

    vol = MockSavedDataset(
        name="raw_pdfs",
        tags={
            CATALOG_MANAGED_TAG: "true",
            "asset_type": "volume",
            "volume_type": "EXTERNAL",
            "location": "s3://bucket/pdfs/",
            "description": "Raw PDF documents",
        },
        namespace="default",
    )
    mock_registry._datasets["underwriting"].append(vol)

    app = FastAPI()
    with patch("feast.api.catalog.credentials.create_vender_from_env", return_value=None), \
         patch("feast.api.catalog.metadata_reader.create_reader_from_env", return_value=None), \
         patch("feast.errors.FeastObjectNotFoundException", FeastObjectNotFoundException):
        import feast.errors
        feast.errors.FeastObjectNotFoundException = FeastObjectNotFoundException
        add_catalog_routes(app, mock_store)
    return TestClient(app)


class TestConfigEndpoint:
    def test_config_returns_endpoints(self, client):
        resp = client.get("/v1/underwriting/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "endpoints" in data
        assert len(data["endpoints"]) > 0
        assert "overrides" in data
        assert data["overrides"]["prefix"] == "underwriting"

    def test_config_different_prefix(self, client):
        resp = client.get("/v1/analytics/config")
        assert resp.status_code == 200
        assert resp.json()["overrides"]["prefix"] == "analytics"


class TestNamespaceEndpoints:
    def test_list_namespaces(self, client):
        resp = client.get("/v1/underwriting/namespaces")
        assert resp.status_code == 200
        data = resp.json()
        assert "namespaces" in data
        assert ["default"] in data["namespaces"]

    def test_get_namespace(self, client):
        resp = client.get("/v1/underwriting/namespaces/default")
        assert resp.status_code == 200
        data = resp.json()
        assert data["namespace"] == ["default"]
        assert "properties" in data

    def test_namespace_exists(self, client):
        resp = client.head("/v1/underwriting/namespaces/default")
        assert resp.status_code == 204

    def test_get_namespace_nonexistent_project(self, client):
        resp = client.get("/v1/nonexistent/namespaces/default")
        assert resp.status_code == 404

    def test_create_namespace(self, client):
        resp = client.post(
            "/v1/underwriting/namespaces",
            json={"namespace": ["production"], "properties": {"tier": "gold"}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["namespace"] == ["production"]

    def test_delete_empty_namespace(self, client):
        resp = client.delete("/v1/underwriting/namespaces/empty_ns")
        assert resp.status_code == 204

    def test_delete_nonempty_namespace(self, seeded_client):
        resp = seeded_client.delete("/v1/underwriting/namespaces/default")
        assert resp.status_code == 409

    def test_update_namespace_properties(self, client):
        resp = client.post(
            "/v1/underwriting/namespaces/default/properties",
            json={"updates": {"tier": "gold"}, "removals": []},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "tier" in data["updated"]


class TestTableEndpoints:
    def test_create_table(self, client):
        resp = client.post(
            "/v1/underwriting/namespaces/default/tables",
            json={
                "name": "test_table",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "col1", "required": True, "type": "string"},
                    ],
                },
                "location": "s3://bucket/test_table/",
                "properties": {"format": "parquet", "domain": "test"},
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "metadata" in data
        assert "metadata-location" in data

    def test_list_tables(self, seeded_client):
        resp = seeded_client.get("/v1/underwriting/namespaces/default/tables")
        assert resp.status_code == 200
        data = resp.json()
        assert "identifiers" in data
        names = [t["name"] for t in data["identifiers"]]
        assert "events" in names

    def test_load_table(self, seeded_client):
        resp = seeded_client.get(
            "/v1/underwriting/namespaces/default/tables/events"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "metadata" in data
        assert data["metadata"]["location"] == "s3://bucket/events/"

    def test_table_exists(self, seeded_client):
        resp = seeded_client.head(
            "/v1/underwriting/namespaces/default/tables/events"
        )
        assert resp.status_code == 204

    def test_table_not_found(self, client):
        resp = client.get(
            "/v1/underwriting/namespaces/default/tables/nonexistent"
        )
        assert resp.status_code == 404
        data = resp.json()
        assert data["error"]["type"] == "NoSuchTableException"

    def test_update_table_set_properties(self, seeded_client):
        resp = seeded_client.post(
            "/v1/underwriting/namespaces/default/tables/events",
            json={
                "updates": [
                    {
                        "action": "set-properties",
                        "updates": {"tier": "gold", "pii": "true"},
                    }
                ]
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"]["properties"]["tier"] == "gold"

    def test_update_table_remove_properties(self, seeded_client):
        resp = seeded_client.post(
            "/v1/underwriting/namespaces/default/tables/events",
            json={
                "updates": [
                    {
                        "action": "remove-properties",
                        "removals": ["domain"],
                    }
                ]
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "domain" not in data["metadata"]["properties"]

    def test_update_table_unsupported_action(self, seeded_client):
        resp = seeded_client.post(
            "/v1/underwriting/namespaces/default/tables/events",
            json={
                "updates": [
                    {
                        "action": "add-schema",
                        "schema": {"type": "struct", "fields": []},
                    }
                ]
            },
        )
        assert resp.status_code == 501
        data = resp.json()
        assert data["error"]["type"] == "UnsupportedOperationException"

    def test_delete_table(self, seeded_client):
        resp = seeded_client.delete(
            "/v1/underwriting/namespaces/default/tables/events"
        )
        assert resp.status_code == 204
        resp = seeded_client.get(
            "/v1/underwriting/namespaces/default/tables/events"
        )
        assert resp.status_code == 404

    def test_create_duplicate_table(self, seeded_client):
        resp = seeded_client.post(
            "/v1/underwriting/namespaces/default/tables",
            json={
                "name": "events",
                "schema": {"type": "struct", "fields": []},
            },
        )
        assert resp.status_code == 409


class TestVolumeEndpoints:
    def test_create_volume(self, client):
        resp = client.post(
            "/v1/underwriting/namespaces/default/volumes",
            json={
                "name": "test_volume",
                "volume-type": "EXTERNAL",
                "storage-location": "s3://bucket/volumes/test/",
                "comment": "Test volume",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test_volume"

    def test_list_volumes(self, seeded_client):
        resp = seeded_client.get("/v1/underwriting/namespaces/default/volumes")
        assert resp.status_code == 200
        data = resp.json()
        assert "volumes" in data
        names = [v["name"] for v in data["volumes"]]
        assert "raw_pdfs" in names

    def test_get_volume(self, seeded_client):
        resp = seeded_client.get(
            "/v1/underwriting/namespaces/default/volumes/raw_pdfs"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "raw_pdfs"
        assert data["storage-location"] == "s3://bucket/pdfs/"

    def test_volume_exists(self, seeded_client):
        resp = seeded_client.head(
            "/v1/underwriting/namespaces/default/volumes/raw_pdfs"
        )
        assert resp.status_code == 204

    def test_volume_not_found(self, client):
        resp = client.get(
            "/v1/underwriting/namespaces/default/volumes/nonexistent"
        )
        assert resp.status_code == 404

    def test_delete_volume(self, seeded_client):
        resp = seeded_client.delete(
            "/v1/underwriting/namespaces/default/volumes/raw_pdfs"
        )
        assert resp.status_code == 204
        resp = seeded_client.get(
            "/v1/underwriting/namespaces/default/volumes/raw_pdfs"
        )
        assert resp.status_code == 404


class TestSearchEndpoint:
    def test_search_by_name(self, seeded_client):
        resp = seeded_client.get("/v1/underwriting/search?query=events")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0
        assert data["results"][0]["name"] == "events"

    def test_search_empty_query_returns_all(self, seeded_client):
        resp = seeded_client.get("/v1/underwriting/search?query=")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    def test_search_by_asset_type(self, seeded_client):
        resp = seeded_client.get(
            "/v1/underwriting/search?query=&asset_type=volume"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert all(r["type"] == "volume" for r in data["results"])

    def test_search_no_results(self, seeded_client):
        resp = seeded_client.get("/v1/underwriting/search?query=zebra")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0

    def test_search_property_filter(self, seeded_client):
        resp = seeded_client.get(
            "/v1/underwriting/search?query=&properties=format:parquet"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1

    def test_search_nonexistent_project(self, client):
        resp = client.get("/v1/nonexistent/search?query=test")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0


class TestCRUDLifecycle:
    """Full create → read → update → delete lifecycle for tables."""

    def test_table_lifecycle(self, client):
        create_resp = client.post(
            "/v1/underwriting/namespaces/default/tables",
            json={
                "name": "lifecycle_table",
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": True, "type": "long"},
                        {"id": 2, "name": "value", "required": False, "type": "string"},
                    ],
                },
                "location": "s3://bucket/lifecycle/",
                "properties": {"format": "parquet", "owner": "test-user"},
            },
        )
        assert create_resp.status_code == 200

        get_resp = client.get(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table"
        )
        assert get_resp.status_code == 200
        metadata = get_resp.json()["metadata"]
        assert metadata["location"] == "s3://bucket/lifecycle/"
        assert metadata["properties"]["owner"] == "test-user"

        head_resp = client.head(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table"
        )
        assert head_resp.status_code == 204

        update_resp = client.post(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table",
            json={
                "updates": [
                    {
                        "action": "set-properties",
                        "updates": {"tier": "gold", "pii": "false"},
                    }
                ]
            },
        )
        assert update_resp.status_code == 200
        assert update_resp.json()["metadata"]["properties"]["tier"] == "gold"

        verify_resp = client.get(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table"
        )
        assert verify_resp.json()["metadata"]["properties"]["tier"] == "gold"

        del_resp = client.delete(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table"
        )
        assert del_resp.status_code == 204

        gone_resp = client.get(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table"
        )
        assert gone_resp.status_code == 404

        double_del = client.delete(
            "/v1/underwriting/namespaces/default/tables/lifecycle_table"
        )
        assert double_del.status_code == 404


class TestErrorResponses:
    def test_404_follows_iceberg_error_model(self, client):
        resp = client.get(
            "/v1/underwriting/namespaces/default/tables/nonexistent"
        )
        data = resp.json()
        assert "error" in data
        assert "message" in data["error"]
        assert "type" in data["error"]
        assert "code" in data["error"]
        assert data["error"]["code"] == 404

    def test_409_duplicate_table(self, seeded_client):
        resp = seeded_client.post(
            "/v1/underwriting/namespaces/default/tables",
            json={
                "name": "events",
                "schema": {"type": "struct", "fields": []},
            },
        )
        data = resp.json()
        assert data["error"]["type"] == "AlreadyExistsException"
        assert data["error"]["code"] == 409

    def test_501_unsupported_update(self, seeded_client):
        resp = seeded_client.post(
            "/v1/underwriting/namespaces/default/tables/events",
            json={
                "updates": [{"action": "set-location", "updates": {}}]
            },
        )
        assert resp.status_code == 501
