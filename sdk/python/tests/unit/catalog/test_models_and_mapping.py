"""Unit tests for catalog Pydantic models and mapping functions."""

import uuid
from datetime import datetime, timezone

import pytest

from feast.api.catalog.mapping import (
    CATALOG_MANAGED_TAG,
    _columns_to_iceberg_schema,
    _make_uuid,
    _timestamp_ms,
    is_catalog_managed,
    namespace_properties_to_project_kwargs,
    project_to_namespace_response,
    saved_dataset_to_load_table_response,
    saved_dataset_to_volume_info,
)
from feast.api.catalog.models import (
    CatalogConfig,
    CreateTableRequest,
    CreateVolumeRequest,
    IcebergField,
    IcebergSchema,
    LoadTableResponse,
    NamespaceResponse,
    SearchResponse,
    SearchResult,
    TableMetadata,
    TableUpdate,
    UpdateTableRequest,
    VolumeInfo,
)


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class MockSavedDataset:
    def __init__(self, name, tags=None, namespace="default", columns=None, data_source_ref=""):
        self.name = name
        self.tags = tags or {}
        self.namespace = namespace
        self.columns = columns or []
        self.data_source_ref = data_source_ref
        self.created_timestamp = None
        self.last_updated_timestamp = None


class MockProject:
    def __init__(self, name, tags=None, description="", owner=""):
        self.name = name
        self.tags = tags or {}
        self.description = description
        self.owner = owner
        self.created_timestamp = None
        self.last_updated_timestamp = None


# ===========================================================================
# Model tests
# ===========================================================================


class TestCatalogConfig:
    def test_defaults(self):
        cfg = CatalogConfig()
        assert cfg.defaults == {}
        assert cfg.overrides == {}
        assert cfg.endpoints is None

    def test_roundtrip(self):
        data = {"defaults": {"k": "v"}, "overrides": {"a": "b"}, "endpoints": ["http://x"]}
        cfg = CatalogConfig.model_validate(data)
        assert cfg.defaults == {"k": "v"}
        assert cfg.overrides == {"a": "b"}
        assert cfg.endpoints == ["http://x"]
        dumped = cfg.model_dump(by_alias=True)
        assert dumped == data


class TestCreateTableRequest:
    def test_schema_alias_roundtrip(self):
        data = {
            "name": "my_table",
            "schema": {"type": "struct", "schema-id": 1, "fields": []},
        }
        req = CreateTableRequest.model_validate(data)
        assert req.name == "my_table"
        assert req.schema_.type == "struct"
        dumped = req.model_dump(by_alias=True)
        assert "schema" in dumped
        assert "schema_" not in dumped

    def test_preserves_optional_fields(self):
        data = {
            "name": "t",
            "schema": {"type": "struct", "fields": []},
            "location": "/data/t",
            "properties": {"owner": "me"},
            "partition-spec": {"spec-id": 0, "fields": []},
            "sort-order": {"order-id": 0, "fields": []},
        }
        req = CreateTableRequest.model_validate(data)
        assert req.location == "/data/t"
        assert req.properties == {"owner": "me"}
        assert req.partition_spec is not None
        assert req.sort_order is not None


class TestLoadTableResponse:
    def test_metadata_location_alias(self):
        data = {
            "metadata-location": "s3://bucket/meta",
            "metadata": {
                "format-version": 2,
                "table-uuid": "abc-123",
                "location": "s3://bucket/data",
                "last-updated-ms": 1000,
                "properties": {},
                "schemas": [],
            },
            "config": {},
        }
        resp = LoadTableResponse.model_validate(data)
        assert resp.metadata_location == "s3://bucket/meta"
        dumped = resp.model_dump(by_alias=True)
        assert "metadata-location" in dumped
        assert "metadata_location" not in dumped

    def test_config_defaults_empty(self):
        data = {
            "metadata-location": "loc",
            "metadata": {
                "format-version": 2,
                "table-uuid": "u",
                "location": "l",
                "last-updated-ms": 0,
                "properties": {},
                "schemas": [],
            },
        }
        resp = LoadTableResponse.model_validate(data)
        assert resp.config == {}


class TestTableMetadata:
    def test_aliases_roundtrip(self):
        data = {
            "format-version": 2,
            "table-uuid": "id-here",
            "location": "/loc",
            "last-updated-ms": 9999,
            "properties": {"k": "v"},
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "last-column-id": 5,
            "current-snapshot-id": -1,
        }
        meta = TableMetadata.model_validate(data)
        assert meta.format_version == 2
        assert meta.table_uuid == "id-here"
        assert meta.last_updated_ms == 9999
        assert meta.last_column_id == 5
        dumped = meta.model_dump(by_alias=True)
        assert "format-version" in dumped
        assert "table-uuid" in dumped
        assert "last-updated-ms" in dumped
        assert "current-schema-id" in dumped
        assert "last-column-id" in dumped

    def test_defaults(self):
        data = {
            "table-uuid": "u",
            "location": "l",
            "last-updated-ms": 0,
            "properties": {},
            "schemas": [],
        }
        meta = TableMetadata.model_validate(data)
        assert meta.format_version == 2
        assert meta.current_schema_id == 0
        assert meta.default_spec_id == 0
        assert meta.default_sort_order_id == 0
        assert meta.current_snapshot_id == -1
        assert meta.snapshots == []


class TestUpdateTableRequest:
    def test_with_updates(self):
        data = {
            "updates": [
                {"action": "set-properties", "updates": {"owner": "alice"}},
                {"action": "remove-properties", "removals": ["old_key"]},
            ]
        }
        req = UpdateTableRequest.model_validate(data)
        assert len(req.updates) == 2
        assert req.updates[0].action == "set-properties"
        assert req.updates[0].updates == {"owner": "alice"}
        assert req.updates[1].removals == ["old_key"]
        assert req.requirements is None

    def test_with_requirements(self):
        data = {
            "requirements": [{"type": "assert-table-uuid", "uuid": "abc"}],
            "updates": [{"action": "set-properties", "updates": {"a": "b"}}],
        }
        req = UpdateTableRequest.model_validate(data)
        assert req.requirements is not None
        assert len(req.requirements) == 1
        assert req.requirements[0].type == "assert-table-uuid"


class TestTableUpdate:
    def test_set_properties(self):
        tu = TableUpdate(action="set-properties", updates={"key": "val"})
        assert tu.action == "set-properties"
        assert tu.updates == {"key": "val"}
        assert tu.removals is None

    def test_remove_properties(self):
        tu = TableUpdate(action="remove-properties", removals=["a", "b"])
        assert tu.removals == ["a", "b"]
        assert tu.updates is None

    def test_schema_alias(self):
        data = {
            "action": "add-schema",
            "schema": {"type": "struct", "schema-id": 1, "fields": []},
        }
        tu = TableUpdate.model_validate(data)
        assert tu.schema_ is not None
        assert tu.schema_.schema_id == 1
        dumped = tu.model_dump(by_alias=True)
        assert "schema" in dumped
        assert "schema_" not in dumped


class TestVolumeInfo:
    def test_aliases(self):
        data = {
            "name": "vol1",
            "catalog-name": "feast_cat",
            "schema-name": "ns",
            "volume-type": "EXTERNAL",
            "storage-location": "s3://bucket/vol1",
        }
        vol = VolumeInfo.model_validate(data)
        assert vol.catalog_name == "feast_cat"
        assert vol.schema_name == "ns"
        assert vol.volume_type == "EXTERNAL"
        assert vol.storage_location == "s3://bucket/vol1"
        dumped = vol.model_dump(by_alias=True)
        assert "catalog-name" in dumped
        assert "schema-name" in dumped
        assert "volume-type" in dumped
        assert "storage-location" in dumped

    def test_optional_fields_default(self):
        data = {
            "name": "v",
            "catalog-name": "c",
            "schema-name": "s",
            "volume-type": "MANAGED",
            "storage-location": "loc",
        }
        vol = VolumeInfo.model_validate(data)
        assert vol.comment is None
        assert vol.owner is None
        assert vol.created_at is None
        assert vol.updated_at is None
        assert vol.properties == {}
        assert vol.config == {}


class TestCreateVolumeRequest:
    def test_aliases(self):
        data = {
            "name": "my_vol",
            "volume-type": "EXTERNAL",
            "storage-location": "/data/vol",
        }
        req = CreateVolumeRequest.model_validate(data)
        assert req.volume_type == "EXTERNAL"
        assert req.storage_location == "/data/vol"
        dumped = req.model_dump(by_alias=True)
        assert "volume-type" in dumped
        assert "storage-location" in dumped

    def test_optional_fields(self):
        req = CreateVolumeRequest(
            name="v",
            volume_type="EXTERNAL",
            storage_location="/loc",
            comment="test vol",
            data_source_ref="ds_ref",
            properties={"tag": "true"},
        )
        assert req.comment == "test vol"
        assert req.data_source_ref == "ds_ref"
        assert req.properties == {"tag": "true"}


class TestSearchResult:
    def test_minimal(self):
        sr = SearchResult(type="table", namespace=["ns"], name="t1")
        assert sr.type == "table"
        assert sr.namespace == ["ns"]
        assert sr.description is None
        assert sr.properties == {}
        assert sr.score == 0

    def test_full(self):
        data = {
            "type": "volume",
            "namespace": ["a", "b"],
            "name": "vol",
            "description": "A volume",
            "properties": {"k": "v"},
            "score": 42,
        }
        sr = SearchResult.model_validate(data)
        assert sr.score == 42
        assert sr.description == "A volume"


class TestSearchResponse:
    def test_defaults(self):
        data = {"query": "test", "results": []}
        resp = SearchResponse.model_validate(data)
        assert resp.query == "test"
        assert resp.results == []
        assert resp.total == 0
        assert resp.page == 1
        assert resp.limit == 50

    def test_with_results(self):
        data = {
            "query": "q",
            "results": [
                {"type": "table", "namespace": ["ns"], "name": "t1"},
                {"type": "volume", "namespace": ["ns"], "name": "v1"},
            ],
            "total": 2,
            "page": 1,
            "limit": 10,
        }
        resp = SearchResponse.model_validate(data)
        assert len(resp.results) == 2
        assert resp.total == 2
        assert resp.results[0].type == "table"
        assert resp.results[1].type == "volume"


class TestIcebergSchema:
    def test_schema_id_alias(self):
        data = {"type": "struct", "schema-id": 5, "fields": []}
        schema = IcebergSchema.model_validate(data)
        assert schema.schema_id == 5
        dumped = schema.model_dump(by_alias=True)
        assert "schema-id" in dumped
        assert dumped["schema-id"] == 5

    def test_defaults(self):
        schema = IcebergSchema()
        assert schema.type == "struct"
        assert schema.schema_id == 0
        assert schema.fields == []

    def test_with_fields(self):
        data = {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "col_a", "required": True, "type": "long"},
                {"id": 2, "name": "col_b", "required": False, "type": "string"},
            ],
        }
        schema = IcebergSchema.model_validate(data)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "col_a"
        assert schema.fields[0].required is True
        assert schema.fields[1].type == "string"


class TestNamespaceResponse:
    def test_basic(self):
        data = {"namespace": ["project_x"], "properties": {"owner": "alice"}}
        resp = NamespaceResponse.model_validate(data)
        assert resp.namespace == ["project_x"]
        assert resp.properties["owner"] == "alice"

    def test_roundtrip(self):
        resp = NamespaceResponse(namespace=["a", "b"], properties={"k": "v"})
        dumped = resp.model_dump(by_alias=True)
        restored = NamespaceResponse.model_validate(dumped)
        assert restored.namespace == ["a", "b"]
        assert restored.properties == {"k": "v"}


# ===========================================================================
# Mapping function tests
# ===========================================================================


class TestMakeUuid:
    def test_deterministic(self):
        u1 = _make_uuid("ns", "table1")
        u2 = _make_uuid("ns", "table1")
        assert u1 == u2

    def test_different_inputs(self):
        u1 = _make_uuid("ns", "table1")
        u2 = _make_uuid("ns", "table2")
        assert u1 != u2

    def test_valid_uuid(self):
        result = _make_uuid("test", "name")
        parsed = uuid.UUID(result)
        assert parsed.version == 5


class TestTimestampMs:
    def test_none_returns_zero(self):
        assert _timestamp_ms(None) == 0

    def test_epoch(self):
        dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert _timestamp_ms(dt) == 0

    def test_known_timestamp(self):
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        expected = int(dt.timestamp() * 1000)
        assert _timestamp_ms(dt) == expected


class TestIsCatalogManaged:
    def test_managed(self):
        ds = MockSavedDataset("d", tags={CATALOG_MANAGED_TAG: "true"})
        assert is_catalog_managed(ds) is True

    def test_not_managed_missing_tag(self):
        ds = MockSavedDataset("d", tags={})
        assert is_catalog_managed(ds) is False

    def test_not_managed_false_value(self):
        ds = MockSavedDataset("d", tags={CATALOG_MANAGED_TAG: "false"})
        assert is_catalog_managed(ds) is False


class TestColumnsToIcebergSchema:
    def test_empty_columns(self):
        schema = _columns_to_iceberg_schema([])
        assert schema.type == "struct"
        assert schema.fields == []
        assert schema.schema_id == 0

    def test_single_column(self):
        cols = [{"name": "id", "type": "long", "nullable": False}]
        schema = _columns_to_iceberg_schema(cols)
        assert len(schema.fields) == 1
        f = schema.fields[0]
        assert f.id == 1
        assert f.name == "id"
        assert f.required is True
        assert f.type == "long"

    def test_multiple_columns(self):
        cols = [
            {"name": "a", "type": "string"},
            {"name": "b", "type": "int", "nullable": False},
        ]
        schema = _columns_to_iceberg_schema(cols, schema_id=3)
        assert schema.schema_id == 3
        assert len(schema.fields) == 2
        assert schema.fields[0].id == 1
        assert schema.fields[0].required is False  # nullable defaults True
        assert schema.fields[1].id == 2
        assert schema.fields[1].required is True

    def test_missing_keys_use_defaults(self):
        cols = [{}]
        schema = _columns_to_iceberg_schema(cols)
        f = schema.fields[0]
        assert f.name == ""
        assert f.type == "string"
        assert f.required is False


class TestSavedDatasetToLoadTableResponse:
    def test_basic_conversion(self):
        ds = MockSavedDataset(
            "my_table",
            tags={"owner": "alice"},
            columns=[{"name": "x", "type": "long"}],
        )
        resp = saved_dataset_to_load_table_response(ds, "proj")
        assert resp.metadata_location == "feast://proj/tables/my_table/metadata"
        assert resp.metadata.location == "feast://proj/tables/my_table"
        assert resp.metadata.format_version == 2
        assert resp.metadata.properties == {"owner": "alice"}
        assert len(resp.metadata.schemas) == 1
        assert len(resp.metadata.schemas[0].fields) == 1

    def test_filters_internal_tags(self):
        ds = MockSavedDataset(
            "t",
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": "table", "location": "/x", "keep": "yes"},
        )
        resp = saved_dataset_to_load_table_response(ds, "ns")
        assert CATALOG_MANAGED_TAG not in resp.metadata.properties
        assert "asset_type" not in resp.metadata.properties
        assert "location" not in resp.metadata.properties
        assert resp.metadata.properties["keep"] == "yes"

    def test_custom_location(self):
        ds = MockSavedDataset("t", tags={"location": "s3://custom/path"})
        resp = saved_dataset_to_load_table_response(ds, "ns")
        assert resp.metadata.location == "s3://custom/path"

    def test_uuid_deterministic(self):
        ds = MockSavedDataset("t")
        r1 = saved_dataset_to_load_table_response(ds, "ns")
        r2 = saved_dataset_to_load_table_response(ds, "ns")
        assert r1.metadata.table_uuid == r2.metadata.table_uuid

    def test_serializes_with_aliases(self):
        ds = MockSavedDataset("t", columns=[{"name": "c", "type": "string"}])
        resp = saved_dataset_to_load_table_response(ds, "ns")
        dumped = resp.model_dump(by_alias=True)
        assert "metadata-location" in dumped
        assert "format-version" in dumped["metadata"]
        assert "table-uuid" in dumped["metadata"]
        assert "last-updated-ms" in dumped["metadata"]


class TestSavedDatasetToVolumeInfo:
    def test_basic_conversion(self):
        ds = MockSavedDataset("vol1", namespace="schema_ns")
        vol = saved_dataset_to_volume_info(ds, "catalog")
        assert vol.name == "vol1"
        assert vol.catalog_name == "catalog"
        assert vol.schema_name == "schema_ns"
        assert vol.volume_type == "EXTERNAL"
        assert vol.storage_location == "feast://catalog/volumes/vol1"

    def test_custom_volume_type(self):
        ds = MockSavedDataset("v", tags={"volume_type": "MANAGED"})
        vol = saved_dataset_to_volume_info(ds, "ns")
        assert vol.volume_type == "MANAGED"

    def test_filters_internal_tags(self):
        ds = MockSavedDataset(
            "v",
            tags={CATALOG_MANAGED_TAG: "true", "asset_type": "volume", "volume_type": "EXT", "comment": "c", "user": "x"},
        )
        vol = saved_dataset_to_volume_info(ds, "ns")
        assert CATALOG_MANAGED_TAG not in vol.properties
        assert "asset_type" not in vol.properties
        assert "volume_type" not in vol.properties
        assert "comment" not in vol.properties
        assert vol.properties["user"] == "x"

    def test_comment_passthrough(self):
        ds = MockSavedDataset("v", tags={"comment": "my comment"})
        vol = saved_dataset_to_volume_info(ds, "ns")
        assert vol.comment == "my comment"

    def test_serializes_with_aliases(self):
        ds = MockSavedDataset("v")
        vol = saved_dataset_to_volume_info(ds, "ns")
        dumped = vol.model_dump(by_alias=True)
        assert "catalog-name" in dumped
        assert "schema-name" in dumped
        assert "volume-type" in dumped
        assert "storage-location" in dumped

    def test_timestamps_from_dataset(self):
        ds = MockSavedDataset("v")
        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        ds.created_timestamp = dt
        ds.last_updated_timestamp = dt
        vol = saved_dataset_to_volume_info(ds, "ns")
        expected_ms = int(dt.timestamp() * 1000)
        assert vol.created_at == expected_ms
        assert vol.updated_at == expected_ms


class TestNamespacePropertiesToProjectKwargs:
    def test_extracts_description_and_owner(self):
        props = {"description": "My project", "owner": "alice", "env": "prod"}
        result = namespace_properties_to_project_kwargs(["my_proj"], props)
        assert result["name"] == "my_proj"
        assert result["description"] == "My project"
        assert result["owner"] == "alice"
        assert result["tags"] == {"env": "prod"}

    def test_missing_description_and_owner(self):
        result = namespace_properties_to_project_kwargs(["p"], {"k": "v"})
        assert result["description"] == ""
        assert result["owner"] == ""
        assert result["tags"] == {"k": "v"}

    def test_multi_segment_namespace(self):
        result = namespace_properties_to_project_kwargs(["a", "b", "c"], {})
        assert result["name"] == "a.b.c"

    def test_empty_properties(self):
        result = namespace_properties_to_project_kwargs(["p"], {})
        assert result["tags"] == {}


class TestProjectToNamespaceResponse:
    def test_basic(self):
        proj = MockProject("proj1", description="desc", owner="bob")
        resp = project_to_namespace_response(proj)
        assert resp.namespace == ["proj1"]
        assert resp.properties["description"] == "desc"
        assert resp.properties["owner"] == "bob"

    def test_with_collection(self):
        proj = MockProject("proj1")
        resp = project_to_namespace_response(proj, collection="my_collection")
        assert resp.namespace == ["my_collection"]

    def test_tags_included(self):
        proj = MockProject("p", tags={"env": "staging", "team": "ml"})
        resp = project_to_namespace_response(proj)
        assert resp.properties["env"] == "staging"
        assert resp.properties["team"] == "ml"

    def test_internal_tags_filtered(self):
        proj = MockProject("p", tags={"_ns_meta_x": "hidden", "visible": "yes"})
        resp = project_to_namespace_response(proj)
        assert "_ns_meta_x" not in resp.properties
        assert resp.properties["visible"] == "yes"

    def test_timestamps_included(self):
        proj = MockProject("p")
        dt = datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        proj.created_timestamp = dt
        proj.last_updated_timestamp = dt
        resp = project_to_namespace_response(proj)
        expected = str(int(dt.timestamp() * 1000))
        assert resp.properties["created_at"] == expected
        assert resp.properties["updated_at"] == expected

    def test_no_timestamps_when_none(self):
        proj = MockProject("p")
        resp = project_to_namespace_response(proj)
        assert "created_at" not in resp.properties
        assert "updated_at" not in resp.properties
