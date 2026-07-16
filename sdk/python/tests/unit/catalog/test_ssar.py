"""Unit tests for SSAR middleware routing logic."""

import time

import pytest

from feast.api.catalog.ssar import SSARCache, _extract_prefix, _map_to_resource, _map_to_verb


# ---------------------------------------------------------------------------
# _extract_prefix
# ---------------------------------------------------------------------------


class TestExtractPrefix:
    def test_simple_prefix(self):
        assert _extract_prefix("/v1/my-project/namespaces") == "my-project"

    def test_prefix_only(self):
        assert _extract_prefix("/v1/alpha") == "alpha"

    def test_prefix_with_deep_path(self):
        assert _extract_prefix("/v1/ns1/namespaces/db/tables/t1") == "ns1"

    def test_no_v1_prefix(self):
        assert _extract_prefix("/api/something/else") is None

    def test_root_path(self):
        assert _extract_prefix("/") is None

    def test_v1_only(self):
        assert _extract_prefix("/v1/") is None

    def test_trailing_slash_stripped(self):
        assert _extract_prefix("/v1/proj/") == "proj"


# ---------------------------------------------------------------------------
# _map_to_resource
# ---------------------------------------------------------------------------


class TestMapToResource:
    def test_config_returns_namespaces(self):
        assert _map_to_resource("/v1/proj/config") == "namespaces"

    def test_namespaces_root(self):
        assert _map_to_resource("/v1/proj/namespaces") == "namespaces"

    def test_namespaces_item(self):
        assert _map_to_resource("/v1/proj/namespaces/db1") == "namespaces"

    def test_tables_collection(self):
        assert _map_to_resource("/v1/proj/namespaces/db1/tables") == "tables"

    def test_tables_item(self):
        assert _map_to_resource("/v1/proj/namespaces/db1/tables/t1") == "tables"

    def test_volumes_collection(self):
        assert _map_to_resource("/v1/proj/namespaces/db1/volumes") == "volumes"

    def test_volumes_item(self):
        assert _map_to_resource("/v1/proj/namespaces/db1/volumes/v1") == "volumes"

    def test_search_returns_tables(self):
        assert _map_to_resource("/v1/proj/search") == "tables"

    def test_tables_rename_returns_tables(self):
        assert _map_to_resource("/v1/proj/tables/rename") == "tables"


# ---------------------------------------------------------------------------
# _map_to_verb
# ---------------------------------------------------------------------------


class TestMapToVerb:
    # GET → list vs get
    def test_get_tables_collection_is_list(self):
        assert _map_to_verb("GET", "/v1/proj/namespaces/db/tables") == "list"

    def test_get_tables_item_is_get(self):
        assert _map_to_verb("GET", "/v1/proj/namespaces/db/tables/t1") == "get"

    def test_get_volumes_collection_is_list(self):
        assert _map_to_verb("GET", "/v1/proj/namespaces/db/volumes") == "list"

    def test_get_volumes_item_is_get(self):
        assert _map_to_verb("GET", "/v1/proj/namespaces/db/volumes/vol1") == "get"

    def test_get_namespaces_collection_is_list(self):
        assert _map_to_verb("GET", "/v1/proj/namespaces") == "list"

    def test_get_namespaces_item_is_get(self):
        assert _map_to_verb("GET", "/v1/proj/namespaces/db1") == "get"

    # HEAD → get
    def test_head_is_get(self):
        assert _map_to_verb("HEAD", "/v1/proj/namespaces/db/tables/t1") == "get"

    def test_head_on_collection_is_get(self):
        assert _map_to_verb("HEAD", "/v1/proj/namespaces/db/tables") == "get"

    # POST → create / update
    def test_post_tables_collection_is_create(self):
        assert _map_to_verb("POST", "/v1/proj/namespaces/db/tables") == "create"

    def test_post_tables_item_is_update(self):
        assert _map_to_verb("POST", "/v1/proj/namespaces/db/tables/t1") == "update"

    def test_post_tables_rename_is_create(self):
        assert _map_to_verb("POST", "/v1/proj/tables/rename") == "create"

    def test_post_properties_is_update(self):
        assert _map_to_verb("POST", "/v1/proj/namespaces/db/properties") == "update"

    def test_post_namespaces_collection_is_create(self):
        assert _map_to_verb("POST", "/v1/proj/namespaces") == "create"

    # DELETE
    def test_delete_table(self):
        assert _map_to_verb("DELETE", "/v1/proj/namespaces/db/tables/t1") == "delete"

    def test_delete_namespace(self):
        assert _map_to_verb("DELETE", "/v1/proj/namespaces/db1") == "delete"

    # Search → list
    def test_get_search_is_list(self):
        assert _map_to_verb("GET", "/v1/proj/search") == "list"

    def test_post_search_is_list(self):
        assert _map_to_verb("POST", "/v1/proj/search") == "list"

    # PUT/PATCH
    def test_put_is_update(self):
        assert _map_to_verb("PUT", "/v1/proj/namespaces/db/tables/t1") == "update"

    def test_patch_is_update(self):
        assert _map_to_verb("PATCH", "/v1/proj/namespaces/db/tables/t1") == "update"


# ---------------------------------------------------------------------------
# SSARCache
# ---------------------------------------------------------------------------


class TestSSARCache:
    def test_get_missing_key_returns_none(self):
        cache = SSARCache(ttl=30, max_size=100)
        assert cache.get(("tok", "ns", "tables", "get")) is None

    def test_put_and_get(self):
        cache = SSARCache(ttl=30, max_size=100)
        key = ("tok", "ns", "tables", "get")
        cache.put(key, True)
        assert cache.get(key) is True

    def test_put_false_and_get(self):
        cache = SSARCache(ttl=30, max_size=100)
        key = ("tok", "ns", "tables", "create")
        cache.put(key, False)
        assert cache.get(key) is False

    def test_expired_entry_returns_none(self):
        cache = SSARCache(ttl=1, max_size=100)
        key = ("tok", "ns", "tables", "list")
        cache.put(key, True)
        time.sleep(1.1)
        assert cache.get(key) is None

    def test_max_size_eviction(self):
        cache = SSARCache(ttl=30, max_size=3)
        cache.put(("a",), True)
        cache.put(("b",), True)
        cache.put(("c",), True)
        cache.put(("d",), True)
        assert cache.get(("d",)) is True
        assert len(cache._cache) <= 3

    def test_expired_entries_cleaned_on_overflow(self):
        cache = SSARCache(ttl=1, max_size=2)
        cache.put(("old1",), True)
        cache.put(("old2",), False)
        time.sleep(1.1)
        cache.put(("new1",), True)
        assert cache.get(("old1",)) is None
        assert cache.get(("old2",)) is None
        assert cache.get(("new1",)) is True

    def test_overwrite_existing_key(self):
        cache = SSARCache(ttl=30, max_size=100)
        key = ("tok", "ns", "tables", "get")
        cache.put(key, True)
        cache.put(key, False)
        assert cache.get(key) is False
