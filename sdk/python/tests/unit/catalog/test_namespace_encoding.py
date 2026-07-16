"""Tests for namespace encoding/decoding (%1F separator support)."""

import pytest

from feast.api.catalog.namespaces import NAMESPACE_SEPARATOR, decode_namespace


class TestDecodeNamespace:
    def test_simple_namespace(self):
        assert decode_namespace("default") == ["default"]

    def test_namespace_with_separator(self):
        result = decode_namespace(f"team-a{NAMESPACE_SEPARATOR}production")
        assert result == ["team-a", "production"]

    def test_multiple_separators(self):
        result = decode_namespace(
            f"org{NAMESPACE_SEPARATOR}team{NAMESPACE_SEPARATOR}env"
        )
        assert result == ["org", "team", "env"]

    def test_empty_string(self):
        assert decode_namespace("") == [""]

    def test_separator_at_start(self):
        result = decode_namespace(f"{NAMESPACE_SEPARATOR}trailing")
        assert result == ["", "trailing"]

    def test_separator_at_end(self):
        result = decode_namespace(f"leading{NAMESPACE_SEPARATOR}")
        assert result == ["leading", ""]

    def test_hyphenated_namespace(self):
        assert decode_namespace("my-project-ns") == ["my-project-ns"]

    def test_underscored_namespace(self):
        assert decode_namespace("my_project_ns") == ["my_project_ns"]

    def test_separator_is_0x1f(self):
        assert NAMESPACE_SEPARATOR == "\x1f"
        assert ord(NAMESPACE_SEPARATOR) == 0x1F
