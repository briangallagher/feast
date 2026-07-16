"""Tests for the connection-ref credential resolution module."""

import base64
from unittest.mock import MagicMock, patch

import pytest

from feast.api.catalog.connections import (
    MANAGED_LABEL,
    SECRET_FIELD_TO_ICEBERG_CONFIG,
    resolve_connection,
    resolve_credentials,
)


def _make_mock_secret(
    name: str,
    namespace: str,
    data: dict,
    labels: dict = None,
):
    secret = MagicMock()
    secret.metadata.name = name
    secret.metadata.namespace = namespace
    secret.metadata.labels = labels or {MANAGED_LABEL: "true"}
    secret.data = {
        k: base64.b64encode(v.encode("utf-8")).decode("utf-8")
        for k, v in data.items()
    }
    return secret


class MockSavedDataset:
    def __init__(self, name, tags=None):
        self.name = name
        self.tags = tags or {}


class TestResolveConnection:
    @patch("feast.api.catalog.connections._get_k8s_client")
    def test_resolves_full_secret(self, mock_k8s):
        secret = _make_mock_secret(
            "dc-minio",
            "underwriting",
            {
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minio123",
                "AWS_S3_ENDPOINT": "http://minio:9000",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        )
        mock_k8s.return_value.read_namespaced_secret.return_value = secret

        result = resolve_connection("dc-minio", "underwriting")
        assert result is not None
        assert result["s3.access-key-id"] == "minioadmin"
        assert result["s3.secret-access-key"] == "minio123"
        assert result["s3.endpoint"] == "http://minio:9000"
        assert result["client.region"] == "us-east-1"
        assert result["s3.path-style-access"] == "true"

    @patch("feast.api.catalog.connections._get_k8s_client")
    def test_aws_endpoint_no_path_style(self, mock_k8s):
        secret = _make_mock_secret(
            "dc-aws",
            "analytics",
            {
                "AWS_ACCESS_KEY_ID": "AKIAXXXX",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_S3_ENDPOINT": "https://s3.amazonaws.com",
            },
        )
        mock_k8s.return_value.read_namespaced_secret.return_value = secret

        result = resolve_connection("dc-aws", "analytics")
        assert result is not None
        assert "s3.path-style-access" not in result

    @patch("feast.api.catalog.connections._get_k8s_client")
    def test_secret_not_found_returns_none(self, mock_k8s):
        from kubernetes import client as k8s_client

        exc = k8s_client.ApiException(status=404)
        mock_k8s.return_value.read_namespaced_secret.side_effect = exc

        result = resolve_connection("nonexistent", "underwriting")
        assert result is None

    @patch("feast.api.catalog.connections._get_k8s_client")
    def test_unmanaged_secret_refused(self, mock_k8s):
        secret = _make_mock_secret(
            "dc-unmanaged",
            "underwriting",
            {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"},
            labels={},
        )
        mock_k8s.return_value.read_namespaced_secret.return_value = secret

        result = resolve_connection("dc-unmanaged", "underwriting")
        assert result is None

    @patch("feast.api.catalog.connections._get_k8s_client")
    def test_partial_fields(self, mock_k8s):
        secret = _make_mock_secret(
            "dc-partial",
            "underwriting",
            {"AWS_ACCESS_KEY_ID": "key"},
        )
        mock_k8s.return_value.read_namespaced_secret.return_value = secret

        result = resolve_connection("dc-partial", "underwriting")
        assert result is not None
        assert result["s3.access-key-id"] == "key"
        assert "s3.secret-access-key" not in result

    @patch("feast.api.catalog.connections._get_k8s_client")
    def test_empty_data_returns_none(self, mock_k8s):
        secret = MagicMock()
        secret.metadata.labels = {MANAGED_LABEL: "true"}
        secret.data = {}
        mock_k8s.return_value.read_namespaced_secret.return_value = secret

        result = resolve_connection("dc-empty", "underwriting")
        assert result is None


class TestResolveCredentials:
    @patch("feast.api.catalog.connections.resolve_connection")
    def test_with_connection_ref(self, mock_resolve):
        mock_resolve.return_value = {"s3.access-key-id": "key"}
        ds = MockSavedDataset("table1", tags={"connection-ref": "dc-minio"})

        result = resolve_credentials(ds, "underwriting")
        assert result is not None
        assert result["s3.access-key-id"] == "key"
        mock_resolve.assert_called_once_with("dc-minio", "underwriting")

    def test_without_connection_ref(self):
        ds = MockSavedDataset("table1", tags={})
        result = resolve_credentials(ds, "underwriting")
        assert result is None

    @patch("feast.api.catalog.connections.resolve_connection")
    def test_connection_ref_not_found(self, mock_resolve):
        mock_resolve.return_value = None
        ds = MockSavedDataset("table1", tags={"connection-ref": "nonexistent"})

        result = resolve_credentials(ds, "underwriting")
        assert result is None
