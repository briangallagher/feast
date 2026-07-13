"""Read real Iceberg metadata.json from S3 for loadTable responses.

When a registered table has an s3:// location pointing to a real Iceberg
table, the Catalog API should return the actual metadata (snapshots,
schemas, partition specs) from the metadata.json on S3 — not synthetic
metadata generated from Feast's SavedDataset model.

This makes the Catalog API a spec-compliant Iceberg REST endpoint that
Spark, PyIceberg, Trino, and DuckDB can consume natively.
"""

import json
import logging
import os
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class IcebergMetadataReader:
    """Reads real Iceberg metadata.json files from S3-compatible storage."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
        self.endpoint = endpoint

    def _parse_s3_location(self, location: str) -> Tuple[str, str]:
        parsed = urlparse(location)
        return parsed.netloc, parsed.path.lstrip("/")

    def _find_latest_metadata(self, bucket: str, prefix: str) -> Optional[str]:
        """Find the latest v*.metadata.json in the table's metadata/ directory."""
        metadata_prefix = prefix.rstrip("/") + "/metadata/"
        try:
            resp = self.s3.list_objects_v2(
                Bucket=bucket, Prefix=metadata_prefix
            )
        except ClientError as e:
            logger.warning("Failed to list metadata for %s/%s: %s", bucket, prefix, e)
            return None

        metadata_files = []
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if re.search(r"v\d+\.metadata\.json$", key):
                metadata_files.append(key)

        if not metadata_files:
            logger.warning("No v*.metadata.json found in s3://%s/%s", bucket, metadata_prefix)
            return None

        metadata_files.sort(
            key=lambda k: int(re.search(r"v(\d+)\.metadata\.json$", k).group(1))
        )
        return metadata_files[-1]

    def read_metadata(self, location: str) -> Optional[Dict[str, Any]]:
        """Read the latest metadata.json for an Iceberg table at the given S3 location."""
        bucket, prefix = self._parse_s3_location(location)
        metadata_key = self._find_latest_metadata(bucket, prefix)
        if not metadata_key:
            return None

        try:
            resp = self.s3.get_object(Bucket=bucket, Key=metadata_key)
            content = resp["Body"].read().decode("utf-8")
            return json.loads(content)
        except (ClientError, json.JSONDecodeError) as e:
            logger.warning("Failed to read s3://%s/%s: %s", bucket, metadata_key, e)
            return None

    def build_load_table_response(
        self,
        location: str,
        table_properties: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Build a spec-compliant loadTable response from real S3 metadata.

        Returns None if metadata can't be read (caller should fall back to
        synthetic metadata).
        """
        bucket, prefix = self._parse_s3_location(location)
        metadata_key = self._find_latest_metadata(bucket, prefix)
        if not metadata_key:
            return None

        metadata = self.read_metadata(location)
        if not metadata:
            return None

        if table_properties:
            existing = metadata.get("properties", {})
            existing.update(table_properties)
            metadata["properties"] = existing

        return {
            "metadata-location": f"s3://{bucket}/{metadata_key}",
            "metadata": metadata,
            "config": {},
        }


def create_reader_from_env() -> Optional[IcebergMetadataReader]:
    """Create a metadata reader from environment variables.

    Reuses the same S3 credentials as STS vending (STS_ENDPOINT,
    STS_ACCESS_KEY, STS_SECRET_KEY). Falls back to AWS_* env vars
    if STS vars aren't set.
    """
    endpoint = (
        os.environ.get("STS_ENDPOINT")
        or os.environ.get("S3_ENDPOINT_URL")
        or os.environ.get("AWS_S3_ENDPOINT")
    )
    access_key = (
        os.environ.get("STS_ACCESS_KEY")
        or os.environ.get("AWS_ACCESS_KEY_ID")
    )
    secret_key = (
        os.environ.get("STS_SECRET_KEY")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    if not all([endpoint, access_key, secret_key]):
        logger.info(
            "Iceberg metadata reader not configured "
            "(need S3 endpoint + credentials via STS_* or AWS_* env vars)"
        )
        return None

    logger.info("Iceberg metadata reader enabled (endpoint: %s)", endpoint)
    return IcebergMetadataReader(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    )
