"""
Supplemental store for catalog-only assets.

Stores asset types that Feast's registry doesn't natively support:
  - Document collections (e.g., "P&C Policy Library")
  - Vector indexes (e.g., "policy-embeddings in Milvus")
  - Dataset pointers (e.g., "curated training set v2")

Uses SQLite for persistence (PVC-backed in OpenShift). Same durability
pattern as Feast's file-based registry — simple, no external dependencies.

Target location in Feast repo: sdk/python/feast/api/catalog/store.py
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import (
    ListTablesResponse,
    LoadTableResult,
    Schema,
    TableIdentifier,
    TableMetadata,
)

_DEFAULT_DB_PATH = "/tmp/catalog_supplemental.db"


class SupplementalStore:
    """SQLite-backed store for catalog-only assets."""

    def __init__(self, db_path: str = _DEFAULT_DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_assets (
                    namespace TEXT NOT NULL,
                    name TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    location TEXT NOT NULL DEFAULT '',
                    properties TEXT NOT NULL DEFAULT '{}',
                    schema_fields TEXT NOT NULL DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (namespace, name)
                )
                """
            )

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def create_asset(
        self,
        namespace: str,
        name: str,
        asset_type: str,
        location: str = "",
        properties: Optional[Dict[str, str]] = None,
        schema_fields: Optional[List[Dict[str, Any]]] = None,
    ) -> LoadTableResult:
        props = properties or {}
        props["asset_type"] = asset_type
        fields = schema_fields or []

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO catalog_assets (namespace, name, asset_type, location, properties, schema_fields)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (namespace, name, asset_type, location, json.dumps(props), json.dumps(fields)),
            )

        return self._to_load_table_result(namespace, name, location, props, fields)

    def get_asset(self, namespace: str, name: str) -> Optional[LoadTableResult]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM catalog_assets WHERE namespace = ? AND name = ?",
                (namespace, name),
            ).fetchone()

        if not row:
            return None

        return self._to_load_table_result(
            namespace=row["namespace"],
            name=row["name"],
            location=row["location"],
            properties=json.loads(row["properties"]),
            schema_fields=json.loads(row["schema_fields"]),
        )

    def asset_exists(self, namespace: str, name: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM catalog_assets WHERE namespace = ? AND name = ?",
                (namespace, name),
            ).fetchone()
        return row is not None

    def list_assets(self, namespace: str) -> List[TableIdentifier]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT namespace, name FROM catalog_assets WHERE namespace = ?",
                (namespace,),
            ).fetchall()

        return [
            TableIdentifier(namespace=[row["namespace"]], name=row["name"])
            for row in rows
        ]

    def delete_asset(self, namespace: str, name: str) -> bool:
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM catalog_assets WHERE namespace = ? AND name = ?",
                (namespace, name),
            )
        return cursor.rowcount > 0

    def _to_load_table_result(
        self,
        namespace: str,
        name: str,
        location: str,
        properties: Dict[str, str],
        schema_fields: List[Dict[str, Any]],
    ) -> LoadTableResult:
        table_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"catalog://{namespace}/{name}"))

        metadata = TableMetadata(
            **{
                "format-version": 2,
                "table-uuid": table_uuid,
                "location": location,
                "schemas": [
                    Schema(type="struct", **{"schema-id": 0}, fields=schema_fields)
                ],
                "current-schema-id": 0,
                "properties": properties,
            }
        )

        return LoadTableResult(
            **{
                "metadata-location": f"catalog://{namespace}/{name}/metadata",
                "metadata": metadata,
                "config": {},
            }
        )
