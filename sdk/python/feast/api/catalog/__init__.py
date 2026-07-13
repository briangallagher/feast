"""
Catalog API — Iceberg REST Catalog endpoints for the Feast server.

This module adds a second API surface to the Feast server process:
  /api/catalog/v1/{prefix}/namespaces  — namespace CRUD (delegates to Feast projects)
  /api/catalog/v1/{prefix}/namespaces/{ns}/tables — table CRUD (merges Feast + supplemental)

Integration: one line in RestRegistryServer._register_routes():
    register_catalog_routes(self.app, self.grpc_handler)

Target location in Feast repo: sdk/python/feast/api/catalog/__init__.py
"""

from __future__ import annotations

import os
from typing import Optional

from fastapi import FastAPI

from .namespaces import get_namespace_router
from .store import SupplementalStore
from .tables import get_table_router

_CATALOG_API_PREFIX = "/api/catalog/v1"


def register_catalog_routes(
    app: FastAPI,
    grpc_handler,
    supplemental_db_path: Optional[str] = None,
) -> None:
    """
    Register Catalog API routes on the Feast FastAPI app.

    Call this from RestRegistryServer._register_routes() alongside
    the existing register_all_routes() call. Both API surfaces share
    the same FastAPI app, auth middleware, and TLS config.
    """
    db_path = supplemental_db_path or os.environ.get(
        "CATALOG_SUPPLEMENTAL_DB_PATH", "/tmp/catalog_supplemental.db"
    )
    supplemental_store = SupplementalStore(db_path=db_path)

    app.include_router(
        get_namespace_router(grpc_handler),
        prefix=_CATALOG_API_PREFIX,
        tags=["Catalog API — Namespaces"],
    )
    app.include_router(
        get_table_router(grpc_handler, supplemental_store),
        prefix=_CATALOG_API_PREFIX,
        tags=["Catalog API — Tables"],
    )
