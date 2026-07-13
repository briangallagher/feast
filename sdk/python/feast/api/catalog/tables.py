"""
Table endpoints following the Iceberg REST Catalog API spec.

This is the core complexity of the Catalog API: table operations merge results
from two backends:
  1. Feast gRPC handler — for Feast-native assets (DataSources, FeatureViews)
  2. Supplemental store — for catalog-only assets (document collections, vector indexes)

Routing logic:
  - GET (list/get): merge results from both backends
  - POST (create): route by asset_type property — Feast-native → gRPC, else → supplemental
  - DELETE: try supplemental first, fall back to Feast gRPC
  - HEAD: check both backends

Target location in Feast repo: sdk/python/feast/api/catalog/tables.py
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Response

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.registry import RegistryServer_pb2

from .mapping import (
    feast_data_source_to_load_table_result,
    feast_data_sources_to_table_identifiers,
)
from .models import (
    CreateTableRequest,
    IcebergErrorResponse,
    ListTablesResponse,
    LoadTableResult,
    TableIdentifier,
)
from .store import SupplementalStore

logger = logging.getLogger(__name__)

FEAST_NATIVE_ASSET_TYPES = {"data_source", "feature_view", "table"}


def get_table_router(grpc_handler, supplemental_store: SupplementalStore) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/{prefix}/namespaces/{namespace}/tables",
        response_model=ListTablesResponse,
        summary="List tables in a namespace",
        description="Returns a merged view of Feast DataSources and catalog-only assets.",
    )
    def list_tables(prefix: str, namespace: str):
        # 1. Feast DataSources via gRPC
        feast_identifiers = _list_feast_data_sources(grpc_handler, namespace)

        # 2. Supplemental store assets
        supplemental_identifiers = supplemental_store.list_assets(namespace)

        # 3. Merge — supplemental assets appear alongside Feast assets
        all_identifiers = feast_identifiers + supplemental_identifiers

        return ListTablesResponse(identifiers=all_identifiers)

    @router.post(
        "/{prefix}/namespaces/{namespace}/tables",
        response_model=LoadTableResult,
        status_code=200,
        summary="Create a table (or catalog asset)",
    )
    def create_table(prefix: str, namespace: str, body: CreateTableRequest):
        asset_type = body.properties.get("asset_type", "table")

        if asset_type in FEAST_NATIVE_ASSET_TYPES:
            return _create_feast_data_source(grpc_handler, namespace, body)
        else:
            return _create_supplemental_asset(
                supplemental_store, namespace, body, asset_type
            )

    @router.get(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        response_model=LoadTableResult,
        summary="Load a table (or catalog asset)",
    )
    def load_table(prefix: str, namespace: str, table: str):
        # Try supplemental store first (cheaper lookup)
        result = supplemental_store.get_asset(namespace, table)
        if result:
            return result

        # Fall back to Feast gRPC
        return _get_feast_data_source(grpc_handler, namespace, table)

    @router.head(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        summary="Check if a table exists",
    )
    def table_exists(prefix: str, namespace: str, table: str):
        if supplemental_store.asset_exists(namespace, table):
            return Response(status_code=204)

        try:
            _get_feast_data_source(grpc_handler, namespace, table)
            return Response(status_code=204)
        except HTTPException:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Table does not exist: {namespace}.{table}",
                    type="NoSuchTableException",
                    code=404,
                ).model_dump(),
            )

    @router.delete(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        status_code=204,
        summary="Drop a table (or catalog asset)",
    )
    def drop_table(prefix: str, namespace: str, table: str):
        # Try supplemental store first
        if supplemental_store.delete_asset(namespace, table):
            return Response(status_code=204)

        # Fall back to Feast gRPC
        try:
            req = RegistryServer_pb2.DeleteDataSourceRequest(
                name=table, project=namespace, commit=True
            )
            grpc_call(grpc_handler.DeleteDataSource, req)
            return Response(status_code=204)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Table does not exist: {namespace}.{table}",
                    type="NoSuchTableException",
                    code=404,
                ).model_dump(),
            )

    return router


# --- Internal helpers ---


def _list_feast_data_sources(
    grpc_handler, namespace: str
) -> list[TableIdentifier]:
    try:
        req = RegistryServer_pb2.ListDataSourcesRequest(
            project=namespace, allow_cache=True
        )
        response = grpc_call(grpc_handler.ListDataSources, req)
        return feast_data_sources_to_table_identifiers(response, namespace)
    except Exception:
        logger.debug(
            "No Feast project '%s' or no data sources — returning empty list",
            namespace,
        )
        return []


def _get_feast_data_source(
    grpc_handler, namespace: str, name: str
) -> LoadTableResult:
    try:
        req = RegistryServer_pb2.GetDataSourceRequest(
            name=name, project=namespace, allow_cache=True
        )
        response = grpc_call(grpc_handler.GetDataSource, req)
        return feast_data_source_to_load_table_result(response, namespace)
    except Exception:
        raise HTTPException(
            status_code=404,
            detail=IcebergErrorResponse(
                message=f"Table does not exist: {namespace}.{name}",
                type="NoSuchTableException",
                code=404,
            ).model_dump(),
        )


def _create_feast_data_source(
    grpc_handler, namespace: str, body: CreateTableRequest
) -> LoadTableResult:
    from feast.protos.feast.core import DataSource_pb2

    ds_proto = DataSource_pb2.DataSource(
        name=body.name,
        project=namespace,
        tags=body.properties,
    )

    req = RegistryServer_pb2.ApplyDataSourceRequest(
        data_source=ds_proto, project=namespace, commit=True
    )
    try:
        grpc_call(grpc_handler.ApplyDataSource, req)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=IcebergErrorResponse(
                message=f"Failed to create data source: {e}",
                type="ServerError",
                code=500,
            ).model_dump(),
        ) from e

    return _get_feast_data_source(grpc_handler, namespace, body.name)


def _create_supplemental_asset(
    store: SupplementalStore,
    namespace: str,
    body: CreateTableRequest,
    asset_type: str,
) -> LoadTableResult:
    if store.asset_exists(namespace, body.name):
        raise HTTPException(
            status_code=409,
            detail=IcebergErrorResponse(
                message=f"Table already exists: {namespace}.{body.name}",
                type="AlreadyExistsException",
                code=409,
            ).model_dump(),
        )

    schema_fields = []
    if body.schema_:
        schema_fields = body.schema_.fields

    return store.create_asset(
        namespace=namespace,
        name=body.name,
        asset_type=asset_type,
        location=body.location or "",
        properties=body.properties,
        schema_fields=schema_fields,
    )
