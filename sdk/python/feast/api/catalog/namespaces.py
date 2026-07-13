"""
Namespace endpoints following the Iceberg REST Catalog API spec.

Delegates to Feast's gRPC handler (ListProjects, GetProject, ApplyProject)
and translates responses to Iceberg-shaped JSON via the mapping layer.

Target location in Feast repo: sdk/python/feast/api/catalog/namespaces.py
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.registry import RegistryServer_pb2

from .mapping import (
    feast_project_to_namespace,
    feast_projects_to_namespaces,
)
from .models import (
    CreateNamespaceRequest,
    IcebergErrorResponse,
    ListNamespacesResponse,
    NamespaceResponse,
)

logger = logging.getLogger(__name__)


def get_namespace_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/{prefix}/namespaces",
        response_model=ListNamespacesResponse,
        summary="List namespaces",
        description="List all namespaces. Each Feast project maps to a single-level namespace.",
    )
    def list_namespaces(
        prefix: str,
        page_token: Optional[str] = Query(None, alias="pageToken"),
        page_size: Optional[int] = Query(None, alias="pageSize"),
    ):
        req = RegistryServer_pb2.ListProjectsRequest(allow_cache=True)
        response = grpc_call(grpc_handler.ListProjects, req)
        return feast_projects_to_namespaces(response)

    @router.get(
        "/{prefix}/namespaces/{namespace}",
        response_model=NamespaceResponse,
        summary="Get namespace properties",
    )
    def get_namespace(prefix: str, namespace: str):
        req = RegistryServer_pb2.GetProjectRequest(
            name=namespace, allow_cache=True
        )
        try:
            response = grpc_call(grpc_handler.GetProject, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Namespace does not exist: {namespace}",
                    type="NoSuchNamespaceException",
                    code=404,
                ).model_dump(),
            )
        return feast_project_to_namespace(response)

    @router.post(
        "/{prefix}/namespaces",
        response_model=NamespaceResponse,
        status_code=200,
        summary="Create a namespace",
    )
    def create_namespace(prefix: str, body: CreateNamespaceRequest):
        if not body.namespace or len(body.namespace) == 0:
            raise HTTPException(status_code=400, detail="Namespace must not be empty")

        namespace_name = body.namespace[0]

        from feast.protos.feast.core import Project_pb2

        project_spec = Project_pb2.ProjectSpec(
            name=namespace_name,
            tags=body.properties,
        )
        project_proto = Project_pb2.Project(spec=project_spec)

        req = RegistryServer_pb2.ApplyProjectRequest(
            project=project_proto, commit=True
        )
        try:
            grpc_call(grpc_handler.ApplyProject, req)
        except Exception as e:
            raise HTTPException(
                status_code=409,
                detail=IcebergErrorResponse(
                    message=f"Namespace already exists: {namespace_name}",
                    type="AlreadyExistsException",
                    code=409,
                ).model_dump(),
            ) from e

        return NamespaceResponse(
            namespace=[namespace_name], properties=body.properties
        )

    return router
