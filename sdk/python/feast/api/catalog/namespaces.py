from fastapi import APIRouter, Response

from feast import FeatureStore
from feast.api.catalog.errors import (
    NamespaceAlreadyExistsException,
    NamespaceNotEmptyException,
    NamespaceNotFoundException,
)
from feast.api.catalog.mapping import (
    CATALOG_MANAGED_TAG,
    namespace_properties_to_project_kwargs,
    project_to_namespace_response,
)
from feast.api.catalog.models import (
    CreateNamespaceRequest,
    ListNamespacesResponse,
    NamespaceResponse,
    UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
)
from feast.errors import FeastObjectNotFoundException
from feast.project import Project

DEFAULT_SCHEMA = "default"

NAMESPACE_SEPARATOR = "\x1f"


def decode_namespace(raw: str) -> list[str]:
    """Decode a %1F-encoded multi-part namespace into its segments.

    FastAPI auto-decodes URL percent-encoding, so the handler receives
    the raw separator byte (0x1F). For the current two-level model,
    {namespace} is the collection name directly. Multi-part support
    is forward-compatible with deeper nesting.
    """
    if NAMESPACE_SEPARATOR in raw:
        return raw.split(NAMESPACE_SEPARATOR)
    return [raw]


def get_namespace_router(store: FeatureStore) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-namespaces"])

    def _ensure_project_exists(prefix: str) -> None:
        try:
            store.registry.get_project(prefix, allow_cache=True)
        except FeastObjectNotFoundException:
            raise NamespaceNotFoundException(prefix)

    def _list_collections(prefix: str) -> set[str]:
        """Gather unique SavedDataset.namespace values in the project."""
        catalog_datasets = store.registry.list_saved_datasets(
            project=prefix,
            allow_cache=True,
            tags={CATALOG_MANAGED_TAG: "true"},
        )
        seen = set()
        for ds in catalog_datasets:
            seen.add(ds.namespace or DEFAULT_SCHEMA)
        return seen

    @router.get("/{prefix}/namespaces")
    def list_namespaces(prefix: str) -> ListNamespacesResponse:
        _ensure_project_exists(prefix)
        collections = _list_collections(prefix)
        if not collections:
            collections.add(DEFAULT_SCHEMA)
        return ListNamespacesResponse(
            namespaces=[decode_namespace(ns) for ns in sorted(collections)]
        )

    @router.post("/{prefix}/namespaces", status_code=200)
    def create_namespace(
        prefix: str, request: CreateNamespaceRequest
    ) -> NamespaceResponse:
        if len(request.namespace) != 1:
            raise NamespaceNotFoundException(".".join(request.namespace))

        ns_name = request.namespace[0]

        try:
            store.registry.get_project(prefix, allow_cache=False)
        except FeastObjectNotFoundException:
            kwargs = namespace_properties_to_project_kwargs(
                [prefix], request.properties or {}
            )
            project = Project(**kwargs)
            store.registry.apply_project(project, commit=True)
            return project_to_namespace_response(project, ns_name)

        existing_collections = _list_collections(prefix)
        if ns_name in existing_collections:
            raise NamespaceAlreadyExistsException(ns_name)

        project = store.registry.get_project(prefix, allow_cache=False)
        ns_meta_key = f"_ns_meta_{ns_name}"
        tags = dict(project.tags) if project.tags else {}
        if request.properties:
            import json
            tags[ns_meta_key] = json.dumps(request.properties)
        else:
            tags[ns_meta_key] = "{}"
        project.tags = tags
        store.registry.apply_project(project, commit=True)
        return project_to_namespace_response(project, ns_name)

    @router.get("/{prefix}/namespaces/{namespace}")
    def get_namespace(prefix: str, namespace: str) -> NamespaceResponse:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        project = store.registry.get_project(prefix, allow_cache=True)
        return project_to_namespace_response(project, ns_name)

    @router.head("/{prefix}/namespaces/{namespace}")
    def namespace_exists(prefix: str, namespace: str) -> Response:
        _ensure_project_exists(prefix)
        return Response(status_code=204)

    @router.delete("/{prefix}/namespaces/{namespace}", status_code=204)
    def drop_namespace(prefix: str, namespace: str) -> Response:
        _ensure_project_exists(prefix)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        catalog_datasets = store.registry.list_saved_datasets(
            project=prefix,
            allow_cache=False,
            tags={CATALOG_MANAGED_TAG: "true"},
        )
        ns_datasets = [ds for ds in catalog_datasets if (ds.namespace or DEFAULT_SCHEMA) == ns_name]
        if ns_datasets:
            raise NamespaceNotEmptyException(namespace)

        return Response(status_code=204)

    @router.post("/{prefix}/namespaces/{namespace}/properties")
    def update_namespace_properties(
        prefix: str, namespace: str, request: UpdateNamespacePropertiesRequest
    ) -> UpdateNamespacePropertiesResponse:
        _ensure_project_exists(prefix)

        project = store.registry.get_project(prefix, allow_cache=False)
        current_tags = dict(project.tags) if project.tags else {}
        removed = []
        missing = []
        updated = []

        if request.removals:
            for key in request.removals:
                if key in current_tags:
                    del current_tags[key]
                    removed.append(key)
                else:
                    missing.append(key)

        if request.updates:
            for key, value in request.updates.items():
                current_tags[key] = value
                updated.append(key)

        project.tags = current_tags
        store.registry.apply_project(project, commit=True)

        return UpdateNamespacePropertiesResponse(
            removed=removed,
            updated=updated,
            missing=missing,
        )

    return router
