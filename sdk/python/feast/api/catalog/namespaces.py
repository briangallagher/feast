import json

from fastapi import APIRouter, Response

from feast import FeatureStore
from feast.api.catalog.errors import (
    NamespaceAlreadyExistsException,
    NamespaceNotEmptyException,
    NamespaceNotFoundException,
)
from feast.api.catalog.mapping import (
    CATALOG_PROJECT,
    DEFAULT_COLLECTION,
    ensure_catalog_project,
    list_collections_for_ns,
    project_to_namespace_response,
)
from feast.api.catalog.models import (
    CreateNamespaceRequest,
    ListNamespacesResponse,
    NamespaceResponse,
    UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
)

NAMESPACE_SEPARATOR = "\x1f"


def decode_namespace(raw: str) -> list[str]:
    """Decode a %1F-encoded multi-part namespace into its segments.

    FastAPI auto-decodes URL percent-encoding, so the handler receives
    the raw separator byte (0x1F).  For the single-project model the
    {namespace} path param is the collection name directly.
    """
    if NAMESPACE_SEPARATOR in raw:
        return raw.split(NAMESPACE_SEPARATOR)
    return [raw]


def get_namespace_router(store: FeatureStore) -> APIRouter:
    router = APIRouter(tags=["iceberg-catalog-namespaces"])

    # ------------------------------------------------------------------
    # Iceberg namespace = collection within an RHAI namespace (prefix)
    # ------------------------------------------------------------------

    @router.get("/{prefix}/namespaces")
    def list_namespaces(prefix: str) -> ListNamespacesResponse:
        ensure_catalog_project(store)
        collections = list_collections_for_ns(store, prefix)
        if not collections:
            collections.add(DEFAULT_COLLECTION)
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
        ensure_catalog_project(store)

        existing_collections = list_collections_for_ns(store, prefix)
        if ns_name in existing_collections:
            raise NamespaceAlreadyExistsException(ns_name)

        # Store _ns_{prefix}_{collection} tag on the catalog project
        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=False)
        ns_tag_key = f"_ns_{prefix}_{ns_name}"
        tags = dict(project.tags) if project.tags else {}
        if request.properties:
            tags[ns_tag_key] = json.dumps(request.properties)
        else:
            tags[ns_tag_key] = "{}"
        project.tags = tags
        store.registry.apply_project(project, commit=True)
        return project_to_namespace_response(project, ns_name)

    @router.get("/{prefix}/namespaces/{namespace}")
    def get_namespace(prefix: str, namespace: str) -> NamespaceResponse:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace
        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=True)
        return project_to_namespace_response(project, ns_name)

    @router.head("/{prefix}/namespaces/{namespace}")
    def namespace_exists(prefix: str, namespace: str) -> Response:
        ensure_catalog_project(store)
        return Response(status_code=204)

    @router.delete("/{prefix}/namespaces/{namespace}", status_code=204)
    def drop_namespace(prefix: str, namespace: str) -> Response:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        # Check that the collection is empty
        catalog_datasets = store.registry.list_saved_datasets(
            project=CATALOG_PROJECT,
            allow_cache=False,
            namespace=prefix,
        )
        ns_datasets = [
            ds
            for ds in catalog_datasets
            if (ds.collection or DEFAULT_COLLECTION) == ns_name
        ]
        if ns_datasets:
            raise NamespaceNotEmptyException(namespace)

        # Remove _ns_{prefix}_{collection} tag from the catalog project
        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=False)
        ns_tag_key = f"_ns_{prefix}_{ns_name}"
        tags = dict(project.tags) if project.tags else {}
        if ns_tag_key in tags:
            del tags[ns_tag_key]
            project.tags = tags
            store.registry.apply_project(project, commit=True)

        return Response(status_code=204)

    @router.post("/{prefix}/namespaces/{namespace}/properties")
    def update_namespace_properties(
        prefix: str, namespace: str, request: UpdateNamespacePropertiesRequest
    ) -> UpdateNamespacePropertiesResponse:
        ensure_catalog_project(store)
        ns_parts = decode_namespace(namespace)
        ns_name = ns_parts[0] if ns_parts else namespace

        project = store.registry.get_project(CATALOG_PROJECT, allow_cache=False)
        ns_tag_key = f"_ns_{prefix}_{ns_name}"
        tags = dict(project.tags) if project.tags else {}

        # Load existing properties from the collection tag
        try:
            current_props: dict[str, str] = json.loads(tags.get(ns_tag_key, "{}"))
        except (json.JSONDecodeError, TypeError):
            current_props = {}

        removed: list[str] = []
        missing: list[str] = []
        updated: list[str] = []

        if request.removals:
            for key in request.removals:
                if key in current_props:
                    del current_props[key]
                    removed.append(key)
                else:
                    missing.append(key)

        if request.updates:
            for key, value in request.updates.items():
                current_props[key] = value
                updated.append(key)

        tags[ns_tag_key] = json.dumps(current_props)
        project.tags = tags
        store.registry.apply_project(project, commit=True)

        return UpdateNamespacePropertiesResponse(
            removed=removed,
            updated=updated,
            missing=missing,
        )

    return router
