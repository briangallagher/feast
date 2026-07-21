"""Resolve Data Connection credentials from K8s Secrets for loadTable/loadVolume.

When a table or volume has a `connection-ref` tag naming a K8s Secret,
this module reads the Secret, maps its fields to Iceberg REST config keys,
and returns them for inclusion in the loadTable/loadVolume response.

Phase 1: static credentials from RHOAI Data Connections (K8s Secrets).
Phase 2: replaced by native STS vending from Polaris/UC.

Design ref: design/07-data-connections-credential-vending.md §Credential Resolution Flow
"""

import base64
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

SECRET_FIELD_TO_ICEBERG_CONFIG = {
    "AWS_ACCESS_KEY_ID": "s3.access-key-id",
    "AWS_SECRET_ACCESS_KEY": "s3.secret-access-key",
    "AWS_S3_ENDPOINT": "s3.endpoint",
    "AWS_DEFAULT_REGION": "client.region",
}

MANAGED_LABELS = ("opendatahub.io/dashboard", "opendatahub.io/managed")


def _get_k8s_client():
    """Lazy-initialize and return a Kubernetes CoreV1Api client."""
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    return k8s_client.CoreV1Api()


def resolve_connection(
    connection_name: str, namespace: str
) -> Optional[Dict[str, str]]:
    """Read a Data Connection K8s Secret and return Iceberg config keys."""
    import os
    override_namespace = os.environ.get("CATALOG_CONNECTIONS_NAMESPACE")
    target_namespace = override_namespace if override_namespace else namespace

    try:
        from kubernetes import client as k8s_client
    except ImportError:
        logger.error("kubernetes package not installed — credential resolution unavailable")
        return None

    try:
        v1 = _get_k8s_client()
        secret = v1.read_namespaced_secret(connection_name, target_namespace)
    except k8s_client.ApiException as e:
        if e.status == 404:
            logger.warning(
                "Data Connection Secret '%s' not found in namespace '%s'",
                connection_name, target_namespace,
            )
            return None
        logger.error(
            "Failed to read Secret '%s' in namespace '%s': %s",
            connection_name, target_namespace, e,
        )
        return None
    except Exception as e:
        logger.error("K8s API unavailable for credential resolution: %s", e)
        return None

    labels = secret.metadata.labels or {}
    if not any(labels.get(lbl) == "true" for lbl in MANAGED_LABELS):
        logger.warning(
            "Secret '%s' in namespace '%s' lacks any managed label (%s) — refusing to read",
            connection_name, namespace, ", ".join(MANAGED_LABELS),
        )
        return None

    config: Dict[str, str] = {}
    secret_data = secret.data or {}
    for secret_key, config_key in SECRET_FIELD_TO_ICEBERG_CONFIG.items():
        value = secret_data.get(secret_key)
        if value:
            try:
                config[config_key] = base64.b64decode(value).decode("utf-8")
            except Exception:
                logger.warning(
                    "Failed to decode field '%s' from Secret '%s'",
                    secret_key, connection_name,
                )

    endpoint = config.get("s3.endpoint", "")
    if endpoint and "amazonaws.com" not in endpoint:
        config["s3.path-style-access"] = "true"

    return config if config else None


def resolve_credentials(dataset, prefix: str) -> Optional[Dict[str, str]]:
    """Resolve credentials for a SavedDataset via its connection-ref tag.

    Args:
        dataset: A Feast SavedDataset instance.
        prefix: The Feast project name (= K8s namespace where the Secret lives).

    Returns:
        Dict of Iceberg config keys if connection-ref is set and resolved,
        None otherwise.
    """
    connection_name = dataset.tags.get("connection-ref")
    if not connection_name:
        return None

    return resolve_connection(connection_name, prefix)
