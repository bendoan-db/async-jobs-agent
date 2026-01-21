"""
Centralized Databricks client management.

Provides a singleton WorkspaceClient instance to avoid redundant
client creation across tool modules.
"""

import logging
from functools import lru_cache

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    """
    Get or create a cached Databricks WorkspaceClient.

    Uses lru_cache to ensure only one client instance is created
    and reused across all tool calls.

    Returns:
        WorkspaceClient: A configured Databricks workspace client.
    """
    logger.debug("Creating Databricks WorkspaceClient")
    return WorkspaceClient()


def reset_client() -> None:
    """
    Reset the cached client.

    Useful for testing or when credentials need to be refreshed.
    """
    get_workspace_client.cache_clear()
    logger.debug("Databricks WorkspaceClient cache cleared")
