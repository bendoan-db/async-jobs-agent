"""
Environment variable utilities for Databricks authentication.

This module provides shared functions for checking required environment
variables across the application.
"""

import os

# Required environment variables for Databricks authentication
REQUIRED_DATABRICKS_ENV_VARS = [
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
]


def check_databricks_environment(verbose: bool = True) -> bool:
    """
    Verify required Databricks environment variables are set.

    Args:
        verbose: If True, print missing variables to stdout.

    Returns:
        True if all required variables are set, False otherwise.
    """
    missing = [var for var in REQUIRED_DATABRICKS_ENV_VARS if not os.getenv(var)]

    if missing and verbose:
        print("Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nSet these in a .env file or export them in your shell:")
        print("  export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com")
        print("  export DATABRICKS_TOKEN=<your-token>")

    return len(missing) == 0


def get_databricks_host() -> str | None:
    """Get the Databricks host URL from environment."""
    return os.getenv("DATABRICKS_HOST")


def get_databricks_token() -> str | None:
    """Get the Databricks token from environment."""
    return os.getenv("DATABRICKS_TOKEN")
