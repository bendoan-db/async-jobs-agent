"""
Centralized configuration loading for the agent application.

This module provides a single source of truth for loading configuration
from agent/config.yaml.
"""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    """
    Load configuration from agent/config.yaml.

    Returns:
        Configuration dictionary with all settings.

    Raises:
        FileNotFoundError: If config.yaml doesn't exist.
        yaml.YAMLError: If config.yaml is invalid.
    """
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_mlflow_experiment_id() -> str:
    """Get the MLflow experiment ID from config."""
    return load_config()["mlflow_experiment_id"]


def get_llm_endpoint_name() -> str:
    """Get the LLM endpoint name from config."""
    return load_config()["llm_endpoint_name"]


def get_system_prompt() -> str:
    """Get the system prompt from config."""
    return load_config()["system_prompt"]


def get_lakebase_instance_name() -> str:
    """Get the Lakebase instance name from config."""
    return load_config()["lakebase_instance_name"]


def get_databricks_job_id() -> str:
    """Get the Databricks job ID from config."""
    return load_config()["databricks_job_id"]


def get_unity_catalog_config() -> dict[str, Any]:
    """Get Unity Catalog configuration for deployment."""
    return load_config().get("unity_catalog", {})


def get_model_serving_config() -> dict[str, Any]:
    """Get Model Serving configuration for deployment."""
    return load_config().get("model_serving", {})
