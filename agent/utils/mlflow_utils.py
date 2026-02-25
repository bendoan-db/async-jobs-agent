"""
MLflow utilities for experiment tracking and model logging.

This module provides shared functions for setting up MLflow tracking
with Databricks.
"""

import mlflow


def setup_mlflow_tracking(experiment_id: str) -> None:
    """
    Configure MLflow to use Databricks tracking service.

    Args:
        experiment_id: The MLflow experiment ID to use.
    """
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_id=experiment_id)


def setup_mlflow_registry() -> None:
    """Configure MLflow to use Unity Catalog as model registry."""
    mlflow.set_registry_uri("databricks-uc")
