#!/usr/bin/env python3
"""
Deploy the LangGraph agent to Databricks Model Serving.

This script:
1. Logs the agent to MLflow
2. Registers the model to Unity Catalog
3. Deploys to a Model Serving endpoint

Configuration is read from agent/config.yaml. Command-line arguments can override config values.

Usage:
    python deploy.py                    # Use all settings from config.yaml
    python deploy.py --skip-deploy      # Only log and register, skip deployment
    python deploy.py --catalog prod     # Override catalog from config

Example:
    python deploy.py
    python deploy.py --workload-size Medium --no-scale-to-zero
"""

import argparse
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

# Load configuration
CONFIG_PATH = Path(__file__).parent / "agent" / "config.yaml"


def load_config() -> dict:
    """Load configuration from YAML file."""
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def check_environment() -> bool:
    """Verify required environment variables are set."""
    required = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        print(f"Missing required environment variables: {missing}")
        print("Set these in .env or export in shell")
        return False
    return True


def log_agent_to_mlflow(
    config: dict,
    run_name: str = "agent-deployment",
) -> str:
    """
    Log the agent to MLflow and return the model URI.

    Args:
        config: Configuration dictionary
        run_name: Name for the MLflow run

    Returns:
        model_uri: URI of the logged model (e.g., runs:/<run_id>/agent)
    """
    import mlflow
    from mlflow.models.resources import DatabricksGenieSpace, DatabricksServingEndpoint

    experiment_id = config["mlflow_experiment_id"]

    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_id=experiment_id)

    # Define the agent code path
    agent_code_path = Path(__file__).parent / "agent" / "agent.py"

    # Define pip requirements
    pip_requirements = [
        "databricks-langchain[memory]>=0.13.0",
        "databricks-agents>=0.16.0",
        "mlflow>=2.21.0",
        "langgraph>=0.2.0",
        "pyyaml>=6.0",
    ]

    # Define resources the agent needs access to
    resources = [
        DatabricksServingEndpoint(endpoint_name=config["llm_endpoint_name"]),
    ]

    # Add Genie space if configured
    if config.get("genie", {}).get("space_id"):
        resources.append(
            DatabricksGenieSpace(genie_space_id=config["genie"]["space_id"])
        )

    print(f"Logging agent to MLflow experiment: {experiment_id}")
    print(f"Agent code path: {agent_code_path}")

    with mlflow.start_run(run_name=run_name) as run:
        # Log agent using mlflow.langchain.log_model for LangGraph agents
        logged_model = mlflow.langchain.log_model(
            lc_model=str(agent_code_path),
            artifact_path="agent",
            pip_requirements=pip_requirements,
            resources=resources,
            input_example={
                "input": [
                    {"role": "user", "content": "Hello, what can you help me with?"}
                ],
                "custom_inputs": {
                    "ica_id": "example_ica",
                    "client_id": "example_client",
                },
            },
        )

        model_uri = logged_model.model_uri
        print(f"Agent logged successfully!")
        print(f"  Run ID: {run.info.run_id}")
        print(f"  Model URI: {model_uri}")

        # Log deployment parameters
        mlflow.log_params({
            "llm_endpoint": config["llm_endpoint_name"],
            "lakebase_instance": config["lakebase_instance_name"],
        })

        return model_uri


def register_to_unity_catalog(
    model_uri: str,
    catalog: str,
    schema: str,
    model_name: str,
) -> tuple[str, int]:
    """
    Register the logged model to Unity Catalog.

    Args:
        model_uri: MLflow model URI (e.g., runs:/<run_id>/agent)
        catalog: Unity Catalog name
        schema: Schema name
        model_name: Model name

    Returns:
        Tuple of (full_model_name, version)
    """
    import mlflow

    full_model_name = f"{catalog}.{schema}.{model_name}"

    print(f"\nRegistering model to Unity Catalog: {full_model_name}")

    # Set registry URI to Unity Catalog
    mlflow.set_registry_uri("databricks-uc")

    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name=full_model_name,
    )

    version = registered_model.version
    print(f"Model registered successfully!")
    print(f"  Model: {full_model_name}")
    print(f"  Version: {version}")

    return full_model_name, version


def deploy_to_model_serving(
    model_name: str,
    model_version: int,
    endpoint_name: str,
    workload_size: str = "Small",
    scale_to_zero: bool = True,
) -> str:
    """
    Deploy the registered model to a Model Serving endpoint.

    Args:
        model_name: Full Unity Catalog model name (catalog.schema.model)
        model_version: Model version to deploy
        endpoint_name: Name for the serving endpoint
        workload_size: Size of the serving workload (Small, Medium, Large)
        scale_to_zero: Whether to enable scale-to-zero

    Returns:
        endpoint_url: URL of the deployed endpoint
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )

    w = WorkspaceClient()

    print(f"\nDeploying to Model Serving endpoint: {endpoint_name}")
    print(f"  Model: {model_name} (version {model_version})")
    print(f"  Workload size: {workload_size}")
    print(f"  Scale to zero: {scale_to_zero}")

    served_entity = ServedEntityInput(
        entity_name=model_name,
        entity_version=str(model_version),
        workload_size=workload_size,
        scale_to_zero_enabled=scale_to_zero,
    )

    # Check if endpoint exists
    endpoint_exists = False
    try:
        w.serving_endpoints.get(endpoint_name)
        endpoint_exists = True
    except Exception:
        endpoint_exists = False

    if endpoint_exists:
        print(f"Endpoint '{endpoint_name}' exists, updating configuration...")
        w.serving_endpoints.update_config_and_wait(
            name=endpoint_name,
            served_entities=[served_entity],
        )
        print("Endpoint updated successfully!")
    else:
        print(f"Creating new endpoint '{endpoint_name}'...")
        w.serving_endpoints.create_and_wait(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[served_entity],
            ),
        )
        print("Endpoint created successfully!")

    # Get endpoint URL
    host = w.config.host.rstrip("/")
    endpoint_url = f"{host}/serving-endpoints/{endpoint_name}/invocations"

    print(f"\nDeployment complete!")
    print(f"  Endpoint URL: {endpoint_url}")
    print(f"\nTest with:")
    print(f'  curl -X POST "{endpoint_url}" \\')
    print(f'    -H "Authorization: Bearer $(databricks auth token)" \\')
    print(f'    -H "Content-Type: application/json" \\')
    print(f'    -d \'{{"input": [{{"role": "user", "content": "Hello"}}]}}\'')

    return endpoint_url


def main():
    parser = argparse.ArgumentParser(
        description="Deploy LangGraph agent to Databricks Model Serving. "
        "Configuration is read from agent/config.yaml. "
        "Command-line arguments override config values."
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Unity Catalog name (overrides config.yaml)",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schema name (overrides config.yaml)",
    )
    parser.add_argument(
        "--model-name",
        default=None,
        help="Model name (overrides config.yaml)",
    )
    parser.add_argument(
        "--endpoint-name",
        default=None,
        help="Serving endpoint name (overrides config.yaml)",
    )
    parser.add_argument(
        "--workload-size",
        default=None,
        choices=["Small", "Medium", "Large"],
        help="Model serving workload size (overrides config.yaml)",
    )
    parser.add_argument(
        "--no-scale-to-zero",
        action="store_true",
        help="Disable scale-to-zero (overrides config.yaml)",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Only log and register, skip deployment to serving endpoint",
    )

    args = parser.parse_args()

    if not check_environment():
        sys.exit(1)

    # Load configuration
    config = load_config()

    # Get Unity Catalog settings (CLI args override config)
    uc_config = config.get("unity_catalog", {})
    catalog = args.catalog or uc_config.get("catalog")
    schema = args.schema or uc_config.get("schema")
    model_name = args.model_name or uc_config.get("model_name")

    if not all([catalog, schema, model_name]):
        print("Error: catalog, schema, and model_name must be specified")
        print("Set them in agent/config.yaml under 'unity_catalog' or pass as arguments")
        sys.exit(1)

    # Get Model Serving settings (CLI args override config)
    serving_config = config.get("model_serving", {})
    endpoint_name = (
        args.endpoint_name
        or serving_config.get("endpoint_name")
        or model_name.replace("_", "-")
    )
    workload_size = args.workload_size or serving_config.get("workload_size", "Small")
    scale_to_zero = not args.no_scale_to_zero and serving_config.get("scale_to_zero", True)

    print("=" * 60)
    print("Databricks Agent Deployment")
    print("=" * 60)
    print(f"Catalog: {catalog}")
    print(f"Schema: {schema}")
    print(f"Model: {model_name}")
    print(f"Endpoint: {endpoint_name}")
    print(f"Workload Size: {workload_size}")
    print(f"Scale to Zero: {scale_to_zero}")
    print("=" * 60)

    # Step 1: Log agent to MLflow
    print("\n[Step 1/3] Logging agent to MLflow...")
    model_uri = log_agent_to_mlflow(
        config=config,
        run_name=f"deploy-{model_name}",
    )

    # Step 2: Register to Unity Catalog
    print("\n[Step 2/3] Registering to Unity Catalog...")
    full_model_name, version = register_to_unity_catalog(
        model_uri=model_uri,
        catalog=catalog,
        schema=schema,
        model_name=model_name,
    )

    # Step 3: Deploy to Model Serving
    if args.skip_deploy:
        print("\n[Step 3/3] Skipping deployment (--skip-deploy flag)")
        print(f"\nTo deploy manually, run:")
        print(f"  databricks serving-endpoints create --json '{{")
        print(f'    "name": "{endpoint_name}",')
        print(f'    "config": {{"served_entities": [{{"entity_name": "{full_model_name}", "entity_version": "{version}"}}]}}')
        print(f"  }}'")
    else:
        print("\n[Step 3/3] Deploying to Model Serving...")
        deploy_to_model_serving(
            model_name=full_model_name,
            model_version=version,
            endpoint_name=endpoint_name,
            workload_size=workload_size,
            scale_to_zero=scale_to_zero,
        )

    print("\n" + "=" * 60)
    print("Deployment complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
