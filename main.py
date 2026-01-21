#!/usr/bin/env python3
"""
Main entry point for running the LangGraph supervisor agent locally.
Sets up Databricks environment variables and runs the agent with an example input.
"""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Required Databricks environment variables for Model Serving endpoints
# These can be set in .env file or exported in shell
REQUIRED_ENV_VARS = [
    "DATABRICKS_HOST",      # e.g., https://<workspace>.cloud.databricks.com
    "DATABRICKS_TOKEN",     # Personal access token or service principal token
]


def load_config() -> dict:
    """Load configuration from YAML file."""
    config_path = Path(__file__).parent / "agent" / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def check_environment() -> bool:
    """Verify required environment variables are set."""
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        print("Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nSet these in a .env file or export them in your shell:")
        print("  export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com")
        print("  export DATABRICKS_TOKEN=<your-token>")
        return False
    return True


def main():
    if not check_environment():
        return

    # Load configuration
    config = load_config()

    # Configure MLflow to use Databricks tracking service
    import mlflow
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_id=config["mlflow_experiment_id"])

    # Import agent after environment check and MLflow setup to avoid initialization errors
    from agent.agent import AGENT

    # Example input message
    example_input = {
    "input": [
        {"role": "user", "content": """what was my highest-revenue generating product?

        ADDITIONAL CONTEXT: 
        ica_id = AA1
        client_id = client_a"""}
        ], 
    "custom_inputs": {
            "ica_id": "AA1", 
            "client_id": "client_a"
        }
    }

    print("=" * 60)
    print("Running LangGraph Supervisor Agent")
    print("=" * 60)

    response = AGENT.predict(example_input)
    print("-" * 60)
    print(response)
    print("-" * 60)


    thread_id = response.custom_outputs["thread_id"]

    example_input_2 = {
    "input": [
        {"role": "user", "content": """What was my previous question?"""}
        ], 
    "custom_inputs": {
            "ica_id": "AA1", 
            "client_id": "client_a",
            "thread_id": thread_id
        }
    }

    follow_up_response = AGENT.predict(example_input_2)
    print("-" * 60)
    print(follow_up_response)
    print("-" * 60)

    print("*" * 60)
    print("Agent execution complete.")


if __name__ == "__main__":
    main()
