#!/usr/bin/env python3
"""
Main entry point for running the LangGraph supervisor agent locally.
Sets up Databricks environment variables and runs the agent with an example input.
"""

from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

from agent.config_loader import get_mlflow_experiment_id
from agent.utils import check_databricks_environment, setup_mlflow_tracking


def main():
    if not check_databricks_environment():
        return

    # Configure MLflow to use Databricks tracking service
    setup_mlflow_tracking(get_mlflow_experiment_id())

    # Import agent after environment check and MLflow setup to avoid initialization errors
    from agent.agent import AGENT

    # Example input message
    example_input = {
        "input": [
            {
                "role": "user",
                "content": """what was my highest-revenue generating product?

        ADDITIONAL CONTEXT:
        ica_id = AA1
        client_id = client_a""",
            }
        ],
        "custom_inputs": {"ica_id": "AA1", "client_id": "client_a"},
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
        "input": [{"role": "user", "content": """What was my previous question?"""}],
        "custom_inputs": {
            "ica_id": "AA1",
            "client_id": "client_a",
            "thread_id": thread_id,
        },
    }

    follow_up_response = AGENT.predict(example_input_2)
    print("-" * 60)
    print(follow_up_response)
    print("-" * 60)

    print("*" * 60)
    print("Agent execution complete.")


if __name__ == "__main__":
    main()
