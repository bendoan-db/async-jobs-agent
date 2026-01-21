"""
Databricks job management tools for the LangGraph agent.

These tools allow the agent to:
1. Start a Databricks job with a user request
2. Poll job status
3. Terminate a running job
"""

import logging
from typing import Any, Optional

from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from langchain_core.tools import tool, StructuredTool
from pydantic import BaseModel, Field

from agent.utils import get_workspace_client, error_response, success_response

logger = logging.getLogger(__name__)


class StartJobInput(BaseModel):
    """Input schema for start_databricks_job tool."""
    user_request: str = Field(description="The user's request/prompt to pass to the job.")
    notebook_params: Optional[dict[str, Any]] = Field(
        default=None,
        description="Optional additional parameters to pass to the notebook."
    )


def create_start_job_tool(job_id: str) -> StructuredTool:
    """
    Factory function to create a start_databricks_job tool with a pre-configured job_id.

    Args:
        job_id: The Databricks job ID to use for all job starts.

    Returns:
        A StructuredTool configured with the specified job_id.
    """
    def start_databricks_job(
        user_request: str,
        notebook_params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Start a Databricks job with the user's request passed as a parameter.

        Use this tool when a user wants to kick off a long-running task or workflow.
        After starting the job, the agent should inform the user that the job has been
        started and provide the run_id so they can check status later.

        Args:
            user_request: The user's request/prompt to pass to the job.
            notebook_params: Optional additional parameters to pass to the notebook.

        Returns:
            A dict containing run_id and status information.
        """
        client = get_workspace_client()

        # Build parameters - always include user_request
        params = {"user_request": user_request}
        if notebook_params:
            params.update(notebook_params)

        try:
            logger.info("Starting Databricks job %s", job_id)
            run = client.jobs.run_now(
                job_id=int(job_id),
                notebook_params=params,
            )
            logger.info("Job %s started with run_id %s", job_id, run.run_id)

            return success_response(
                message=f"Job started successfully. Run ID: {run.run_id}. "
                        f"The user can check the status later by asking about run {run.run_id}.",
                run_id=run.run_id,
                job_id=job_id,
            )
        except Exception as e:
            return error_response(
                operation="start job",
                error=e,
                identifier=f"job {job_id}",
            )

    return StructuredTool.from_function(
        func=start_databricks_job,
        name="start_databricks_job",
        description="Start a Databricks job with the user's request. Use this when a user wants to kick off a long-running task or workflow.",
        args_schema=StartJobInput,
    )


@tool
def poll_databricks_job(run_id: str) -> dict[str, Any]:
    """
    Check the status of a Databricks job run.

    Use this tool when the user asks about the status of a previously started job.

    Args:
        run_id: The run ID returned when the job was started.

    Returns:
        A dict containing the current status, state message, and result if completed.
    """
    client = get_workspace_client()

    try:
        logger.info("Polling status for run_id %s", run_id)
        run = client.jobs.get_run(run_id=int(run_id))

        state = run.state
        life_cycle_state = state.life_cycle_state if state else None
        result_state = state.result_state if state else None
        state_message = state.state_message if state else None

        # Determine if the job is still running
        is_running = life_cycle_state in [
            RunLifeCycleState.PENDING,
            RunLifeCycleState.RUNNING,
            RunLifeCycleState.TERMINATING,
        ]

        # Build response
        response: dict[str, Any] = {
            "success": True,
            "run_id": run_id,
            "life_cycle_state": life_cycle_state.value if life_cycle_state else None,
            "is_running": is_running,
            "state_message": state_message,
        }

        # Add result info if completed
        if result_state:
            response["result_state"] = result_state.value
            response["is_successful"] = result_state == RunResultState.SUCCESS

        # Add run page URL if available
        if run.run_page_url:
            response["run_page_url"] = run.run_page_url

        # Add task outputs if available and job is complete
        if not is_running and run.tasks:
            task_results = []
            for task in run.tasks:
                task_info: dict[str, Any] = {
                    "task_key": task.task_key,
                    "state": task.state.life_cycle_state.value if task.state else None,
                }
                if task.state and task.state.result_state:
                    task_info["result"] = task.state.result_state.value
                task_results.append(task_info)
            response["tasks"] = task_results

        logger.info("Run %s status: %s", run_id, life_cycle_state)
        return response

    except Exception as e:
        return error_response(
            operation="get status",
            error=e,
            identifier=f"run {run_id}",
        )


@tool
def terminate_databricks_job(run_id: str) -> dict[str, Any]:
    """
    Terminate/cancel a running Databricks job.

    Use this tool when the user wants to stop a previously started job.

    Args:
        run_id: The run ID of the job to terminate.

    Returns:
        A dict containing the termination result.
    """
    client = get_workspace_client()

    try:
        logger.info("Attempting to terminate run_id %s", run_id)

        # First check if the job is actually running
        run = client.jobs.get_run(run_id=int(run_id))
        state = run.state
        life_cycle_state = state.life_cycle_state if state else None

        if life_cycle_state not in [
            RunLifeCycleState.PENDING,
            RunLifeCycleState.RUNNING,
        ]:
            logger.warning(
                "Run %s is not in a cancellable state: %s",
                run_id,
                life_cycle_state
            )
            return {
                "success": False,
                "run_id": run_id,
                "message": f"Job run {run_id} is not in a cancellable state. "
                          f"Current state: {life_cycle_state.value if life_cycle_state else 'unknown'}",
                "life_cycle_state": life_cycle_state.value if life_cycle_state else None,
            }

        # Cancel the run
        client.jobs.cancel_run(run_id=int(run_id))
        logger.info("Run %s cancelled successfully", run_id)

        return success_response(
            message=f"Job run {run_id} has been cancelled successfully.",
            run_id=run_id,
        )

    except Exception as e:
        return error_response(
            operation="terminate",
            error=e,
            identifier=f"run {run_id}",
        )


def create_job_tools(job_id: str) -> list[StructuredTool]:
    """
    Create all job management tools with the specified job_id.

    Args:
        job_id: The Databricks job ID to use for starting jobs.

    Returns:
        A list of configured job management tools.
    """
    return [
        create_start_job_tool(job_id),
        poll_databricks_job,
        terminate_databricks_job,
    ]
