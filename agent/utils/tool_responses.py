"""
Standardized tool response helpers.

Provides consistent response formatting for tool success and error cases.
"""

import logging
from typing import Any, Optional, TypedDict

logger = logging.getLogger(__name__)


class ToolResponse(TypedDict, total=False):
    """Base response type for all tool operations."""
    success: bool
    message: str
    error: Optional[str]


class JobStartResponse(ToolResponse):
    """Response from starting a Databricks job."""
    run_id: int
    job_id: str


class JobStatusResponse(ToolResponse):
    """Response from polling a Databricks job."""
    run_id: str
    life_cycle_state: Optional[str]
    result_state: Optional[str]
    is_running: bool
    is_successful: Optional[bool]
    state_message: Optional[str]
    run_page_url: Optional[str]
    tasks: Optional[list[dict[str, Any]]]


class JobTerminateResponse(ToolResponse):
    """Response from terminating a Databricks job."""
    run_id: str
    life_cycle_state: Optional[str]


def success_response(message: str, **kwargs: Any) -> dict[str, Any]:
    """
    Create a standardized success response.

    Args:
        message: Human-readable success message.
        **kwargs: Additional fields to include in the response.

    Returns:
        A dict with success=True and the provided message and fields.
    """
    return {
        "success": True,
        "message": message,
        **kwargs,
    }


def error_response(
    operation: str,
    error: Exception,
    identifier: Optional[str] = None,
    log_error: bool = True,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Create a standardized error response.

    Args:
        operation: Description of the operation that failed (e.g., "start job", "poll status").
        error: The exception that was caught.
        identifier: Optional identifier (e.g., job_id, run_id) for context.
        log_error: Whether to log the error (default True).
        **kwargs: Additional fields to include in the response.

    Returns:
        A dict with success=False, error details, and a formatted message.
    """
    error_str = str(error)

    if identifier:
        message = f"Failed to {operation} for {identifier}: {error_str}"
    else:
        message = f"Failed to {operation}: {error_str}"

    if log_error:
        logger.error(message, exc_info=True)

    return {
        "success": False,
        "error": error_str,
        "message": message,
        **kwargs,
    }
