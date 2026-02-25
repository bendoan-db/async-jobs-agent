"""
Genie tool for the workflow task agent.

Wraps the GenieAgent from databricks_langchain to allow
querying structured data using natural language.
"""

import logging
from typing import Any, Optional

from databricks_langchain.genie import GenieAgent
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class GenieQueryInput(BaseModel):
    """Input schema for the Genie query tool."""
    query: str = Field(description="The natural language query to ask Genie about the data.")


def create_genie_tool(
    space_id: str,
    agent_name: str = "Genie",
    description: Optional[str] = None,
) -> StructuredTool:
    """
    Create a Genie tool that can query structured data using natural language.

    Args:
        space_id: The Databricks Genie space ID.
        agent_name: Name for the Genie agent (default: "Genie").
        description: Description of what this Genie space can answer.

    Returns:
        A StructuredTool configured to query the Genie space.
    """
    logger.info("Creating Genie tool for space_id: %s", space_id)

    genie_agent = GenieAgent(
        genie_space_id=space_id,
        genie_agent_name=agent_name,
        description=description,
    )

    def query_genie(query: str) -> str:
        """Query the Genie space with a natural language question."""
        try:
            logger.info("Querying Genie space %s: %s", space_id, query[:100])
            messages: list[dict[str, Any]] = [{"role": "user", "content": query}]
            response = genie_agent.invoke({"messages": messages})

            if isinstance(response, dict):
                result = response.get("content", str(response))
            elif hasattr(response, "content"):
                result = response.content
            else:
                result = str(response)

            logger.info("Genie query completed successfully")
            return result

        except Exception as e:
            logger.error("Error querying Genie space %s: %s", space_id, e)
            return f"Error querying Genie: {e}"

    tool_description = description or "Query structured data using natural language through Databricks Genie."

    return StructuredTool.from_function(
        func=query_genie,
        name="query_genie",
        description=tool_description,
        args_schema=GenieQueryInput,
    )
