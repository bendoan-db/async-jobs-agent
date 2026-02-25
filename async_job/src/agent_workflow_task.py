"""
Agent workflow task: Self-contained LangGraph agent with Genie tools.

This is a standalone agent that runs inside a Databricks workflow task.
It has access to Genie for data queries but NO job tools (preventing
recursive workflow launches). Intermediate steps and the final result
are logged to Lakebase.
"""

import argparse
import inspect
import json
import logging
import os
import sys
from typing import Annotated, Any, Sequence, TypedDict

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path setup: make sibling modules (lakebase_utils, genie_tools) importable
# ---------------------------------------------------------------------------
try:
    _THIS_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _THIS_DIR = os.path.dirname(os.path.abspath(
        inspect.getfile(inspect.currentframe())
    ))

if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from databricks_langchain import ChatDatabricks  # noqa: E402
from langchain_core.messages import AIMessage, AnyMessage  # noqa: E402
from langchain_core.runnables import RunnableConfig, RunnableLambda  # noqa: E402
from langgraph.graph import END, StateGraph  # noqa: E402
from langgraph.graph.message import add_messages  # noqa: E402
from langgraph.prebuilt.tool_node import ToolNode  # noqa: E402

from genie_tools import create_genie_tool  # noqa: E402
from lakebase_utils import log_to_lakebase  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration — passed via CLI args or hardcoded defaults
# ---------------------------------------------------------------------------
LLM_ENDPOINT_NAME = os.getenv("LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4-5")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f0efd2ef5d17d0acf4b9182dc9d980")
GENIE_DESCRIPTION = os.getenv(
    "GENIE_DESCRIPTION",
    "Query customer transaction data using natural language.",
)

SYSTEM_PROMPT = """\
You are a data analyst assistant. Use the query_genie tool to answer the user's question.

After receiving the raw data from Genie, you MUST summarize the results into a \
clear, natural language response. Do not return raw data or tables directly. \
Instead, interpret the data and provide key insights, trends, and a direct \
answer to the user's question in plain language.\
"""


# ---------------------------------------------------------------------------
# Agent definition
# ---------------------------------------------------------------------------
class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]


def build_graph(tools: list, llm_endpoint: str, system_prompt: str):
    """Build a simple LangGraph agent→tools loop."""
    model = ChatDatabricks(endpoint=llm_endpoint)
    model_with_tools = model.bind_tools(tools) if tools else model

    preprocessor = RunnableLambda(
        lambda state: [{"role": "system", "content": system_prompt}] + list(state["messages"])
    )
    model_runnable = preprocessor | model_with_tools

    def call_model(state: AgentState, config: RunnableConfig):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    def should_continue(state: AgentState):
        last = state["messages"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "continue"
        return "end"

    tool_node = ToolNode(tools)

    graph = StateGraph(AgentState)
    graph.add_node("agent", RunnableLambda(call_model))
    graph.add_node("tools", tool_node)
    graph.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
    graph.add_edge("tools", "agent")
    graph.set_entry_point("agent")

    return graph.compile()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Agent workflow task")
    parser.add_argument(
        "--lakebase-instance", required=True,
        help="Lakebase instance name for logging",
    )
    parser.add_argument(
        "--user-request", required=False, default=None,
        help="The user's request to process",
    )
    args = parser.parse_args()

    # Resolve user_request
    user_request = args.user_request
    if not user_request:
        try:
            user_request = dbutils.widgets.get("user_request")  # type: ignore[name-defined]
        except Exception:
            pass

    if not user_request:
        raise ValueError(
            "No user_request provided. Pass --user-request or set via job_parameters."
        )

    logger.info("Agent workflow task starting — user_request: %s", user_request)
    lakebase_instance = args.lakebase_instance

    # Build tools and graph
    tools = [create_genie_tool(
        space_id=GENIE_SPACE_ID,
        description=GENIE_DESCRIPTION,
    )]
    graph = build_graph(tools, LLM_ENDPOINT_NAME, SYSTEM_PROMPT)

    # Stream through the graph and log intermediate steps
    step_number = 0
    result_text = ""

    for event in graph.stream(
        {"messages": [{"role": "user", "content": user_request}]},
        stream_mode="updates",
    ):
        for node_name, node_data in event.items():
            for msg in node_data.get("messages", []):
                # Tool calls from the LLM
                if isinstance(msg, AIMessage) and msg.tool_calls:
                    for tc in msg.tool_calls:
                        step_number += 1
                        log_to_lakebase(
                            instance_name=lakebase_instance,
                            task_name="agent_workflow_task",
                            message=json.dumps({
                                "step": step_number,
                                "tool": tc.get("name", "unknown"),
                                "arguments": tc.get("args", {}),
                            }, default=str),
                            status="tool_call",
                        )
                        logger.info("Step %d — tool_call: %s", step_number, tc.get("name"))

                # Tool results
                elif hasattr(msg, "type") and msg.type == "tool":
                    step_number += 1
                    log_to_lakebase(
                        instance_name=lakebase_instance,
                        task_name="agent_workflow_task",
                        message=json.dumps({
                            "step": step_number,
                            "tool_call_id": getattr(msg, "tool_call_id", ""),
                            "output": msg.content if hasattr(msg, "content") else str(msg),
                        }, default=str),
                        status="tool_result",
                    )
                    logger.info("Step %d — tool_result: %s", step_number, str(msg.content)[:200])

                # Final text response
                elif isinstance(msg, AIMessage) and not msg.tool_calls and msg.content:
                    result_text += msg.content

    if not result_text:
        result_text = "(no text output)"

    logger.info("Agent response: %s", result_text[:500])

    # Log final result
    log_to_lakebase(
        instance_name=lakebase_instance,
        task_name="agent_workflow_task",
        message=result_text,
    )

    print("Agent workflow task completed successfully!")


if __name__ == "__main__":
    main()
