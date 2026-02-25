import logging
import os
import uuid
from typing import Annotated, Any, Generator, Optional, Sequence, TypedDict

import mlflow
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
    CheckpointSaver,
)
from langchain_core.messages import AIMessage, AIMessageChunk, AnyMessage
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
)

from agent.config_loader import (
    get_llm_endpoint_name,
    get_system_prompt,
    get_lakebase_instance_name,
    get_databricks_job_id,
)
from agent.tools import create_job_tools

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

############################################
# Load configuration
############################################
LLM_ENDPOINT_NAME = get_llm_endpoint_name()
SYSTEM_PROMPT = get_system_prompt()
LAKEBASE_INSTANCE_NAME = get_lakebase_instance_name()
DATABRICKS_JOB_ID = get_databricks_job_id()

###############################################################################
## Define tools — primary agent only has workflow job tools
###############################################################################

UC_TOOL_NAMES: list[str] = []
VECTOR_SEARCH_TOOLS = []

tools: list = []

if UC_TOOL_NAMES:
    uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
    tools.extend(uc_toolkit.tools)

tools.extend(VECTOR_SEARCH_TOOLS)
tools.extend(create_job_tools(DATABRICKS_JOB_ID))

#####################
## Define agent logic
#####################

class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]
    job_started: bool  # Flag to track if start_databricks_job was called

class LangGraphResponsesAgent(ResponsesAgent):
    """Stateful agent using ResponsesAgent with pooled Lakebase checkpointing."""

    def __init__(self, _lakebase_config: dict[str, Any]):
        self.tools = tools
        self.model = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
        self.system_prompt = SYSTEM_PROMPT
        self.model_with_tools = self.model.bind_tools(self.tools) if self.tools else self.model

    def _create_graph(self, checkpointer: Any):
        def should_continue(state: AgentState):
            """Determine whether to continue tool execution or end the graph.

            If a job was just started (job_started flag is True), we end immediately
            to return control to the user rather than continuing the tool loop.
            """
            # If a job was started, break out of the graph
            if state.get("job_started", False):
                return "end"

            messages = state["messages"]
            last_message = messages[-1]
            if isinstance(last_message, AIMessage) and last_message.tool_calls:
                return "continue"
            return "end"

        preprocessor = (
            RunnableLambda(lambda state: [{"role": "system", "content": self.system_prompt}] + state["messages"])
            if self.system_prompt
            else RunnableLambda(lambda state: state["messages"])
        )
        model_runnable = preprocessor | self.model_with_tools

        def call_model(state: AgentState, config: RunnableConfig):
            response = model_runnable.invoke(state, config)
            return {"messages": [response]}

        # Wrap ToolNode to detect when start_databricks_job is called
        tool_node = ToolNode(self.tools)

        def call_tools(state: AgentState, config: RunnableConfig):
            """Execute tools and check if start_databricks_job was called."""
            result = tool_node.invoke(state, config)

            # Check if start_databricks_job was called by examining the tool messages
            job_started = False
            messages = state.get("messages", [])

            # Find the last AIMessage to check which tools were called
            for msg in reversed(messages):
                if isinstance(msg, AIMessage) and msg.tool_calls:
                    for tool_call in msg.tool_calls:
                        if tool_call.get("name") == "start_databricks_job":
                            job_started = True
                            break
                    break

            # Add job_started flag to the result
            result["job_started"] = job_started
            return result

        workflow = StateGraph(AgentState)
        workflow.add_node("agent", RunnableLambda(call_model))

        if self.tools:
            workflow.add_node("tools", call_tools)
            workflow.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
            workflow.add_edge("tools", "agent")
        else:
            workflow.add_edge("agent", END)

        workflow.set_entry_point("agent")
        return workflow.compile(checkpointer=checkpointer)

    def _get_or_create_thread_id(self, request: ResponsesAgentRequest) -> str:
        """Get thread_id from request or create a new one.

        Priority:
        1. Use thread_id from custom_inputs if present
        2. Use conversation_id from chat context if available
        3. Generate a new UUID

        Returns:
            thread_id: The thread identifier to use for this conversation
        """
        ci = dict(request.custom_inputs or {})

        if "thread_id" in ci:
            return ci["thread_id"]

        # using conversation id from chat context as thread id
        # https://mlflow.org/docs/latest/api_reference/python_api/mlflow.types.html#mlflow.types.agent.ChatContext
        if request.context and getattr(request.context, "conversation_id", None):
            return request.context.conversation_id

        # Generate new thread_id
        return str(uuid.uuid4())

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        thread_id = self._get_or_create_thread_id(request)
        ci = dict(request.custom_inputs or {})
        ci["thread_id"] = thread_id
        request.custom_inputs = ci

        # Convert incoming Responses messages to ChatCompletions format
        # LangChain will automatically convert from ChatCompletions to LangChain format
        cc_msgs = self.prep_msgs_for_cc_llm([i.model_dump() for i in request.input])
        langchain_msgs = cc_msgs
        checkpoint_config = {"configurable": {"thread_id": thread_id}}

        with CheckpointSaver(instance_name=LAKEBASE_INSTANCE_NAME) as checkpointer:
            graph = self._create_graph(checkpointer)

            for event in graph.stream(
                {"messages": langchain_msgs},
                checkpoint_config,
                stream_mode=["updates", "messages"],
            ):
                if event[0] == "updates":
                    for node_data in event[1].values():
                        if len(node_data.get("messages", [])) > 0:
                            yield from output_to_responses_items_stream(node_data["messages"])
                elif event[0] == "messages":
                    try:
                        chunk = event[1][0]
                        if isinstance(chunk, AIMessageChunk) and chunk.content:
                            yield ResponsesAgentStreamEvent(
                                **self.create_text_delta(delta=chunk.content, item_id=chunk.id),
                            )
                    except Exception as exc:
                        logger.error("Error streaming chunk: %s", exc)


# ----- Export model -----
mlflow.langchain.autolog()
AGENT = LangGraphResponsesAgent(LAKEBASE_INSTANCE_NAME)
mlflow.models.set_model(AGENT)
