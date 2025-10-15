import itertools
import json
import operator
import os
from typing import Any, Generator, Optional
from typing import List
from typing import Union, Sequence

import requests
from databricks.sdk import WorkspaceClient
from databricks_ai_bridge.genie import Genie
from databricks_langchain import VectorSearchRetrieverTool, UCFunctionToolkit, ChatDatabricks
from langchain.agents import tool
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableLambda, RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from pydantic import BaseModel
from pydantic import Field
from requests.auth import HTTPBasicAuth


def generate_sp_token():

    # Configuration for OAuth
    client_id = os.environ['DATABRICKS_CLIENT_ID']
    client_secret = os.environ['DATABRICKS_CLIENT_SECRET']
    token_endpoint = "/oidc/v1/token"

    # Construct the full URL for the token request
    full_url = f"{os.environ['DATABRICKS_HOST']}{token_endpoint}"

    # Make the request for a bearer token
    response = requests.post(
      full_url,
      auth=HTTPBasicAuth(client_id, client_secret),
      data={
          "grant_type": "client_credentials",
          "scope": "all-apis"
      }
    )

    # Extract the bearer token from the response
    if response.status_code == 200:
        bearer_token = response.json().get('access_token')
        return bearer_token
    else:
        raise Exception('Could not generate SP token')


class DatabricksBaseFunction(BaseModel):
    tool_id: str
    tool_name: str
    tool_enabled: bool
    tool_description: str
    tool_description_details: str
    tool_coordinates: str
    tool_type: str


class DatabricksVectorStoreFunction(DatabricksBaseFunction):
    tool_columns: List[str]


class DatabricksTools:
    def build(self):
        raise Exception('Not implemented')


class DatabricksUCFunctionTools(DatabricksTools):

    def __init__(
            self,
            uc_functions: List[DatabricksBaseFunction]
    ):
        self.uc_functions = uc_functions

    def build(self) -> List[BaseTool]:
        function_names = list(map(lambda x: x.tool_coordinates, self.uc_functions))
        uc_toolkit = UCFunctionToolkit(function_names=function_names)
        return uc_toolkit.tools


class DatabricksGenieTool(DatabricksTools):

    def __init__(
            self,
            genie_space: DatabricksBaseFunction
    ):
        self.genie_space = genie_space

    def build(self):
        class SearchGenie(BaseModel):
            query: str = Field(..., description="The question for which you need to run analysis on Databricks Genie.")

        @tool(self.genie_space.tool_coordinates, args_schema=SearchGenie, description=self.genie_space.tool_description_details)
        def search_content(query: str) -> str:
            client = WorkspaceClient(
                host=os.getenv("DATABRICKS_HOST"),
                client_id=os.getenv("DATABRICKS_CLIENT_ID"),
                client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
            )
            genie = Genie(space_id=self.genie_space.tool_coordinates, client=client)
            genie_response = genie.ask_question(query)
            return genie_response.result

        return search_content


class DatabricksGenieTools(DatabricksTools):

    def __init__(
            self,
            genie_spaces: List[DatabricksBaseFunction]
    ):
        self.genie_spaces = genie_spaces

    def build(self) -> List[BaseTool]:
        return [DatabricksGenieTool(genie_space).build() for genie_space in self.genie_spaces]


class DatabricksVectorStoreTools(DatabricksTools):

    def __init__(
            self,
            vector_stores: List[DatabricksVectorStoreFunction]
    ):
        self.vector_stores = vector_stores

    def build(self) -> List[BaseTool]:
        # Hack. Vector Store on langgraph does not support extra_options where one could pass our client ID / secrets
        # Instead, we generate a PAT token
        os.environ['DATABRICKS_TOKEN'] = generate_sp_token()
        vector_stores = []
        for vector_store in self.vector_stores:
            vector_stores.append(VectorSearchRetrieverTool(
                index_name=vector_store.tool_coordinates,
                tool_description=vector_store.tool_description,
                disable_notice=True,
                columns=vector_store.tool_columns
            ))
        # Yet, UC function will complain as multiple auth are now provided. So we quickly set / unset env variable
        # Working as a demo given low concurrency, might be an issue on shared environments with multiple sessions
        del os.environ['DATABRICKS_TOKEN']
        return vector_stores


def get_available_tools():
    with open('config/tools.json', 'r') as f:
        providers = json.loads(f.read())
        for provider in providers.keys():
            tools_json = providers[provider]
            tools = []
            for tool in tools_json:
                if tool['tool_type'] == 'vector':
                    tools.append(DatabricksVectorStoreFunction(**tool))
                elif tool['tool_type'] == 'function':
                    tools.append(DatabricksBaseFunction(**tool))
                elif tool['tool_type'] == 'genie':
                    tools.append(DatabricksBaseFunction(**tool))
                else:
                    raise Exception(
                        'Unsupported tool [{}], invalid format [{}]'.format(tool['tool_id'], tool['tool_type']))
            providers[provider] = tools
        return providers


def build_tools(selected_tools):
    tools = []
    get_attr = operator.attrgetter('tool_type')
    selected_tools_groups = [[k, list(g)] for k, g in itertools.groupby(sorted(selected_tools, key=get_attr), get_attr)]
    for k, gs in selected_tools_groups:
        if k == 'vector':
            tools.extend(DatabricksVectorStoreTools(gs).build())
        elif k == 'function':
            tools.extend(DatabricksUCFunctionTools(gs).build())
        elif k == 'genie':
            tools.extend(DatabricksGenieTools(gs).build())
        else:
            raise Exception(f'Unsupported function type [{k}]')
    return tools


class LangGraphAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def predict(
            self,
            messages: list[ChatAgentMessage],
            context: Optional[ChatContext] = None,
            custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {"messages": self._convert_messages_to_dict(messages)}

        messages = []
        for event in self.agent.stream(request, config=RunnableConfig(recursion_limit=50), stream_mode="updates"):
            for node_data in event.values():
                messages.extend(ChatAgentMessage(**msg) for msg in node_data.get("messages", []))
        return ChatAgentResponse(messages=messages)

    def predict_stream(
            self,
            messages: list[ChatAgentMessage],
            context: Optional[ChatContext] = None,
            custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, config=RunnableConfig(recursion_limit=50), stream_mode="updates"):
            for node_data in event.values():
                yield from (ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"])


class Workflow:

    def __init__(
            self,
            llm: LanguageModelLike,
            tools: Union[Sequence[BaseTool], ToolNode],
            system_prompt: str,
    ):

        # Bind tools to the model
        llm_with_tools = llm.bind_tools(tools)

        def should_continue(state: ChatAgentState):
            messages = state["messages"]
            last_message = messages[-1]
            # If there are function calls, continue. else, end
            if last_message.get("tool_calls"):
                return "continue"
            else:
                return "end"

        preprocessor = RunnableLambda(lambda state: [{"role": "system", "content": system_prompt}] + state["messages"])
        model_runnable = preprocessor | llm_with_tools

        def call_model(state, config):
            response = model_runnable.invoke(state, config)
            return {"messages": [response]}

        # Helper class that enables building an agent that produces ChatAgent compatible messages as state is updated
        workflow = StateGraph(ChatAgentState)
        workflow.add_node("agent", RunnableLambda(call_model))
        workflow.add_node("tools", ChatAgentToolNode(tools))
        workflow.set_entry_point("agent")
        workflow.add_conditional_edges(
            "agent",
            should_continue,
            {
                "continue": "tools",
                "end": END,
            },
        )
        workflow.add_edge("tools", "agent")
        self.agent = LangGraphAgent(workflow.compile())

    def predict(
            self,
            messages: list[ChatAgentMessage],
            context: Optional[ChatContext] = None,
            custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        return self.agent.predict(messages, context, custom_inputs)

    def predict_stream(
            self,
            messages: list[ChatAgentMessage],
            context: Optional[ChatContext] = None,
            custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        return self.agent.predict_stream(messages, context, custom_inputs)


def build_agent(model_base, tools, system_prompt):
    llm: LanguageModelLike = ChatDatabricks(
        endpoint=model_base,
        extra_params={"enable_safety_filter": False}
    )
    return Workflow(
        llm,
        build_tools(tools),
        system_prompt
    )
