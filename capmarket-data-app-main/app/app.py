import itertools
import os
import uuid

import requests
import streamlit as st
from mlflow.types.agent import ChatAgentMessage
from langchain_core.documents.base import Document
from mlflow.types.chat import Function

from utils.agent import get_available_tools, build_agent
from utils.model_output import *


def get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
        user_token=headers.get("X-Forwarded-Access-Token"),
    )


def on_selection_prompt_change():
    st.session_state.selection_prompt = st.session_state.selection_prompt_pill.split(' ', 1)[-1]
    st.session_state.selection_prompt_pill = None


def build_pills(prompts):
    st.pills(
        "prompt_selection",
        options=prompts,
        selection_mode='single',
        key='selection_prompt_pill',
        on_change=on_selection_prompt_change,
        label_visibility='hidden',
    )


def chat_message(st_container, name, avatar):
    return st_container.container(key=f"{name}-{uuid.uuid4()}").chat_message(name=name, avatar=avatar)


def chat_user(st_container, message):
    with chat_message(st_container, name='user', avatar=":material/person:"):
        st.markdown('**You**')
        st.markdown(message)


def chat_assistant(st_container, message):
    with chat_message(st_container, name='error', avatar="images/databricks.svg"):
        st.markdown('**Assistant**')
        st.markdown(message)


def chat_agent(st_container, title, ts):
    available_functions = st.session_state['providers']
    provider_found = None
    with st_container.expander(title, expanded=False, icon=":material/memory:"):
        col1, col2 = st.columns([0.05, 0.95])
        for t in ts:
            function = t.function
            if type(function) is Function:
                function_name = function.name
                function_name = function_name.split('__')[-1]
                for provider in available_functions.keys():
                    function_names = set(map(lambda x: x.tool_coordinates.split('.')[-1], available_functions[provider]))
                    if function_name in function_names:
                        col1.image(f'images/{provider}_agent.png')
                        provider_found = provider.capitalize()
                        break
            col2.write(t)
        if provider_found:
            st.session_state['provider'] = provider_found


def display_history(st_container, messages):
    for message in messages:
        if message.role == 'user':
            chat_user(st_container, message.content)
        elif message.role == 'assistant':
            chat_assistant(st_container, message.content)


def display_supporting_docs(docs, docs_count, docs_provider, columns=3):
    docs = list(docs.values())
    if len(docs) > 0:
        with st.container(border=True):
            st.write("Supporting documents")
            sdgs = [docs[i:i + columns] for i in range(0, len(docs), columns)]
            for sdg in sdgs:
                cols = st.columns(columns)
                for i, sd in enumerate(sdg):
                    col = cols[i % columns]
                    document_type = sd.metadata['document_type'].strip()
                    section_number = sd.metadata['section_number'].strip()
                    tile_name, ext = os.path.splitext(sd.metadata['document_name'].split('/')[-1])
                    document_name = sd.metadata['document_name'].split('/')[-1]
                    if len(document_type) > 0 and len(section_number) > 0:
                        tile_name = f'{tile_name} ({document_type}, section {section_number})'
                    chunk_id = sd.metadata['chunk_id']
                    references = docs_count[chunk_id]
                    pill_options = [f'{references} reference(s)']
                    if docs_provider.get(chunk_id):
                        pill_options.append(docs_provider[chunk_id])
                    with col.popover(f':material/description: {tile_name}', use_container_width=True):
                        st.header(sd.metadata['company_name'])
                        st.subheader(f"Document: [{document_name}]")
                        st.pills('metadata', options=pill_options, label_visibility='hidden', key=str(uuid.uuid4()))
                        page_content = re.sub('\\$', '\\$', sd.page_content)
                        st.markdown(page_content)


def build_css():
    with open('config/app.css', 'r') as f:
        st.markdown("""
        <style>
        {}
        </style>
        """.format(f.read()), unsafe_allow_html=True)


def build_page():
    st.set_page_config(
        page_title="Data Intelligence for FSI data providers",
        layout="wide",
        initial_sidebar_state="expanded"
    )


def build_state():

    if 'model' not in st.session_state.keys():
        st.session_state['model'] = None

    if 'prompt' not in st.session_state.keys():
        with open('config/prompt.txt', 'r') as f:
            st.session_state['prompt'] = f.read()

    if 'history' not in st.session_state.keys():
        st.session_state['history'] = []

    if 'provider' not in st.session_state.keys():
        st.session_state['provider'] = None

    if 'providers' not in st.session_state.keys():
        st.session_state['providers'] = get_available_tools()

    if ('tools' not in st.session_state.keys()) or ('selection' not in st.session_state.keys()):
        tools = list(itertools.chain.from_iterable(st.session_state['providers'].values()))
        st.session_state['tools'] = dict(map(lambda x: [x.tool_id, x], tools))
        st.session_state['selection'] = dict(map(lambda x: [x.tool_id, False], tools))


def build_sidebar():
    st_sidebar = st.sidebar
    st_sidebar.header("Data Intelligence")
    st_sidebar.caption("""General intelligence refers to broad AI capabilities like reasoning, summarization,
    or language generation, whereas **Data Intelligence** combines those capabilities with
    deep understanding of your enterprise’s unique data, metadata, governance rules, and business context.""")
    if st_sidebar.button("See Reference Architecture", key='architecture', type='tertiary', use_container_width=False):
        display_architecture()

    st_sidebar.subheader("Setup")
    st_sidebar.caption("Select data sources and model you want included in your agentic workflow.")
    with st_sidebar.expander('**Step1**: Select Foundation Model'):
        st.caption('Models are available on Databricks **AI gateway**, grounded for regulatory-safe '
                   'practices through output guardrails')
        with st.container(border=False, key='sb_data_intelligence_model'):
            col1, col2 = st.columns([0.2, 0.8])
            col1.image(f'images/anthropic_model.png', width=30)
            col2.write('**Anthropic**')
            st.radio(
                'foundation model',
                options=[
                    'databricks-claude-sonnet-4',
                    'databricks-claude-3-7-sonnet'
                ],
                key='selected_model',
                index=1,
                label_visibility='hidden'
            )

    with st_sidebar.expander('**Step2**: Add Data Intelligence'):
        selection = {}
        st.caption('Governed through **Unity Catalog**, data can be made accessible to AI agent through UC Functions, '
                   'online tables and vector store capabilities')
        for provider_name in st.session_state['providers'].keys():
            with st.container(border=False, key=f'sb_data_intelligence_{provider_name}'):
                provider_tools = st.session_state['providers'][provider_name]
                col1, col2 = st.columns([0.2, 0.8])
                col1.image(f'images/{provider_name}_source.png', width=30)
                col2.write(f'**{provider_name.capitalize()}**')
                username = get_user_info()['user_name']
                for provider_tool in provider_tools:
                    description = provider_tool.tool_description
                    if not provider_tool.tool_enabled:
                        if provider_tool.tool_type == 'genie':
                            description = '{}. Genie MCP is only available through playground. Stay tuned'.format(description)
                        else:
                            description = '{}. User [{}] does not have access to this resource'.format(description, username)
                    cb = st.checkbox(
                        value=provider_tool.tool_enabled,
                        label=provider_tool.tool_name,
                        help=description,
                        disabled=not provider_tool.tool_enabled,
                        label_visibility='visible'
                    )
                    selection[provider_tool.tool_id] = cb

    st_sidebar.divider()
    st_sidebar.subheader("Authors")
    st_sidebar.caption("antoine.amend@databricks.com\nkeon.shahab@databricks.com")
    return selection


def bootstrap_if_needed(selected_tools):
    bootstrap = False
    tools = []
    if 'agent' not in st.session_state.keys():
        bootstrap = True
        print(f"Agent not initialized, bootstrapping model")
    elif st.session_state['selected_model'] != st.session_state['model']:
        bootstrap = True
        print(f"Foundation model changed to [{st.session_state['selected_model']}], bootstrapping model")
    for tool_id in selected_tools.keys():
        if selected_tools[tool_id] != st.session_state['selection'][tool_id]:
            bootstrap = True
            print(f"Tool [{tool_id}] changed state to [{selected_tools[tool_id]}], bootstrapping model")
        if selected_tools[tool_id]:
            tools.append(st.session_state['tools'][tool_id])
    if bootstrap:
        st.session_state['agent'] = build_agent(st.session_state['selected_model'], tools, st.session_state['prompt'])
    st.session_state['selection'] = selected_tools
    st.session_state['model'] = st.session_state['selected_model']


def chat(query):
    # Display user message
    chat_user(st, query)

    # Append question to history
    message_id = str(uuid.uuid4())
    st.session_state['history'].append(ChatAgentMessage(role='user', content=query, id=message_id))

    # calling agent
    with st.spinner(f"reasoning", show_time=True):

        # we pass all history as context, reading output as chunks (streams) as new tools get called
        model_output_stream = st.session_state.agent.predict_stream(messages=st.session_state['history'])
        supporting_docs = {}
        supporting_docs_count = {}
        supporting_docs_provider = {}
        try:
            for event in model_output_stream:
                delta = event.delta

                # Might be a tool calling or the actual response
                if delta.role == 'assistant':
                    if delta.tool_calls and delta.content:
                        # Assistant yields new tool calling, logging as agent
                        chat_agent(st, delta.content, delta.tool_calls)
                    else:
                        # Assistant does not yield further tool call, answering question
                        model_output = clean_response(delta.content)
                        if len(model_output) > 0:
                            # Display assistant response
                            chat_assistant(st, model_output)

                # This is response from a tool calling function
                elif delta.role == 'tool':
                    try:
                        # As vector store, output is langchain document
                        documents = eval(delta.content)
                        if type(documents) is list and type(documents[0]) is Document:
                            # Keeping track of all documents used to formulate response
                            provider = st.session_state['provider']
                            for document in documents:
                                if 'chunk_id' in document.metadata:
                                    chunk_id = document.metadata['chunk_id']
                                    supporting_docs[chunk_id] = document
                                    supporting_docs_count[chunk_id] = supporting_docs_count.get(chunk_id, 0) + 1
                                    supporting_docs_provider[chunk_id] = provider
                    except Exception as e:
                        # Not a langchain document collection
                        pass
        except requests.exceptions.HTTPError as e:
            # Error often comes from our guardrails due to sensitive information being disclosed
            if (e.response.status_code == 400) and (is_guardrail_error(e.response.json())):
                error_data = e.response.json()
                guardrail_info = extract_guardrail_info(error_data)
                model_output = ['[ERROR] Content policy violation detected.', get_friendly_message(guardrail_info)]
                model_output = '\n'.join(model_output)
                # Pretty print error
                chat_assistant(st, model_output)
            else:
                raise e
        except Exception as e:
            # Unknown exception, pretty print error
            model_output = f'[ERROR] Error whilst processing request, {e}'
            chat_assistant(st, model_output)
            raise e

    # Append response to history
    message_id = str(uuid.uuid4())
    st.session_state['history'].append(ChatAgentMessage(role='assistant', content=model_output, id=message_id))

    # Display all documents below assistant response
    display_supporting_docs(supporting_docs, supporting_docs_count, supporting_docs_provider, columns=3)


@st.dialog("Reference Architecture", width='large')
def display_architecture():
    st.image('images/architecture.png')


# Ensure environment variable is set correctly
# assert os.getenv('DATABRICKS_CLIENT_ID'), "DATABRICKS_CLIENT_ID must be set"
# assert os.getenv('DATABRICKS_CLIENT_SECRET'), "DATABRICKS_CLIENT_ID must be set"
assert os.getenv('DATABRICKS_HOST'), "DATABRICKS_HOST must be set"

if 'http' not in os.environ['DATABRICKS_HOST']:
    os.environ['DATABRICKS_HOST'] = f"https://{os.environ['DATABRICKS_HOST']}"

# Page configuration
build_page()

# Custom CSS for better styling
build_css()

# Add status variables
build_state()

# Build sidebar
tool_selection = build_sidebar()

# Build header
st.image('images/header.png')
st.caption('''Databricks Data Intelligence helps you monitor and assess portfolio risk by combining internal
holdings data with external insights like earnings filings, news and market data.
Powered by **Databricks Data Intelligence**, **Anthropic** and supported by multiple data providers such as 
**Factset**, it can surface meaningful signals, links them to your positions, and suggests where deeper analysis or 
action may be needed with full traceability to the 
data source.''')

# Build chat
prompt = st.chat_input("Ask me anything about your portfolio")
display_history(st, st.session_state['history'])

# Reload model if needed
with st.spinner('Bootstrapping agent', show_time=True):
    bootstrap_if_needed(tool_selection)

# Chat
st.session_state.setdefault("selection_prompt", None)
if st.session_state.selection_prompt:
    chat(st.session_state.selection_prompt)
    st.session_state.selection_prompt = None

if prompt:
    chat(prompt)

# Pre-canned questions we know work
# Always show selection on the bottom of page, after previous response
selection_prompt = [
    ":material/stars_2: What tools do you have at your disposal?",
    ":material/stars_2: Do I have any exposure to manufacturing companies?",
    ":material/stars_2: Do I have any positions in companies involved in anti-PD-1 therapy?",
    ":material/stars_2: What are top performing stocks in my portfolio?",
    ":material/stars_2: What is my exposure to Italy?",
]

if len(st.session_state['history']) > 0:
    selection_prompt.append(":material/stars_2: Consolidate above analysis in a nicely formatted document")

build_pills(selection_prompt)
