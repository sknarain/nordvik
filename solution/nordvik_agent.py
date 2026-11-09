import os
import json
from dotenv import load_dotenv
from openai import OpenAI
import gradio as gr
from langchain_core.messages import SystemMessage, HumanMessage, ToolMessage
from langchain_ollama import ChatOllama
from tools import get_accounts_data, search_docs, below_target_margin_contracts
# Always remember to do this!
load_dotenv(override=True)

#OLLAMA_BASE_URL = "http://localhost:11434/v1"

model_name = "llama3.2:latest"


def chat(message, history):
    history.append(HumanMessage(content=message))
    #messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": message}]

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=message)
    ]
    ai_mesg = llm_with_tools.invoke(messages)
    tool_map = {"get_accounts_data": get_accounts_data, "search_docs": search_docs, "below_target_margin_contracts": below_target_margin_contracts}

    if ai_mesg.tool_calls:
       for tool_call in ai_mesg.tool_calls:
           # Select the function and run it 
           selected_tool = tool_map[tool_call["name"]]
           if tool_call["name"] == "search_docs":
              tool_output = selected_tool.invoke(tool_call["args"])
           else:
              tool_output = selected_tool.invoke(tool_call)
        
           # Format the output into a ToolMessage linked by the specific tool_call_id
           messages.append(
               ToolMessage(
                  content=str(tool_output),
                  tool_call_id=tool_call["id"]
               )   
           )   

       response = llm_with_tools.invoke(messages)
       return response.content

system_prompt = f"""

# Your role

You are a senior data analyst, chatting with visitors of the website.
You answer questions related to sales revenue, open opportunities, service tickets, QBR, and accounts.

If asked about you, explain clearly that you are an AI chat agent for Nordvik.


Use tool get_accounts_data questions related to sales revenue, open opportunities, service tickets.
Use tool search_docs for questions related to quartery review, QBR, and engineering change requests.
Use tool below_target_margin_contracts for questions related to contracts, margin, below target.
All amounts are in Euro and should not be converted.
Do not agregate any data. 

# Rules

Engage with the user. Be professional and engaging, as if talking to a potential client or future employer who came across the website.

Provide detailed answers using the data  provided the tool.

IMPORTANT: If you don't know the answer, say so. Never make up an answer.
If the user asks a question that cannot be answered using the tools, say that you don't know.

Always mention the source of the information.
"""


# Tools used by the agent
tools = [get_accounts_data, search_docs, below_target_margin_contracts]

llm = ChatOllama(
    model="llama3.2",
    temperature=0.7,
)

llm_with_tools = llm.bind_tools(tools)
query = "What is our total revenue, open pipeline and open service tickets for a given supermarket chain across all countries?"
query = "What did we commit to a customer in the last four quarterly reviews, and which of those commitments are still open?"

gr.ChatInterface(
   chat,
   title="Nordvik AI Agent",
   description="Ask any question related to Nordvik sales, accounts, and customers",
   ).launch(inbrowser=True)
