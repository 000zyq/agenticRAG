from __future__ import annotations

import os

from app.retrieval.search import search_docs

try:
    from google.adk.agents import LlmAgent
    from google.adk.tools import FunctionTool
except Exception:  # pragma: no cover - optional at runtime
    LlmAgent = None
    FunctionTool = None


def build_adk_agents():
    """Define ADK agents for optional ADK UI usage."""
    if LlmAgent is None or FunctionTool is None:
        return None

    search_tool = FunctionTool.from_fn(search_docs) if hasattr(FunctionTool, "from_fn") else FunctionTool(search_docs)

    retriever = LlmAgent(
        name="retriever",
        model=os.getenv("OPENAI_CHAT_MODEL", "gpt-4.1"),
        instruction="Retrieve relevant passages using search_docs tool.",
        tools=[search_tool],
    )

    coordinator = LlmAgent(
        name="coordinator",
        model=os.getenv("OPENAI_CHAT_MODEL", "gpt-4.1"),
        instruction="Coordinate user requests and call retriever as needed.",
        sub_agents=[retriever],
    )

    return coordinator
