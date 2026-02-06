from __future__ import annotations

from app.agents.retriever import retrieve_contexts
from app.agents.answerer import generate_answer
from app.storage.repository import ensure_session, append_message


def run_chat(session_id: str, message: str) -> dict:
    ensure_session(session_id)
    append_message(session_id, "user", message)
    contexts = retrieve_contexts(message)
    result = generate_answer(message, contexts)

    append_message(session_id, "assistant", result["answer"])

    return {
        "session_id": session_id,
        "answer": result["answer"],
        "citations": result["citations"],
    }
