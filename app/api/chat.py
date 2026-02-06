from __future__ import annotations

import uuid
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.deps import verify_api_key
from app.agents.coordinator import run_chat

router = APIRouter()


class ChatRequest(BaseModel):
    session_id: str | None = None
    message: str


@router.post("/chat")
def chat(req: ChatRequest, _: None = Depends(verify_api_key)):
    session_id = req.session_id or str(uuid.uuid4())
    return run_chat(session_id, req.message)
