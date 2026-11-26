from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.deps import verify_api_key
from app.graph.query import run_graph_query

router = APIRouter()


class GraphQueryRequest(BaseModel):
    mode: Literal["entity_subgraph", "relation_search"]
    query: str | None = None
    subject: str | None = None
    predicate: str | None = None
    object: str | None = Field(default=None)
    doc_id: str | None = None
    depth: int = 1
    limit: int = 20
    entity_types: list[str] | None = None
    relation_families: list[str] | None = None


@router.post("/graph/query")
def graph_query(req: GraphQueryRequest, _: None = Depends(verify_api_key)):
    if req.mode == "entity_subgraph" and not (req.query or "").strip():
        raise HTTPException(status_code=400, detail="query is required for entity_subgraph mode")
    if req.mode == "relation_search" and not (req.subject or "").strip():
        raise HTTPException(status_code=400, detail="subject is required for relation_search mode")
    return run_graph_query(
        mode=req.mode,
        query=req.query,
        subject=req.subject,
        predicate=req.predicate,
        object_text=req.object,
        doc_id=req.doc_id,
        depth=req.depth,
        limit=req.limit,
        entity_types=req.entity_types,
        relation_families=req.relation_families,
    )
