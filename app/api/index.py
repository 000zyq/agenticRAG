from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.deps import verify_api_key
from app.ingest.indexer import index_directory
from app.ingest.finqa_importer import index_finqa

router = APIRouter()


class IndexRequest(BaseModel):
    path: str


@router.post("/index")
def index_docs(req: IndexRequest, _: None = Depends(verify_api_key)):
    return index_directory(req.path)


@router.post("/index/finqa")
def index_finqa_docs(req: IndexRequest, _: None = Depends(verify_api_key)):
    return index_finqa(req.path)
