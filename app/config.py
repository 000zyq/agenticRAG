from __future__ import annotations

import os
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


class Settings(BaseModel):
    openai_api_key: str = Field(..., alias="OPENAI_API_KEY")
    openai_base_url: str | None = Field(None, alias="OPENAI_BASE_URL")
    openai_chat_model: str = Field("gpt-4.1", alias="OPENAI_CHAT_MODEL")
    openai_rerank_model: str = Field("gpt-4.1", alias="OPENAI_RERANK_MODEL")
    api_key: str = Field(..., alias="API_KEY")

    milvus_uri: str = Field("http://localhost:19530", alias="MILVUS_URI")
    milvus_token: str | None = Field(None, alias="MILVUS_TOKEN")
    milvus_collection: str = Field("rag_chunks", alias="MILVUS_COLLECTION")

    postgres_dsn: str = Field(..., alias="POSTGRES_DSN")

    embedding_url: str = Field("http://localhost:8001/embed", alias="EMBEDDING_URL")
    embedding_dim: int = Field(1024, alias="EMBEDDING_DIM")
    embedding_batch_size: int = Field(32, alias="EMBEDDING_BATCH_SIZE")
    embedding_concurrency: int = Field(2, alias="EMBEDDING_CONCURRENCY")

    milvus_insert_batch: int = Field(200, alias="MILVUS_INSERT_BATCH")

    chunk_strategy: str = Field("recursive", alias="CHUNK_STRATEGY")
    chunk_size_tokens: int = Field(800, alias="CHUNK_SIZE_TOKENS")
    chunk_overlap_tokens: int = Field(100, alias="CHUNK_OVERLAP_TOKENS")

    retrieval_top_n: int = Field(20, alias="RETRIEVAL_TOP_N")
    retrieval_top_k: int = Field(5, alias="RETRIEVAL_TOP_K")

    host: str = Field("0.0.0.0", alias="HOST")
    port: int = Field(8000, alias="PORT")

    embedding_host: str = Field("0.0.0.0", alias="EMBEDDING_HOST")
    embedding_port: int = Field(8001, alias="EMBEDDING_PORT")

    class Config:
        populate_by_name = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(**os.environ)
