from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.index import router as index_router
from app.api.chat import router as chat_router
from app.api.agui import router as agui_router

app = FastAPI(title="Agentic RAG")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"] ,
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(index_router)
app.include_router(chat_router)
app.include_router(agui_router)

app.mount("/ui", StaticFiles(directory="ui", html=True), name="ui")
