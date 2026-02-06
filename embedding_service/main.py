from __future__ import annotations

import os
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np
from FlagEmbedding import BGEM3FlagModel

load_dotenv()

MODEL_NAME = os.getenv("BGE_M3_MODEL", "BAAI/bge-m3")
USE_FP16 = os.getenv("BGE_M3_FP16", "false").lower() == "true"

app = FastAPI(title="Embedding Service")


class EmbeddingModel:
    def __init__(self, model_name: str = "BAAI/bge-m3", use_fp16: bool = False) -> None:
        # Mac CPU: fp16 may be unstable, default to fp32
        self.model = BGEM3FlagModel(
            model_name,
            use_fp16=use_fp16,
        )

    def encode(self, texts: list[str], mode: str = "dense") -> dict:
        output = self.model.encode(
            texts,
            return_dense=mode in ("dense", "all"),
            return_sparse=mode in ("sparse", "all"),
            return_colbert_vecs=mode in ("colbert", "all"),
        )

        result: dict = {}

        if mode in ("dense", "all"):
            dense = []
            for v in output["dense_vecs"]:
                v = v.astype(np.float32)
                dense.append(v.tolist())
            result["dense"] = dense

        if mode in ("sparse", "all"):
            lexical = output.get("lexical_weights") or []
            sparse = []

            for i in range(len(texts)):
                weights = lexical[i] if i < len(lexical) else {}
                indices = list(map(int, weights.keys()))
                values = list(map(float, weights.values()))
                sparse.append({"indices": indices, "values": values})

            result["sparse"] = sparse

        if mode in ("colbert", "all"):
            colbert = []
            for mat in output["colbert_vecs"]:
                colbert.append(mat.astype(np.float32).tolist())
            result["colbert"] = colbert

        return result


model = EmbeddingModel(MODEL_NAME, use_fp16=USE_FP16)


class EmbedRequest(BaseModel):
    texts: List[str]
    mode: str | None = "dense"


@app.post("/embed")
def embed(req: EmbedRequest):
    result = model.encode(req.texts, mode=req.mode or "dense")
    # Keep backward compatibility with previous API: return vectors for dense mode
    dense = result.get("dense", [])
    return {"vectors": dense, "dim": len(dense[0]) if dense else 0, "result": result}


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL_NAME}
