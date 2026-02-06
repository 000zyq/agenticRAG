from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from ag_ui.core import (
    RunStartedEvent,
    RunFinishedEvent,
    RunErrorEvent,
    StepStartedEvent,
    StepFinishedEvent,
    ToolCallStartEvent,
    ToolCallArgsEvent,
    ToolCallEndEvent,
    ToolCallResultEvent,
    TextMessageStartEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
)
from ag_ui.encoder import EventEncoder

from app.agents.answerer import stream_answer
from app.ingest.embedding_client import embed_texts
from app.retrieval.milvus_client import search as milvus_search
from app.retrieval.rerank import rerank
from app.config import get_settings

router = APIRouter()


def _extract_query(payload: dict) -> str:
    if "message" in payload and isinstance(payload["message"], str):
        return payload["message"].strip()

    messages = payload.get("messages") or []
    for msg in reversed(messages):
        role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", None)
        if role != "user":
            continue
        content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", None)
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            texts = []
            for item in content:
                if isinstance(item, dict):
                    if item.get("type") == "text":
                        texts.append(item.get("text", ""))
                elif isinstance(item, str):
                    texts.append(item)
            joined = " ".join([t for t in texts if t])
            if joined:
                return joined.strip()
    return ""


def _format_results(
    results: list[dict],
    include_vector_score: bool = False,
    include_rerank_score: bool = False,
) -> list[dict]:
    formatted = []
    for r in results:
        item = {
            "source_path": r.get("source_path"),
            "page": r.get("page"),
            "snippet": (r.get("text") or "")[:240],
        }
        if include_vector_score:
            item["vector_score"] = r.get("score")
        if include_rerank_score:
            item["rerank_score"] = r.get("rerank_score")
        formatted.append(item)
    return formatted


@router.post("/agui/run")
async def agui_run(request: Request):
    payload = await request.json()

    thread_id = payload.get("threadId") or payload.get("thread_id") or str(uuid.uuid4())
    run_id = payload.get("runId") or payload.get("run_id") or str(uuid.uuid4())
    query = _extract_query(payload)

    encoder = EventEncoder()

    async def event_stream():
        try:
            progress_message_id = str(uuid.uuid4())
            yield encoder.encode(
                RunStartedEvent(thread_id=thread_id, run_id=run_id, input=payload)
            )
            yield encoder.encode(
                TextMessageStartEvent(message_id=progress_message_id, role="assistant")
            )
            yield encoder.encode(
                TextMessageContentEvent(
                    message_id=progress_message_id, delta="进度：开始处理请求...\n"
                )
            )
            await asyncio.sleep(0)

            settings = get_settings()

            # Step: retrieve
            yield encoder.encode(StepStartedEvent(step_name="retrieve"))
            yield encoder.encode(
                TextMessageContentEvent(
                    message_id=progress_message_id, delta="步骤1/3 检索：开始\n"
                )
            )

            tool_call_id = str(uuid.uuid4())
            tool_message_id = str(uuid.uuid4())
            args_json = json.dumps(
                {
                    "query": query,
                    "top_n": settings.retrieval_top_n,
                    "top_k": settings.retrieval_top_k,
                }
            )

            yield encoder.encode(
                ToolCallStartEvent(
                    tool_call_id=tool_call_id,
                    tool_call_name="search_docs",
                    parent_message_id=tool_message_id,
                )
            )
            yield encoder.encode(ToolCallArgsEvent(tool_call_id=tool_call_id, delta=args_json))

            embeddings = embed_texts([query]) if query else []
            if embeddings:
                embedding = embeddings[0]
                candidates = milvus_search(embedding, settings.retrieval_top_n)
            else:
                candidates = []
            formatted_candidates = _format_results(
                candidates, include_vector_score=True, include_rerank_score=False
            )

            yield encoder.encode(ToolCallEndEvent(tool_call_id=tool_call_id))
            yield encoder.encode(
                ToolCallResultEvent(
                    message_id=tool_message_id,
                    tool_call_id=tool_call_id,
                    content=json.dumps(
                        {
                            "candidates": formatted_candidates,
                        },
                        ensure_ascii=False,
                    ),
                    role="tool",
                )
            )

            yield encoder.encode(StepFinishedEvent(step_name="retrieve"))
            yield encoder.encode(
                TextMessageContentEvent(
                    message_id=progress_message_id,
                    delta=f"检索完成：候选 {len(formatted_candidates)} 条\n",
                )
            )
            for idx, item in enumerate(formatted_candidates[:5], start=1):
                score = item.get("vector_score")
                score_text = f"{score:.4f}" if isinstance(score, (int, float)) else "n/a"
                yield encoder.encode(
                    TextMessageContentEvent(
                        message_id=progress_message_id,
                        delta=(
                            f"- [{idx}] {item.get('source_path')} p.{item.get('page')} "
                            f"(向量分数 {score_text})\n"
                        ),
                    )
                )

            # Step: rerank
            yield encoder.encode(StepStartedEvent(step_name="rerank"))
            yield encoder.encode(
                TextMessageContentEvent(
                    message_id=progress_message_id, delta="步骤2/3 重排：开始\n"
                )
            )

            rerank_tool_id = str(uuid.uuid4())
            rerank_message_id = str(uuid.uuid4())
            rerank_args = json.dumps(
                {
                    "query": query,
                    "candidates": len(candidates),
                    "top_k": settings.retrieval_top_k,
                }
            )

            yield encoder.encode(
                ToolCallStartEvent(
                    tool_call_id=rerank_tool_id,
                    tool_call_name="rerank",
                    parent_message_id=rerank_message_id,
                )
            )
            yield encoder.encode(
                ToolCallArgsEvent(tool_call_id=rerank_tool_id, delta=rerank_args)
            )

            reranked = rerank(query, candidates)
            top_k = reranked[: settings.retrieval_top_k]
            formatted_rerank = _format_results(
                top_k, include_vector_score=True, include_rerank_score=True
            )

            yield encoder.encode(ToolCallEndEvent(tool_call_id=rerank_tool_id))
            yield encoder.encode(
                ToolCallResultEvent(
                    message_id=rerank_message_id,
                    tool_call_id=rerank_tool_id,
                    content=json.dumps({"results": formatted_rerank}, ensure_ascii=False),
                    role="tool",
                )
            )

            yield encoder.encode(StepFinishedEvent(step_name="rerank"))
            yield encoder.encode(
                TextMessageContentEvent(
                    message_id=progress_message_id,
                    delta=f"重排完成：TopK {len(formatted_rerank)} 条\n",
                )
            )
            for idx, item in enumerate(formatted_rerank[:5], start=1):
                score = item.get("rerank_score")
                score_text = f"{score:.2f}" if isinstance(score, (int, float)) else "n/a"
                yield encoder.encode(
                    TextMessageContentEvent(
                        message_id=progress_message_id,
                        delta=(
                            f"- [{idx}] {item.get('source_path')} p.{item.get('page')} "
                            f"(重排分数 {score_text})\n"
                        ),
                    )
                )

            # Step: answer
            yield encoder.encode(StepStartedEvent(step_name="answer"))
            yield encoder.encode(
                TextMessageContentEvent(
                    message_id=progress_message_id, delta="步骤3/3 生成回答：开始\n"
                )
            )

            answer_tool_id = str(uuid.uuid4())
            answer_message_id = str(uuid.uuid4())
            answer_args = json.dumps({"model": settings.openai_chat_model})

            yield encoder.encode(
                ToolCallStartEvent(
                    tool_call_id=answer_tool_id,
                    tool_call_name="generate_answer",
                    parent_message_id=answer_message_id,
                )
            )
            yield encoder.encode(
                ToolCallArgsEvent(tool_call_id=answer_tool_id, delta=answer_args)
            )

            yield encoder.encode(TextMessageEndEvent(message_id=progress_message_id))

            msg_id = str(uuid.uuid4())
            yield encoder.encode(TextMessageStartEvent(message_id=msg_id, role="assistant"))

            for delta in stream_answer(query, top_k):
                yield encoder.encode(TextMessageContentEvent(message_id=msg_id, delta=delta))

            yield encoder.encode(TextMessageEndEvent(message_id=msg_id))

            yield encoder.encode(ToolCallEndEvent(tool_call_id=answer_tool_id))
            yield encoder.encode(
                ToolCallResultEvent(
                    message_id=answer_message_id,
                    tool_call_id=answer_tool_id,
                    content=json.dumps({"status": "complete"}, ensure_ascii=False),
                    role="tool",
                )
            )
            yield encoder.encode(StepFinishedEvent(step_name="answer"))

            citations = [
                {
                    "source_path": r.get("source_path"),
                    "page": r.get("page"),
                    "snippet": (r.get("text") or "")[:200],
                }
                for r in top_k
            ]

            citations_tool_id = str(uuid.uuid4())
            citations_message_id = str(uuid.uuid4())
            yield encoder.encode(
                ToolCallStartEvent(
                    tool_call_id=citations_tool_id,
                    tool_call_name="citations",
                    parent_message_id=citations_message_id,
                )
            )
            yield encoder.encode(
                ToolCallArgsEvent(
                    tool_call_id=citations_tool_id,
                    delta=json.dumps({"count": len(citations)}),
                )
            )
            yield encoder.encode(ToolCallEndEvent(tool_call_id=citations_tool_id))
            yield encoder.encode(
                ToolCallResultEvent(
                    message_id=citations_message_id,
                    tool_call_id=citations_tool_id,
                    content=json.dumps({"citations": citations}, ensure_ascii=False),
                    role="tool",
                )
            )

            yield encoder.encode(RunFinishedEvent(thread_id=thread_id, run_id=run_id, result=None))
        except Exception as exc:
            yield encoder.encode(RunErrorEvent(message=str(exc)))

    return StreamingResponse(event_stream(), media_type="text/event-stream")
