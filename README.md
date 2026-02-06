# Agentic RAG (ADK + OpenAI + Milvus + Postgres + BGE‑M3)

## Quickstart

### 1. Install dependencies (uv)
```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

### 2. Start infrastructure
```bash
docker-compose up -d
```

### 3. Configure environment
```bash
cp .env.example .env
# fill in OPENAI_API_KEY and API_KEY
```

### 4. Run migrations
```bash
alembic upgrade head
```

### 5. Start Embedding service
```bash
uvicorn embedding_service.main:app --reload --host 0.0.0.0 --port 8001
```

### 6. Start API service
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 7. Open UI
- http://localhost:8000/ui

## CopilotKit AG-UI Frontend

This frontend uses CopilotKit and connects to the AG-UI streaming endpoint `/agui/run`.

```bash
cd frontend
npm install
cp .env.example .env.local
npm run dev
```

Then open: http://localhost:3000

## API

### POST /index
```json
{ "path": "/abs/path/to/docs" }
```

### POST /index/finqa
```json
{ "path": "/abs/path/to/finqa" }
```
This will also store FinQA questions/answers into Postgres table `finqa_qa` for evaluation.

### POST /chat
```json
{ "session_id": "optional", "message": "你的问题" }
```

Required header: `X-API-Key`

### POST /agui/run
AG-UI protocol streaming endpoint (used by CopilotKit frontend).

## Notes
- PDF/DOCX only (no OCR)
- BGE‑M3 is loaded in the embedding service (CPU)
- Milvus stores vectors; Postgres stores metadata + sessions
