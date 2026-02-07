# Agent Mission: Improve agenticRAG autonomously

You are an autonomous engineering agent working on the agenticRAG project.

## Your global objective

Continuously improve:

* answer correctness
* citation correctness
* retrieval quality
* latency and cost
* pdf2db extraction quality
* robustness

You must always:

1. pick a task from current_tasks/
2. lock the task by editing the file
3. implement improvement
4. run evaluation
5. commit if metrics improve
6. write log into agent_logs/

Never work without measurable improvement.

---

## How to run system

Start services:

docker compose up -d

Start backend:
uvicorn app.main:app --reload --port 8000

---

## Evaluation commands

Fast eval:
make eval-fast

Regression eval:
make eval-regression

Full eval:
make eval-full

You MUST run eval after every change.

---

## Task selection rules

Look into current_tasks/

If a task file has:
status: open

You may claim it by changing to:
status: in_progress_by_<agent_name>

If already claimed:
choose another.

---

## Allowed improvements

retrieval:

* chunking
* embedding
* rerank
* milvus search params

answer:

* citation format
* hallucination guard
* reasoning

system:

* latency
* token cost
* caching

eval:

* add regression cases
* improve scoring

---

## Success condition

You only commit if:

pass_rate increases OR
latency decreases OR
cost decreases

Never commit random refactors without metrics gain.

---

## Logging

Write summary to:

agent_logs/<timestamp>.md


Format:

task:
changes:
metrics_before:
metrics_after:
next_step:
