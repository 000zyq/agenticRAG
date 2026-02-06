"use client";

import { useMemo } from "react";
import { CopilotChat } from "@copilotkit/react-ui";
import { useCopilotChatInternal } from "@copilotkit/react-core";

function parseToolResult(result: unknown) {
  if (!result) return null;
  if (typeof result === "string") {
    try {
      return JSON.parse(result);
    } catch {
      return null;
    }
  }
  if (typeof result === "object") return result as Record<string, any>;
  return null;
}

type StepStatus = "pending" | "active" | "complete";

function ProgressBoard() {
  const chat = useCopilotChatInternal();
  const messages = Array.isArray(chat?.messages) ? chat.messages : [];
  const isLoading = Boolean(chat?.isLoading);

  const view = useMemo(() => {
    let latestUserIndex = -1;
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      if (messages[i].role === "user") {
        latestUserIndex = i;
        break;
      }
    }

    const runMessages = latestUserIndex >= 0 ? messages.slice(latestUserIndex + 1) : [];
    const toolCalls = runMessages.flatMap((m: any) => m.toolCalls || []);
    const toolMessages = runMessages.filter((m: any) => m.role === "tool");
    const latestUser = latestUserIndex >= 0 ? messages[latestUserIndex] : null;
    const queryText = typeof latestUser?.content === "string" ? latestUser.content : "";

    const toolMap = (toolName: string) => {
      const call = toolCalls.find((tc: any) => tc?.function?.name === toolName);
      const toolMessage = call
        ? toolMessages.find((m: any) => m.toolCallId === call.id)
        : undefined;
      const result = toolMessage ? parseToolResult(toolMessage.content) : null;
      return { call, toolMessage, result };
    };

    const statusFor = (toolName: string): StepStatus => {
      const { call, toolMessage } = toolMap(toolName);
      if (toolMessage) return "complete";
      if (call) return "active";
      return isLoading ? "pending" : "pending";
    };

    return {
      queryText,
      statusFor,
      toolMap,
      hasUser: latestUserIndex >= 0,
    };
  }, [messages, isLoading]);

  const steps = [
    {
      key: "request",
      label: "Request Parsed",
      status: view.hasUser ? "complete" : "pending",
      detail: view.queryText ? [`Query: ${view.queryText}`] : [],
    },
    {
      key: "search_docs",
      label: "检索候选",
      status: view.statusFor("search_docs"),
      detail: (() => {
        const { result } = view.toolMap("search_docs");
        const candidates = result?.candidates || result?.results || [];
        if (!candidates.length) return ["等待检索结果..."];
        return candidates.slice(0, 5).map((r: any, idx: number) => {
          const score =
            typeof r.vector_score === "number" ? r.vector_score.toFixed(4) : "n/a";
          return `${idx + 1}. ${r.source_path} p.${r.page} (向量 ${score})`;
        });
      })(),
    },
    {
      key: "rerank",
      label: "重排筛选",
      status: view.statusFor("rerank"),
      detail: (() => {
        const { result } = view.toolMap("rerank");
        const results = result?.results || [];
        if (!results.length) return ["等待重排结果..."];
        return results.slice(0, 5).map((r: any, idx: number) => {
          const score =
            typeof r.rerank_score === "number" ? r.rerank_score.toFixed(2) : "n/a";
          return `${idx + 1}. ${r.source_path} p.${r.page} (重排 ${score})`;
        });
      })(),
    },
    {
      key: "generate_answer",
      label: "生成回答",
      status: view.statusFor("generate_answer"),
      detail: ["正在生成答案内容..."],
    },
    {
      key: "citations",
      label: "引用整理",
      status: view.statusFor("citations"),
      detail: (() => {
        const { result } = view.toolMap("citations");
        const citations = result?.citations || [];
        if (!citations.length) return ["等待引用信息..."];
        return citations.slice(0, 5).map((c: any, idx: number) => {
          return `${idx + 1}. ${c.source_path} p.${c.page}`;
        });
      })(),
    },
  ];

  const completedCount = steps.filter((s) => s.status === "complete").length;
  const progress = Math.round((completedCount / steps.length) * 100);

  return (
    <section className="progress-board">
      <div className="progress-header">
        <div>
          <h2>RAG 执行进度</h2>
          <p>实时展示检索、重排与生成过程</p>
        </div>
        <div className="progress-summary">
          <span>
            {completedCount}/{steps.length} complete
          </span>
          <div className="progress-meter">
            <div className="progress-meter-fill" style={{ width: `${progress}%` }} />
          </div>
        </div>
      </div>

      <div className="progress-steps">
        {steps.map((step, idx) => (
          <div key={step.key} className={`progress-step ${step.status}`}>
            <div className="step-head">
              <div className="step-index">{idx + 1}</div>
              <div className="step-title">
                <div>{step.label}</div>
                <span className={`step-status ${step.status}`}>
                  {step.status === "complete"
                    ? "complete"
                    : step.status === "active"
                      ? "in progress"
                      : "pending"}
                </span>
              </div>
            </div>
            <div className="step-body">
              {step.detail.map((line, lineIdx) => (
                <div key={lineIdx} className="step-line">
                  {line}
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

export default function Page() {
  return (
    <main className="page">
      <header className="header">
        <h1>Agentic RAG</h1>
        <p>AG-UI + CopilotKit：实时进度与检索结果展示</p>
      </header>

      <div className="workspace">
        <ProgressBoard />
        <div className="chat-panel">
          <CopilotChat
            className="chat"
            labels={{
              title: "RAG Assistant",
              initial: "你好！我可以帮助你检索财报并回答问题。",
            }}
          />
        </div>
      </div>
    </main>
  );
}
