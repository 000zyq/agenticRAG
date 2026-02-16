"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
type FactType = "flow" | "stock";

type DiscrepancyCandidate = {
  candidate_id: number;
  engine: string;
  value: string | null;
  quality_score: string | null;
  source_page: number | null;
  raw_label: string | null;
  raw_value: string | null;
  column_label: string | null;
};

type DiscrepancyItem = {
  fact_type: FactType;
  group_id: string;
  key: Record<string, any>;
  engine_values: Record<string, string[]>;
  pages: number[];
  candidates: DiscrepancyCandidate[];
  resolution: {
    fact_id: number | null;
    value: string | null;
    selected_candidate_id: number | null;
    resolution_status: string | null;
    resolution_method: string | null;
    reviewed_by: string | null;
    reviewed_at: string | null;
    review_notes: string | null;
  };
};

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
      const toolMessage = call ? toolMessages.find((m: any) => m.toolCallId === call.id) : undefined;
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
          const score = typeof r.vector_score === "number" ? r.vector_score.toFixed(4) : "n/a";
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
          const score = typeof r.rerank_score === "number" ? r.rerank_score.toFixed(2) : "n/a";
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
                  {step.status === "complete" ? "complete" : step.status === "active" ? "in progress" : "pending"}
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

function DiscrepancyResolver() {
  const backendBase = process.env.NEXT_PUBLIC_BACKEND_URL || "http://127.0.0.1:8000";

  const [reportIdInput, setReportIdInput] = useState("2");
  const [factType, setFactType] = useState<"all" | FactType>("all");
  const [fiscalYearInput, setFiscalYearInput] = useState("");
  const [periodFilter, setPeriodFilter] = useState<"all" | "annual" | "q1" | "q2" | "q3" | "q4">("annual");
  const [items, setItems] = useState<DiscrepancyItem[]>([]);
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [activePage, setActivePage] = useState(1);
  const [loadedReportId, setLoadedReportId] = useState<number | null>(null);
  const [reviewedBy, setReviewedBy] = useState("");
  const [notesByGroup, setNotesByGroup] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [resolvingId, setResolvingId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const itemRefs = useRef<Array<HTMLElement | null>>([]);

  const reportId = Number(reportIdInput);

  useEffect(() => {
    if (items.length === 0) return;
    if (selectedIdx < 0 || selectedIdx >= items.length) {
      setSelectedIdx(0);
    }
  }, [items.length, selectedIdx]);

  useEffect(() => {
    if (selectedIdx < 0) return;
    const node = itemRefs.current[selectedIdx];
    if (node) {
      node.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }, [selectedIdx]);

  const selectIndex = useCallback(
    (idx: number) => {
      if (idx < 0 || idx >= items.length) return;
      setSelectedIdx(idx);
      const pages = items[idx]?.pages || [];
      if (pages.length > 0) {
        setActivePage(pages[0]);
      }
    },
    [items],
  );

  const loadDiscrepancies = useCallback(async () => {
    if (!Number.isFinite(reportId) || reportId <= 0) {
      setError("report_id 不合法");
      return;
    }
    const parsedYear = Number(fiscalYearInput);
    const fiscalYear = Number.isFinite(parsedYear) && parsedYear >= 1900 && parsedYear <= 2100 ? Math.trunc(parsedYear) : null;
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        report_id: String(reportId),
        fact_type: factType,
        period: periodFilter,
        limit: "200",
      });
      if (fiscalYear !== null) {
        params.set("fiscal_year", String(fiscalYear));
      }
      const url = `${backendBase}/fact-review/discrepancies?${params.toString()}`;
      const resp = await fetch(url);
      if (!resp.ok) {
        throw new Error(`加载失败: ${resp.status}`);
      }
      const data = await resp.json();
      const loadedItems = (data.items || []) as DiscrepancyItem[];
      setItems(loadedItems);
      setLoadedReportId(reportId);
      setSelectedIdx(0);
      if (loadedItems.length > 0) {
        const pages = loadedItems[0].pages || [];
        setActivePage(pages.length > 0 ? pages[0] : 1);
      }
    } catch (e) {
      const message = e instanceof Error ? e.message : "unknown error";
      setError(message);
      setItems([]);
      setLoadedReportId(null);
    } finally {
      setLoading(false);
    }
  }, [backendBase, factType, fiscalYearInput, periodFilter, reportId]);

  const resolveCandidate = useCallback(
    async (item: DiscrepancyItem, candidateId: number) => {
      setResolvingId(candidateId);
      setError(null);
      try {
        const resp = await fetch(`${backendBase}/fact-review/resolve`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            fact_type: item.fact_type,
            candidate_id: candidateId,
            report_id: reportId,
            reviewed_by: reviewedBy || null,
            review_notes: notesByGroup[item.group_id] || null,
          }),
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || `提交失败: ${resp.status}`);
        }
        setItems((prev) =>
          prev.map((g) =>
            g.group_id === item.group_id
              ? {
                  ...g,
                  resolution: {
                    ...g.resolution,
                    selected_candidate_id: candidateId,
                    resolution_status: "verified",
                    resolution_method: "manual",
                    reviewed_by: reviewedBy || null,
                    reviewed_at: new Date().toISOString(),
                    review_notes: notesByGroup[item.group_id] || null,
                  },
                }
              : g,
          ),
        );
      } catch (e) {
        const message = e instanceof Error ? e.message : "unknown error";
        setError(message);
      } finally {
        setResolvingId(null);
      }
    },
    [backendBase, notesByGroup, reportId, reviewedBy],
  );

  const nextUnresolved = useCallback(() => {
    if (items.length === 0) return;
    const start = (selectedIdx + 1) % items.length;
    for (let offset = 0; offset < items.length; offset += 1) {
      const idx = (start + offset) % items.length;
      if (items[idx].resolution?.resolution_method !== "manual") {
        selectIndex(idx);
        return;
      }
    }
    selectIndex(start);
  }, [items, selectIndex, selectedIdx]);

  const pdfSrc = loadedReportId
    ? `/api/report-file/${loadedReportId}?page=${Math.max(activePage, 1)}#page=${Math.max(activePage, 1)}&zoom=page-width`
    : "";

  const unresolvedCount = items.filter((x) => x.resolution?.resolution_method !== "manual").length;

  return (
    <section className="reconcile-panel">
      <div className="reconcile-header">
        <div>
          <h2>Engine 冲突人工裁决</h2>
          <p>左侧选冲突并裁决，右侧自动同步到对应 PDF 页。</p>
        </div>
        <div className="reconcile-controls">
          <input value={reportIdInput} onChange={(e) => setReportIdInput(e.target.value)} placeholder="report_id" />
          <select value={factType} onChange={(e) => setFactType(e.target.value as "all" | FactType)}>
            <option value="all">all</option>
            <option value="flow">flow</option>
            <option value="stock">stock</option>
          </select>
          <input value={fiscalYearInput} onChange={(e) => setFiscalYearInput(e.target.value)} placeholder="fiscal_year" />
          <button type="button" onClick={() => setFiscalYearInput("")}>Clear Year</button>
          <select value={periodFilter} onChange={(e) => setPeriodFilter(e.target.value as "all" | "annual" | "q1" | "q2" | "q3" | "q4")}>
            <option value="all">all periods</option>
            <option value="annual">annual</option>
            <option value="q1">Q1</option>
            <option value="q2">Q2</option>
            <option value="q3">Q3</option>
            <option value="q4">Q4</option>
          </select>
          <button type="button" onClick={loadDiscrepancies} disabled={loading}>
            {loading ? "Loading..." : "Load"}
          </button>
        </div>
      </div>

      <div className="reconcile-meta">
        <div>conflicts: {items.length}</div>
        <div>unresolved: {unresolvedCount}</div>
        <input value={reviewedBy} onChange={(e) => setReviewedBy(e.target.value)} placeholder="reviewer" />
        <button type="button" onClick={nextUnresolved} disabled={items.length === 0}>
          Next Conflict
        </button>
      </div>

      {error ? <div className="reconcile-error">{error}</div> : null}

      <div className="reconcile-workspace">
        <div className="discrepancy-list">
          {items.length === 0 ? <div className="empty-state">No discrepancy groups.</div> : null}
          {items.map((item, idx) => {
            const selectedCandidateId = item.resolution?.selected_candidate_id;
            const noteValue = notesByGroup[item.group_id] || "";
            return (
              <article
                key={item.group_id}
                className={`discrepancy-card ${idx === selectedIdx ? "active" : ""}`}
                ref={(node) => {
                  itemRefs.current[idx] = node;
                }}
                onClick={() => selectIndex(idx)}
              >
                <header className="discrepancy-head">
                  <div>
                    <strong>{item.key.metric_code}</strong>
                    <span>{item.key.metric_name_cn}</span>
                  </div>
                  <span className={`badge ${item.resolution?.resolution_method === "manual" ? "manual" : "auto"}`}>
                    {item.resolution?.resolution_method === "manual" ? "manual" : "unresolved"}
                  </span>
                </header>

                <div className="discrepancy-key">
                  {item.fact_type === "flow"
                    ? `${item.key.period_start_date || ""} -> ${item.key.period_end_date || ""}`
                    : `${item.key.as_of_date || ""}`}
                  {` | scope=${item.key.consolidation_scope || "-"}`}
                </div>

                <div className="engine-values">
                  {Object.entries(item.engine_values).map(([engine, values]) => (
                    <div key={engine}>
                      <span>{engine}</span>
                      <code>{values.join(", ")}</code>
                    </div>
                  ))}
                </div>

                <div className="page-chips">
                  {(item.pages || []).map((p) => (
                    <button
                      key={p}
                      type="button"
                      className={`page-chip ${activePage === p ? "active" : ""}`}
                      onClick={(e) => {
                        e.stopPropagation();
                        setActivePage(p);
                        setSelectedIdx(idx);
                      }}
                    >
                      p.{p}
                    </button>
                  ))}
                </div>

                <div className="candidate-table">
                  {item.candidates.map((c) => (
                    <div key={c.candidate_id} className="candidate-row">
                      <div>
                        <div>
                          <strong>{c.engine}</strong> {c.value}
                        </div>
                        <div className="candidate-meta">
                          page{" "}
                          {c.source_page ? (
                            <button
                              type="button"
                              className={`inline-page-link ${activePage === c.source_page ? "active" : ""}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                setSelectedIdx(idx);
                                setActivePage(c.source_page);
                              }}
                            >
                              p.{c.source_page}
                            </button>
                          ) : (
                            "-"
                          )}{" "}
                          | col {c.column_label || "-"} | q {c.quality_score || "-"}
                        </div>
                        <div className="candidate-meta">{c.raw_label || ""}</div>
                      </div>
                      <button
                        disabled={resolvingId === c.candidate_id}
                        className={selectedCandidateId === c.candidate_id ? "selected" : ""}
                        onClick={(e) => {
                          e.stopPropagation();
                          resolveCandidate(item, c.candidate_id);
                        }}
                      >
                        {selectedCandidateId === c.candidate_id ? "Selected" : "Select"}
                      </button>
                    </div>
                  ))}
                </div>

                <textarea
                  value={noteValue}
                  onChange={(e) =>
                    setNotesByGroup((prev) => ({
                      ...prev,
                      [item.group_id]: e.target.value,
                    }))
                  }
                  onClick={(e) => e.stopPropagation()}
                  placeholder="review note"
                />
              </article>
            );
          })}
        </div>

        <div className="pdf-panel">
          <div className="pdf-toolbar">
            <div>
              current page: <strong>{activePage}</strong>
            </div>
            <div>
              <button
                type="button"
                onClick={() => setActivePage((p) => Math.max(1, p - 1))}
                disabled={activePage <= 1 || !loadedReportId}
              >
                Prev Page
              </button>
              <button type="button" onClick={() => setActivePage((p) => p + 1)} disabled={!loadedReportId}>Next Page</button>
            </div>
          </div>
          {pdfSrc ? (
            <iframe
              key={`${loadedReportId}-${activePage}`}
              title="report pdf"
              src={pdfSrc}
              className="pdf-frame"
            />
          ) : (
            <div className="empty-state">No PDF.</div>
          )}
        </div>
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

      <DiscrepancyResolver />
    </main>
  );
}
