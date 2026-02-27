import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

function getBackendBaseUrl(): string {
  const explicit = process.env.BACKEND_BASE_URL || process.env.NEXT_PUBLIC_BACKEND_URL;
  if (explicit) return explicit.replace(/\/$/, "");

  const agentUrl = process.env.AGENT_URL || "http://127.0.0.1:8000/agui/run";
  if (agentUrl.endsWith("/agui/run")) {
    return agentUrl.slice(0, -"/agui/run".length);
  }
  return agentUrl.replace(/\/$/, "");
}

export async function GET(
  req: NextRequest,
  { params }: { params: { reportId: string } },
) {
  const reportId = params.reportId;
  if (!/^\d+$/.test(reportId)) {
    return new Response("Invalid reportId", { status: 400 });
  }

  const backendBase = getBackendBaseUrl();
  const upstreamUrl = `${backendBase}/fact-review/report-file/${reportId}`;
  const range = req.headers.get("range");

  const upstreamHeaders = new Headers();
  if (range) {
    upstreamHeaders.set("range", range);
  }

  let upstream: Response;
  try {
    upstream = await fetch(upstreamUrl, {
      method: "GET",
      headers: upstreamHeaders,
      cache: "no-store",
    });
  } catch {
    return new Response("Failed to fetch upstream PDF", { status: 502 });
  }

  if (!upstream.ok && upstream.status !== 206) {
    const text = await upstream.text();
    return new Response(text || "Upstream PDF error", { status: upstream.status });
  }

  const headers = new Headers();
  const passThrough = [
    "content-type",
    "content-length",
    "content-range",
    "accept-ranges",
    "last-modified",
    "etag",
    "cache-control",
  ];
  for (const key of passThrough) {
    const value = upstream.headers.get(key);
    if (value) {
      headers.set(key, value);
    }
  }
  if (!headers.get("content-type")) {
    headers.set("content-type", "application/pdf");
  }
  headers.set("content-disposition", "inline");

  return new Response(upstream.body, {
    status: upstream.status,
    headers,
  });
}
