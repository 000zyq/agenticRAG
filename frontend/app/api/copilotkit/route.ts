import { CopilotRuntime, EmptyAdapter, copilotRuntimeNextJSAppRouterEndpoint } from "@copilotkit/runtime";
import { HttpAgent } from "@ag-ui/client";

const agentUrl = process.env.AGENT_URL || "http://127.0.0.1:8000/agui/run";

const ragAgent = new HttpAgent({ url: agentUrl, agentId: "rag_agent" });
const runtime = new CopilotRuntime({
  agents: { rag_agent: ragAgent },
});

const { handleRequest } = copilotRuntimeNextJSAppRouterEndpoint({
  runtime,
  serviceAdapter: new EmptyAdapter(),
  endpoint: "/api/copilotkit",
});

export async function POST(req: Request) {
  return handleRequest(req);
}

export async function GET(req: Request) {
  return handleRequest(req);
}
