"use client";

import { CopilotKit } from "@copilotkit/react-core";

export default function Providers({ children }: { children: React.ReactNode }) {
  return (
    <CopilotKit runtimeUrl="/api/copilotkit" agent="rag_agent">
      {children}
    </CopilotKit>
  );
}
