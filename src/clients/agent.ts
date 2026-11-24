// Client for the agent-server running inside a sandbox.

import { timeouts } from "../config.js";
import { HttpError, request, Unreachable } from "../lib/http.js";

export type Prompt = { messageId: string; text: string; attachmentRefs?: string[] };

export type PromptReply =
  | { status: "admitted"; recovered?: boolean }
  | { status: "already-completed" }
  | { status: "unreachable" }; // the VM is gone

export const agent = {
  async prompt(agentUrl: string, prompt: Prompt): Promise<PromptReply> {
    try {
      return await request("POST", `${agentUrl}/api/prompt`, prompt);
    } catch (e) {
      // No answer, or the agent is shutting down: either way this VM won't take the prompt.
      if (e instanceof Unreachable || (e instanceof HttpError && e.status === 503)) return { status: "unreachable" };
      throw e; // anything else is retried by ctx.run
    }
  },

  /** Graceful shutdown hook: flush and unmount. Best effort: a dead agent is fine too. */
  async shutdown(agentUrl: string): Promise<void> {
    await request("POST", `${agentUrl}/api/shutdown`, {}, timeouts.shutdown).catch(() => {});
  },
};
