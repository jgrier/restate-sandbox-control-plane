// Conversation[sessionId]: the prompt queue.
//
// There's no queue in this code. Every prompt is a one-way send to `deliver` on the
// session's Conversation key, and Restate runs the invocations for one key one at a time,
// in order. So `deliver` simply runs one turn to completion, and the next prompt waits in
// Restate until it's done.

import * as restate from "@restatedev/restate-sdk";
import type { Prompt } from "../clients/agent.js";
import { deliverTimeout, timeouts } from "../config.js";
import { Session, type DeliverReply, type TurnOutcome } from "./session.js";

export type PromptOutcome =
  | { status: "completed" }
  | { status: "aborted" | "rejected" | "failed"; reason: string };

export const Conversation = restate.object({
  name: "Conversation",
  handlers: {
    deliver: async (ctx: restate.ObjectContext, prompt: Prompt & { generation: number }): Promise<PromptOutcome> => {
      const session = ctx.objectClient(Session, ctx.key);

      // The agent's `turn.ended` reaches Session, which completes this awakeable.
      const turn = ctx.awakeable<TurnOutcome>();

      // Hand the prompt to an agent. Session deals with dead or hung VMs; this either works
      // or fails for good.
      let reply: DeliverReply;
      try {
        reply = await session.deliverPrompt({ ...prompt, turnToken: turn.id }).orTimeout(deliverTimeout);
      } catch (e) {
        if (!(e instanceof restate.TerminalError)) throw e;
        return { status: e.code === 409 ? "rejected" : "failed", reason: e.message };
      }
      if (reply.status === "already-completed") return { status: "completed" };

      // Wait for the turn to end. The next prompt stays queued until then.
      let outcome: TurnOutcome;
      try {
        outcome = await turn.promise.orTimeout(timeouts.maxTurn);
      } catch (e) {
        if (!(e instanceof restate.TimeoutError)) throw e;
        await session.discardSandbox({ reason: "turn timed out" }).orTimeout(timeouts.call + timeouts.shutdown);
        return { status: "failed", reason: "the agent got stuck" };
      }

      // Completed, stopped by the user, or the VM died mid-turn. No re-runs: the next prompt
      // gets a fresh VM, and the agent marks the unfinished turn as interrupted.
      return outcome.status === "completed" ? { status: "completed" } : { status: "aborted", reason: outcome.reason };
    },
  },
});
