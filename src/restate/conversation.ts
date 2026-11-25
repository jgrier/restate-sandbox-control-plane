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

// Restate: virtual object. One invocation per key at a time, in arrival order: this key's inbox
// is the prompt queue.
export const Conversation = restate.object({
  name: "Conversation",
  handlers: {
    deliver: async (ctx: restate.ObjectContext, prompt: Prompt & { generation: number }): Promise<PromptOutcome> => {
      // Restate: a client for calling another virtual object (request-response).
      const session = ctx.objectClient(Session, ctx.key);

      // Restate: awakeable, a durable promise with an id. Session completes it when the agent's
      // `turn.ended` arrives (or the turn is aborted).
      const turn = ctx.awakeable<TurnOutcome>();

      // Hand the prompt to an agent. Session deals with dead or hung VMs; this either works
      // or fails for good.
      // Restate: a call between objects, with a timeout (orTimeout). Timing out stops our wait;
      // it doesn't cancel the work Session is doing.
      let reply: DeliverReply;
      try {
        reply = await session.deliverPrompt({ ...prompt, turnToken: turn.id }).orTimeout(deliverTimeout);
      } catch (e) {
        // Restate: a TerminalError is a final answer, never retried. Anything else is retried.
        if (!(e instanceof restate.TerminalError)) throw e;
        return { status: e.code === 409 ? "rejected" : "failed", reason: e.message };
      }
      if (reply.status === "already-completed") return { status: "completed" };

      // Wait for the turn to end. The next prompt stays queued until then.
      // Restate: while waiting, the invocation is suspended (no compute); the timeout is a
      // durable timer.
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
