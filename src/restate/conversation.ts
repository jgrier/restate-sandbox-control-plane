// Conversation[sessionId]: the prompt queue.
//
// There's no queue in this code. Every prompt is a one-way send to `deliver` on the
// session's Conversation key, and Restate runs the invocations for one key one at a time,
// in order. So `deliver` simply runs one turn to completion, and the next prompt waits in
// Restate until it's done.

import * as restate from "@restatedev/restate-sdk";
import type { Prompt } from "../clients/agent.js";
import { maxRecoveries, startCallTimeout, timeouts } from "../config.js";
import { Session, type TurnOutcome } from "./session.js";

export type PromptOutcome =
  | { status: "completed" }
  | { status: "aborted" | "rejected" | "failed"; reason: string };

export const Conversation = restate.object({
  name: "Conversation",
  handlers: {
    deliver: async (ctx: restate.ObjectContext, prompt: Prompt & { generation: number }): Promise<PromptOutcome> => {
      const session = ctx.objectClient(Session, ctx.key);

      for (let attempt = 0; attempt <= maxRecoveries; attempt++) {
        // The agent's `turn.ended` reaches Session, which completes this awakeable.
        const turn = ctx.awakeable<TurnOutcome>();
        const reply = await session.deliverPrompt({ ...prompt, turnToken: turn.id }).orTimeout(startCallTimeout);

        if (reply.status === "rejected") return { status: "rejected", reason: reply.reason };
        if (reply.status === "already-completed") return { status: "completed" };
        if (reply.status === "gone") continue; // the VM vanished first; try again on a new one

        let outcome: TurnOutcome;
        try {
          outcome = await turn.promise.orTimeout(timeouts.maxTurn);
        } catch (e) {
          if (!(e instanceof restate.TimeoutError)) throw e;
          await session.discardSandbox({ reason: "turn timed out" }).orTimeout(timeouts.call + timeouts.shutdown);
          return { status: "failed", reason: "the agent got stuck" };
        }

        if (outcome.status === "completed") return { status: "completed" };
        if (outcome.reason === "stopped") return { status: "aborted", reason: "the session was stopped" };

        // The VM died mid-turn. Deliver the same message again: the next agent finds the
        // unfinished turn in the transcript, repairs it, and carries on.
        ctx.console.warn(`turn ${prompt.messageId} interrupted (${outcome.reason}), re-driving it on a new sandbox`);
      }
      return { status: "failed", reason: `gave up after ${maxRecoveries} recovery attempts` };
    },
  },
});
