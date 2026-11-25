// Conversation[sessionId]: the prompt queue.
//
// There's no queue in this code. Every prompt is a one-way send to `deliver` on the
// session's Conversation key, and Restate runs the invocations for one key one at a time,
// in order. So `deliver` simply runs one turn to completion, and the next prompt waits in
// Restate until it's done.

import * as restate from "@restatedev/restate-sdk";
import type { Prompt } from "../clients/agent.js";
import { deliverTimeout, timeouts } from "../config.js";
import { Session, type TurnOutcome } from "./session.js";

export type PromptOutcome =
  | { status: "completed" }
  | { status: "aborted" | "rejected" | "failed"; reason: string };

type Ctx = restate.ObjectContext;
type QueuedPrompt = Prompt & { generation: number };

// Restate: virtual object. One invocation per key at a time, in arrival order: this key's inbox
// is the prompt queue.
export const Conversation = restate.object({
  name: "Conversation",
  handlers: {
    /** Run one turn: hand the prompt to the agent, then wait for the turn to end. */
    deliver: async (ctx: Ctx, prompt: QueuedPrompt): Promise<PromptOutcome> => {
      // Restate: awakeable, a durable promise with an id. Session completes it when the agent's
      // `turn.ended` arrives (or the turn is aborted).
      const turnEnded = ctx.awakeable<TurnOutcome>();

      const started = await startTurn(ctx, prompt, turnEnded.id);
      if (started !== "started") return started;

      return waitForTurnToEnd(ctx, turnEnded.promise);
    },
  },
});

/**
 * Ask Session to get the prompt to an agent. Session deals with dead or hung VMs, so this
 * either starts the turn or comes back with a final outcome.
 */
async function startTurn(ctx: Ctx, prompt: QueuedPrompt, turnToken: string): Promise<"started" | PromptOutcome> {
  try {
    // Restate: a call between objects, with a timeout. Timing out stops our wait; it doesn't
    // cancel the work Session is doing.
    const reply = await session(ctx).deliverPrompt({ ...prompt, turnToken }).orTimeout(deliverTimeout);
    return reply.status === "already-completed" ? { status: "completed" } : "started";
  } catch (e) {
    // Restate: a TerminalError is a final answer, never retried. Anything else is retried.
    if (!(e instanceof restate.TerminalError)) throw e;
    return { status: e.code === 409 ? "rejected" : "failed", reason: e.message };
  }
}

/** Wait for the turn to end. Meanwhile the next prompt stays queued behind this invocation. */
async function waitForTurnToEnd(ctx: Ctx, turnEnded: restate.RestatePromise<TurnOutcome>): Promise<PromptOutcome> {
  try {
    // Restate: while waiting, the invocation is suspended (no compute); the timeout is a
    // durable timer.
    const outcome = await turnEnded.orTimeout(timeouts.maxTurn);
    return asPromptOutcome(outcome);
  } catch (e) {
    if (!(e instanceof restate.TimeoutError)) throw e;
    return giveUpOnStuckTurn(ctx);
  }
}

/** No turn.ended in time: throw the sandbox away so the next prompt gets a fresh one. */
async function giveUpOnStuckTurn(ctx: Ctx): Promise<PromptOutcome> {
  await session(ctx).discardSandbox({ reason: "turn timed out" }).orTimeout(timeouts.call + timeouts.shutdown);
  return { status: "failed", reason: "the agent got stuck" };
}

/**
 * Completed, stopped by the user, or the VM died mid-turn. No re-runs: the next prompt gets a
 * fresh VM, and the agent marks the unfinished turn as interrupted.
 */
function asPromptOutcome(outcome: TurnOutcome): PromptOutcome {
  return outcome.status === "completed" ? { status: "completed" } : { status: "aborted", reason: outcome.reason };
}

/** Restate: a client for calling this session's Session object (request-response). */
function session(ctx: Ctx) {
  return ctx.objectClient(Session, ctx.key);
}
