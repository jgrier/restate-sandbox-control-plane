// Session[sessionId]: the coordinator.
//
// A session owns one disk for its whole life and at most one live sandbox at a time. It
// finds or starts that sandbox, hands prompts to the agent inside it, tracks the turn in
// flight, and receives every lifecycle event. None of its handlers wait for a turn to finish
// (Conversation does that), so stop, delete and events always get through promptly.

import * as restate from "@restatedev/restate-sdk";
import { agent, type Prompt } from "../clients/agent.js";
import { maxDeliveryAttempts, startCallTimeout, timeouts } from "../config.js";
import { retry, statusRead } from "./common.js";
import { Disk } from "./disk.js";
import { Sandbox } from "./sandbox.js";

type Turn = { messageId: string; turnToken: string; sandboxId: string };

type SessionState = {
  status: "active" | "deleted";
  ownerId: string;
  diskId: string;
  sandboxId: string; // the current or most recent sandbox
  generation: number; // bumped by stop and delete, so prompts sent earlier are rejected
  currentTurn: Turn;
};

type Ctx = restate.ObjectContext<SessionState>;

/** How a turn ends, as reported to the awakeable Conversation is waiting on. */
export type TurnOutcome = { status: "completed" } | { status: "aborted"; reason: string };

/** deliverPrompt either returns one of these or fails with a TerminalError. */
export type DeliverReply = { status: "admitted" } | { status: "already-completed" };

export type LifecycleEvent =
  | { eventId: string; type: "turn.ended"; sandboxId: string; messageId: string }
  | { eventId: string; type: "sandbox.stopped"; sandboxId: string; reason: "deleted" | "crashed" };

// Restate: virtual object keyed by session id. Exclusive handlers (the default) run one at a time
// per key, and each one reads and writes this key's state atomically.
export const Session = restate.object({
  name: "Session",
  handlers: {
    create: async (ctx: Ctx, req: { ownerId: string }) => {
      if (await ctx.get("status")) return; // already created

      const diskId = `disk-${ctx.key}`;
      // Restate: call another virtual object (Disk[diskId]) and wait for its reply.
      await ctx.objectClient(Disk, diskId).create({ labels: { sessionId: ctx.key, ownerId: req.ownerId } }).orTimeout(timeouts.call);
      ctx.set("status", "active");
      ctx.set("ownerId", req.ownerId);
      ctx.set("diskId", diskId);
      ctx.set("generation", 0);
      ctx.console.info(`session created, disk ${diskId}`);
    },

    /** Get or create a live sandbox, and return where its agent is. */
    resume: async (ctx: Ctx) => {
      const rejected = await rejection(ctx);
      if (rejected) throw new restate.TerminalError(rejected, { errorCode: 409 });
      return liveSandbox(ctx);
    },

    /**
     * Get the prompt to an agent: start a sandbox if needed, and replace it if its agent turns out
     * to be unreachable. Either the prompt is delivered, or this fails with a TerminalError (409 if
     * the session can't take it, 503 if no VM would take it). Doesn't wait for the turn.
     */
    deliverPrompt: async (ctx: Ctx, req: Prompt & { turnToken: string; generation: number }): Promise<DeliverReply> => {
      const rejected = await rejection(ctx, req.generation);
      // Restate: TerminalError fails the call for good (no retries); the code reaches the caller.
      if (rejected) throw new restate.TerminalError(rejected, { errorCode: 409 });

      const prompt = { messageId: req.messageId, text: req.text, attachmentRefs: req.attachmentRefs };
      for (let attempt = 1; attempt <= maxDeliveryAttempts; attempt++) {
        const { sandboxId, agentUrl } = await liveSandbox(ctx);
        // Restate: ctx.run journals the side effect's result. If it throws, the handler is re-run
        // and this step executes again (bounded by `retry`); once it succeeds it's never repeated.
        const reply = await ctx.run("send prompt to agent", () => agent.prompt(agentUrl, prompt), retry);

        if (reply.status === "unreachable") {
          // Dead or hung: make sure it's really gone; the next attempt starts a new one.
          ctx.console.warn(`agent on ${sandboxId} is unreachable, replacing the VM`);
          await ctx.objectClient(Sandbox, sandboxId).stop({ reason: "unreachable" }).orTimeout(timeouts.call + timeouts.shutdown);
          continue;
        }
        if (reply.status === "admitted") {
          ctx.set("currentTurn", { messageId: req.messageId, turnToken: req.turnToken, sandboxId });
          // Restate: one-way send. Delivered durably; we don't wait for Sandbox to process it.
          ctx.objectSendClient(Sandbox, sandboxId).touch();
          // Restate: ctx.console logs are suppressed while a journal is being replayed.
          ctx.console.info(`turn ${req.messageId} started on ${sandboxId}`);
          return { status: "admitted" };
        }
        return { status: "already-completed" };
      }
      throw new restate.TerminalError(`no agent took the prompt after ${maxDeliveryAttempts} VMs`, { errorCode: 503 });
    },

    /**
     * Every lifecycle event from agents and the sandbox service lands here. Replays are safe
     * without remembering past events: the proxy sends each one with an idempotency key, and an
     * event only acts if it matches the current state (the turn in flight, a live sandbox).
     */
    onEvent: async (ctx: Ctx, event: LifecycleEvent) => {
      const turn = await ctx.get("currentTurn");
      if (event.type === "turn.ended") {
        // Must be this turn, on this sandbox: a recovered turn keeps its messageId but runs on a new VM.
        if (turn?.messageId !== event.messageId || turn.sandboxId !== event.sandboxId) return;
        endTurn(ctx, turn, { status: "completed" });
        ctx.objectSendClient(Sandbox, event.sandboxId).touch();
        ctx.console.info(`turn ${event.messageId} ended`);
      } else {
        await ctx.objectClient(Sandbox, event.sandboxId).markDestroyed({ reason: event.reason }).orTimeout(timeouts.call);
        if (turn?.sandboxId === event.sandboxId) {
          const reason = event.reason === "crashed" ? "vm died" : "vm destroyed";
          ctx.console.warn(`${event.sandboxId} stopped mid-turn (${reason})`);
          endTurn(ctx, turn, { status: "aborted", reason });
        }
      }
    },

    /** Stop the session's sandbox and reject prompts sent before now. The next prompt wakes it again. */
    stop: async (ctx: Ctx) => {
      // Restate: object state. ctx.set is committed together with the invocation's progress.
      ctx.set("generation", ((await ctx.get("generation")) ?? 0) + 1);
      await stopSandbox(ctx, "stopped");
      ctx.console.info("session stopped");
    },

    /** Throw the current sandbox away, e.g. because its agent got stuck. Queued prompts carry on. */
    discardSandbox: async (ctx: Ctx, req: { reason: string }) => {
      ctx.console.warn(`discarding sandbox: ${req.reason}`);
      await stopSandbox(ctx, req.reason);
    },

    delete: async (ctx: Ctx) => {
      if ((await ctx.get("status")) !== "active") return;

      ctx.set("generation", ((await ctx.get("generation")) ?? 0) + 1);
      await stopSandbox(ctx, "stopped");
      await ctx.objectClient(Disk, (await ctx.get("diskId"))!).delete().orTimeout(timeouts.call);
      ctx.set("status", "deleted");
      ctx.console.info("session deleted");
    },

    // Restate: shared handler. Read-only, and runs alongside the exclusive ones, so it's never
    // blocked by a running handler.
    getStatus: restate.handlers.object.shared(statusRead, async (ctx: restate.ObjectSharedContext<SessionState>) => ({
      status: (await ctx.get("status")) ?? "unknown",
      ownerId: await ctx.get("ownerId"),
      diskId: await ctx.get("diskId"),
      sandboxId: await ctx.get("sandboxId"),
      generation: (await ctx.get("generation")) ?? 0,
      currentTurn: (await ctx.get("currentTurn"))?.messageId ?? null,
    })),
  },
});

/** Why this session can't take a prompt right now, if it can't. */
async function rejection(ctx: Ctx, generation?: number): Promise<string | undefined> {
  const status = await ctx.get("status");
  if (status !== "active") return `session is ${status ?? "unknown"}`;
  if (generation !== undefined && generation !== (await ctx.get("generation")))
    return "the session was stopped after this prompt was sent";
}

/** The session's running sandbox, or a new one booted on the session's disk. */
async function liveSandbox(ctx: Ctx): Promise<{ sandboxId: string; agentUrl: string }> {
  const current = await ctx.get("sandboxId");
  if (current) {
    const vm = await ctx.objectClient(Sandbox, current).getStatus().orTimeout(timeouts.call);
    if (vm.status === "running") return { sandboxId: current, agentUrl: vm.agentUrl! };
  }

  // Restate: ctx.rand is deterministic. The value is recorded in the journal, so a retry or a
  // replay gets the same id (and the sandbox service creates the same VM, not a second one).
  const sandboxId = `sbx-${ctx.rand.uuidv4().slice(0, 8)}`;
  const diskId = (await ctx.get("diskId"))!;
  ctx.console.info(current ? `${current} is gone, starting ${sandboxId}` : `starting ${sandboxId}`);

  const { mountToken, forceMount } = await ctx.objectClient(Disk, diskId).assign({ sandboxId }).orTimeout(timeouts.call);
  ctx.set("sandboxId", sandboxId);
  const { agentUrl } = await ctx
    .objectClient(Sandbox, sandboxId)
    .start({ diskId, mountToken, forceMount, labels: { sessionId: ctx.key } })
    .orTimeout(startCallTimeout);
  return { sandboxId, agentUrl };
}

/** Abort any turn in flight, destroy the sandbox, release the disk. */
async function stopSandbox(ctx: Ctx, reason: string): Promise<void> {
  const turn = await ctx.get("currentTurn");
  if (turn) endTurn(ctx, turn, { status: "aborted", reason });

  const sandboxId = await ctx.get("sandboxId");
  if (!sandboxId) return;
  await ctx.objectClient(Sandbox, sandboxId).stop({ reason }).orTimeout(timeouts.call + timeouts.shutdown);
  await ctx.objectClient(Disk, (await ctx.get("diskId"))!).release({ sandboxId }).orTimeout(timeouts.call);
}

/** Wake up the Conversation waiting on this turn. */
function endTurn(ctx: Ctx, turn: Turn, outcome: TurnOutcome): void {
  // Restate: complete the awakeable Conversation is waiting on, from a different handler.
  ctx.resolveAwakeable(turn.turnToken, outcome);
  ctx.clear("currentTurn");
}
