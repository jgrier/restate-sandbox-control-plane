// Session[sessionId]: the coordinator.
//
// A session owns one disk for its whole life and at most one live sandbox at a time. It
// finds or starts that sandbox, hands prompts to the agent inside it, tracks the turn in
// flight, and receives every lifecycle event. None of its handlers wait for a turn to finish
// (Conversation does that), so stop, delete and events always get through promptly.

import * as restate from "@restatedev/restate-sdk";
import { agent, type Prompt, type PromptReply } from "../clients/agent.js";
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
type LiveSandbox = { sandboxId: string; agentUrl: string };
type PromptRequest = Prompt & { turnToken: string; generation: number };

/** How a turn ends, as reported to the awakeable Conversation is waiting on. */
export type TurnOutcome = { status: "completed" } | { status: "aborted"; reason: string };

/** deliverPrompt either returns one of these or fails with a TerminalError. */
export type DeliverReply = { status: "admitted" } | { status: "already-completed" };

export type LifecycleEvent =
  | { eventId: string; type: "turn.ended"; sandboxId: string; messageId: string }
  | { eventId: string; type: "sandbox.stopped"; sandboxId: string; reason: "deleted" | "crashed" };

type TurnEnded = Extract<LifecycleEvent, { type: "turn.ended" }>;
type SandboxStopped = Extract<LifecycleEvent, { type: "sandbox.stopped" }>;

// Restate: virtual object keyed by session id. Exclusive handlers (the default) run one at a time
// per key, and each one reads and writes this key's state atomically.
export const Session = restate.object({
  name: "Session",
  handlers: {
    /** Create the session and its disk. Idempotent. */
    create: async (ctx: Ctx, req: { ownerId: string }) => {
      if (await exists(ctx)) return;
      const diskId = await createDisk(ctx, req.ownerId);
      markActive(ctx, req.ownerId, diskId);
    },

    /** Get or create a live sandbox, and return where its agent is. */
    resume: async (ctx: Ctx) => {
      await requireAcceptsPrompts(ctx);
      return liveSandbox(ctx);
    },

    /**
     * Get the prompt to an agent: start a sandbox if needed, and replace it if its agent turns out
     * to be unreachable. Either the prompt is delivered, or this fails with a TerminalError (409 if
     * the session can't take it, 503 if no VM would take it). Doesn't wait for the turn.
     */
    deliverPrompt: async (ctx: Ctx, req: PromptRequest): Promise<DeliverReply> => {
      await requireAcceptsPrompts(ctx, req.generation);

      for (let attempt = 1; attempt <= maxDeliveryAttempts; attempt++) {
        const sandbox = await liveSandbox(ctx);
        const reply = await sendPromptToAgent(ctx, sandbox, req);

        if (reply.status === "unreachable") {
          await replaceUnreachableSandbox(ctx, sandbox);
          continue;
        }
        if (reply.status === "admitted") recordTurnStarted(ctx, req, sandbox);
        return { status: reply.status };
      }
      throw noAgentTookThePrompt();
    },

    /**
     * Every lifecycle event from agents and the sandbox service lands here. Replays are safe
     * without remembering past events: the proxy sends each one with an idempotency key, and an
     * event only acts if it matches the current state (the turn in flight, a live sandbox).
     */
    onEvent: async (ctx: Ctx, event: LifecycleEvent) => {
      if (event.type === "turn.ended") await onTurnEnded(ctx, event);
      else await onSandboxStopped(ctx, event);
    },

    /** Stop the session's sandbox and reject prompts sent before now. The next prompt wakes it again. */
    stop: async (ctx: Ctx) => {
      await rejectPromptsSentBeforeNow(ctx);
      await stopSandbox(ctx, "stopped");
      ctx.console.info("session stopped");
    },

    /** Throw the current sandbox away, e.g. because its agent got stuck. Queued prompts carry on. */
    discardSandbox: async (ctx: Ctx, req: { reason: string }) => {
      ctx.console.warn(`discarding sandbox: ${req.reason}`);
      await stopSandbox(ctx, req.reason);
    },

    /** Stop the sandbox, delete the disk. The session refuses everything afterwards. */
    delete: async (ctx: Ctx) => {
      if ((await ctx.get("status")) !== "active") return;
      await rejectPromptsSentBeforeNow(ctx);
      await stopSandbox(ctx, "stopped");
      await deleteDisk(ctx);
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

// ------------------------------------------------------------------ the session

async function exists(ctx: Ctx): Promise<boolean> {
  return (await ctx.get("status")) !== null;
}

function markActive(ctx: Ctx, ownerId: string, diskId: string): void {
  // Restate: object state. ctx.set is committed together with the invocation's progress.
  ctx.set("status", "active");
  ctx.set("ownerId", ownerId);
  ctx.set("diskId", diskId);
  ctx.set("generation", 0);
  ctx.console.info(`session created, disk ${diskId}`);
}

/** Throws (409) if the session is gone, or was stopped after this prompt was sent. */
async function requireAcceptsPrompts(ctx: Ctx, promptGeneration?: number): Promise<void> {
  const status = await ctx.get("status");
  if (status !== "active") throw rejected(`session is ${status ?? "unknown"}`);
  if (promptGeneration !== undefined && promptGeneration !== (await ctx.get("generation")))
    throw rejected("the session was stopped after this prompt was sent");
}

/** Prompts carry the generation they were sent in; bumping it rejects the ones still queued. */
async function rejectPromptsSentBeforeNow(ctx: Ctx): Promise<void> {
  ctx.set("generation", ((await ctx.get("generation")) ?? 0) + 1);
}

function rejected(reason: string) {
  // Restate: TerminalError fails the call for good (no retries); the code reaches the caller.
  return new restate.TerminalError(reason, { errorCode: 409 });
}

// ------------------------------------------------------------------ prompts and turns

async function sendPromptToAgent(ctx: Ctx, sandbox: LiveSandbox, req: PromptRequest): Promise<PromptReply> {
  const prompt = { messageId: req.messageId, text: req.text, attachmentRefs: req.attachmentRefs };
  // Restate: ctx.run journals the side effect's result. If it throws, the handler is re-run
  // and this step executes again (bounded by `retry`); once it succeeds it's never repeated.
  return ctx.run("send prompt to agent", () => agent.prompt(sandbox.agentUrl, prompt), retry);
}

function recordTurnStarted(ctx: Ctx, req: PromptRequest, sandbox: LiveSandbox): void {
  ctx.set("currentTurn", { messageId: req.messageId, turnToken: req.turnToken, sandboxId: sandbox.sandboxId });
  touch(ctx, sandbox.sandboxId);
  // Restate: ctx.console logs are suppressed while a journal is being replayed.
  ctx.console.info(`turn ${req.messageId} started on ${sandbox.sandboxId}`);
}

function noAgentTookThePrompt() {
  return new restate.TerminalError(`no agent took the prompt after ${maxDeliveryAttempts} VMs`, { errorCode: 503 });
}

async function onTurnEnded(ctx: Ctx, event: TurnEnded): Promise<void> {
  const turn = await ctx.get("currentTurn");
  if (turn?.messageId !== event.messageId || turn.sandboxId !== event.sandboxId) return; // not this turn
  endTurn(ctx, turn, { status: "completed" });
  touch(ctx, event.sandboxId);
  ctx.console.info(`turn ${event.messageId} ended`);
}

async function onSandboxStopped(ctx: Ctx, event: SandboxStopped): Promise<void> {
  await sandbox(ctx, event.sandboxId).markDestroyed({ reason: event.reason }).orTimeout(timeouts.call);

  const turn = await ctx.get("currentTurn");
  if (turn?.sandboxId !== event.sandboxId) return; // no turn was running on it
  const reason = event.reason === "crashed" ? "vm died" : "vm destroyed";
  ctx.console.warn(`${event.sandboxId} stopped mid-turn (${reason})`);
  endTurn(ctx, turn, { status: "aborted", reason });
}

/** Wake up the Conversation waiting on this turn. */
function endTurn(ctx: Ctx, turn: Turn, outcome: TurnOutcome): void {
  // Restate: complete the awakeable Conversation is waiting on, from a different handler.
  ctx.resolveAwakeable(turn.turnToken, outcome);
  ctx.clear("currentTurn");
}

// ------------------------------------------------------------------ the sandbox

/** The session's running sandbox, or a new one booted on the session's disk. */
async function liveSandbox(ctx: Ctx): Promise<LiveSandbox> {
  const current = await ctx.get("sandboxId");
  if (current) {
    const vm = await sandbox(ctx, current).getStatus().orTimeout(timeouts.call);
    if (vm.status === "running") return { sandboxId: current, agentUrl: vm.agentUrl! };
  }
  return startNewSandbox(ctx, current);
}

async function startNewSandbox(ctx: Ctx, previous: string | null): Promise<LiveSandbox> {
  const sandboxId = newSandboxId(ctx);
  ctx.console.info(previous ? `${previous} is gone, starting ${sandboxId}` : `starting ${sandboxId}`);

  const diskId = (await ctx.get("diskId"))!;
  const { mountToken, forceMount } = await disk(ctx, diskId).assign({ sandboxId }).orTimeout(timeouts.call);
  ctx.set("sandboxId", sandboxId);
  const { agentUrl } = await sandbox(ctx, sandboxId)
    .start({ diskId, mountToken, forceMount, labels: { sessionId: ctx.key } })
    .orTimeout(startCallTimeout);
  return { sandboxId, agentUrl };
}

function newSandboxId(ctx: Ctx): string {
  // Restate: ctx.rand is deterministic. The value is recorded in the journal, so a retry or a
  // replay gets the same id (and the sandbox service creates the same VM, not a second one).
  return `sbx-${ctx.rand.uuidv4().slice(0, 8)}`;
}

/** Dead or hung: make sure it's really gone; the next delivery attempt starts a new one. */
async function replaceUnreachableSandbox(ctx: Ctx, unreachable: LiveSandbox): Promise<void> {
  ctx.console.warn(`agent on ${unreachable.sandboxId} is unreachable, replacing the VM`);
  await sandbox(ctx, unreachable.sandboxId).stop({ reason: "unreachable" }).orTimeout(timeouts.call + timeouts.shutdown);
}

/** Abort any turn in flight, destroy the sandbox, release the disk. */
async function stopSandbox(ctx: Ctx, reason: string): Promise<void> {
  const turn = await ctx.get("currentTurn");
  if (turn) endTurn(ctx, turn, { status: "aborted", reason });

  const sandboxId = await ctx.get("sandboxId");
  if (!sandboxId) return;
  await sandbox(ctx, sandboxId).stop({ reason }).orTimeout(timeouts.call + timeouts.shutdown);
  await disk(ctx, (await ctx.get("diskId"))!).release({ sandboxId }).orTimeout(timeouts.call);
}

/** Activity on the sandbox: push its idle shutdown back. */
function touch(ctx: Ctx, sandboxId: string): void {
  // Restate: one-way send. Delivered durably; we don't wait for Sandbox to process it.
  ctx.objectSendClient(Sandbox, sandboxId).touch();
}

// ------------------------------------------------------------------ the disk

async function createDisk(ctx: Ctx, ownerId: string): Promise<string> {
  const diskId = `disk-${ctx.key}`;
  await disk(ctx, diskId).create({ labels: { sessionId: ctx.key, ownerId } }).orTimeout(timeouts.call);
  return diskId;
}

async function deleteDisk(ctx: Ctx): Promise<void> {
  await disk(ctx, (await ctx.get("diskId"))!).delete().orTimeout(timeouts.call);
}

// ------------------------------------------------------------------ clients for the other objects

/** Restate: request-response clients for other virtual objects. */
function sandbox(ctx: Ctx, sandboxId: string) {
  return ctx.objectClient(Sandbox, sandboxId);
}

function disk(ctx: Ctx, diskId: string) {
  return ctx.objectClient(Disk, diskId);
}
