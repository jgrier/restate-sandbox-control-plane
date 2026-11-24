// Sandbox[sandboxId]: the digital twin of one VM.
//
// An opaque resource: it boots, it can be touched to stay alive, and it's destroyed when it
// has been idle too long or when asked. It knows nothing about prompts or turns, and calls
// no other object. Destroyed is final: a new VM is always a new Sandbox key.

import * as restate from "@restatedev/restate-sdk";
import { agent } from "../clients/agent.js";
import { sandboxes, type CreateVm } from "../clients/sandboxes.js";
import { timeouts } from "../config.js";
import { retry } from "./common.js";

type SandboxState = {
  status: "starting" | "running" | "destroyed";
  diskId: string;
  agentUrl: string;
  destroyedReason: string;
  idleTimer: string; // invocation id of the pending idleTimeout call
  lastActivityAt: number;
};

type Ctx = restate.ObjectContext<SandboxState>;

export const Sandbox = restate.object({
  name: "Sandbox",
  handlers: {
    /** Boot the VM with its disk attached. Returns once the agent inside is healthy. */
    start: async (ctx: Ctx, req: Omit<CreateVm, "sandboxId">): Promise<{ agentUrl: string }> => {
      const status = await ctx.get("status");
      if (status === "running") return { agentUrl: (await ctx.get("agentUrl"))! };
      if (status === "destroyed") throw new restate.TerminalError(`${ctx.key} is destroyed`);

      ctx.set("status", "starting");
      ctx.set("diskId", req.diskId);
      ctx.console.info(`booting VM on ${req.diskId}`);
      try {
        const { agentUrl } = await ctx.run("create VM", () => sandboxes.create({ sandboxId: ctx.key, ...req }), retry);
        ctx.set("agentUrl", agentUrl);
        ctx.set("status", "running");
        await armIdleTimer(ctx);
        ctx.console.info("VM running");
        return { agentUrl };
      } catch (e) {
        await markDestroyed(ctx, "failed to boot");
        throw e;
      }
    },

    /** Activity: push the idle shutdown back. */
    touch: async (ctx: Ctx) => {
      if ((await ctx.get("status")) === "running") await armIdleTimer(ctx);
    },

    /** The idle timer fired. */
    idleTimeout: async (ctx: Ctx) => {
      ctx.clear("idleTimer"); // this invocation *is* the timer
      ctx.console.info(`idle for ${timeouts.idle / 1000}s, shutting down`);
      await destroyVm(ctx, "idle");
    },

    stop: async (ctx: Ctx, req: { reason: string }) => destroyVm(ctx, req.reason),

    /** The VM is gone (it crashed, or someone else destroyed it). Just record it. */
    markDestroyed: async (ctx: Ctx, req: { reason: string }) => markDestroyed(ctx, req.reason),

    /** Demo only: act as if the idle timer fired now. */
    expireIdleNow: async (ctx: Ctx) => {
      await cancelIdleTimer(ctx);
      ctx.console.info("idle timer expired early (demo)");
      await destroyVm(ctx, "idle");
    },

    getStatus: restate.handlers.object.shared(async (ctx: restate.ObjectSharedContext<SandboxState>) => ({
      status: (await ctx.get("status")) ?? "unknown",
      diskId: await ctx.get("diskId"),
      agentUrl: await ctx.get("agentUrl"),
      destroyedReason: await ctx.get("destroyedReason"),
      lastActivityAt: await ctx.get("lastActivityAt"),
    })),
  },
});

/**
 * The idle timer is a delayed call to ourselves. Activity cancels the pending call and
 * schedules a fresh one: no clocks to compare, no counters.
 */
async function armIdleTimer(ctx: Ctx): Promise<void> {
  await cancelIdleTimer(ctx);
  const timer = ctx.objectSendClient(Sandbox, ctx.key).idleTimeout(restate.rpc.sendOpts({ delay: timeouts.idle }));
  ctx.set("idleTimer", await timer.invocationId);
  ctx.set("lastActivityAt", await ctx.date.now());
}

async function cancelIdleTimer(ctx: Ctx): Promise<void> {
  const timer = await ctx.get("idleTimer");
  if (timer) ctx.invocation(timer as restate.InvocationId).cancel();
  ctx.clear("idleTimer");
}

/** Graceful shutdown hook (flush, unmount), then destroy the VM. */
async function destroyVm(ctx: Ctx, reason: string): Promise<void> {
  if ((await ctx.get("status")) !== "running") return;
  const agentUrl = (await ctx.get("agentUrl"))!;

  await ctx.run("shutdown hook", () => agent.shutdown(agentUrl));
  await ctx.run("destroy VM", () => sandboxes.destroy(ctx.key), retry);
  await markDestroyed(ctx, reason);
}

async function markDestroyed(ctx: Ctx, reason: string): Promise<void> {
  if ((await ctx.get("status")) === "destroyed") return;
  ctx.set("status", "destroyed");
  ctx.set("destroyedReason", reason);
  await cancelIdleTimer(ctx);
  ctx.console.info(`destroyed (${reason})`);
}
