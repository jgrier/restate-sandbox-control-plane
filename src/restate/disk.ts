// Disk[diskId]: the digital twin of one persistent disk.
//
// It knows nothing about sessions, prompts or VMs beyond which sandbox it's assigned to.
// The agent inside the VM takes the real mount; this object records what was authorized.

import * as restate from "@restatedev/restate-sdk";
import { disks } from "../clients/disks.js";
import { retry, statusRead } from "./common.js";

type DiskState = {
  status: "ready" | "deleted";
  mountToken: string;
  assignedSandboxId: string;
};

type Ctx = restate.ObjectContext<DiskState>;

// Restate: virtual object keyed by disk id: the digital twin of one disk.
export const Disk = restate.object({
  name: "Disk",
  handlers: {
    /** Create the real disk. Idempotent. */
    create: async (ctx: Ctx, req: { labels: Record<string, string> }) => {
      if (await exists(ctx)) return;
      const mountToken = await createRealDisk(ctx, req.labels);
      markReady(ctx, mountToken);
    },

    /** Authorize a sandbox to mount this disk. Force the mount if the previous holder never released it. */
    assign: async (ctx: Ctx, req: { sandboxId: string }) => {
      const previous = await ctx.get("assignedSandboxId");
      ctx.set("assignedSandboxId", req.sandboxId);
      ctx.console.info(`assigned to ${req.sandboxId}${previous ? `, taking over from ${previous}` : ""}`);
      return { mountToken: (await ctx.get("mountToken"))!, forceMount: previous !== null };
    },

    /** The sandbox is done with the disk. Ignored if it isn't the assigned one. */
    release: async (ctx: Ctx, req: { sandboxId: string }) => {
      if ((await ctx.get("assignedSandboxId")) !== req.sandboxId) return;
      ctx.clear("assignedSandboxId");
      ctx.console.info(`released by ${req.sandboxId}`);
    },

    /** Delete the real disk and everything on it. Idempotent. */
    delete: async (ctx: Ctx) => {
      if ((await ctx.get("status")) !== "ready") return;
      await deleteRealDisk(ctx);
      markDeleted(ctx);
    },

    // Restate: shared handler (read-only, never blocked).
    getStatus: restate.handlers.object.shared(statusRead, async (ctx: restate.ObjectSharedContext<DiskState>) => ({
      status: (await ctx.get("status")) ?? "unknown",
      assignedSandboxId: await ctx.get("assignedSandboxId"),
    })),
  },
});

async function exists(ctx: Ctx): Promise<boolean> {
  // Restate: object state (ctx.get / ctx.set), private to this key.
  return (await ctx.get("status")) !== null;
}

async function createRealDisk(ctx: Ctx, labels: Record<string, string>): Promise<string> {
  // Restate: ctx.run around the side effect; the disk id is the idempotency token.
  const { mountToken } = await ctx.run("create disk", () => disks.create(ctx.key, labels), retry);
  return mountToken;
}

async function deleteRealDisk(ctx: Ctx): Promise<void> {
  await ctx.run("delete disk", () => disks.delete(ctx.key), retry);
}

function markReady(ctx: Ctx, mountToken: string): void {
  ctx.set("mountToken", mountToken);
  ctx.set("status", "ready");
  ctx.console.info("disk created");
}

function markDeleted(ctx: Ctx): void {
  ctx.clear("assignedSandboxId");
  ctx.set("status", "deleted");
  ctx.console.info("disk deleted");
}
