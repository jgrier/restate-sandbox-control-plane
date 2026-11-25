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

// Restate: virtual object keyed by disk id: the digital twin of one disk.
export const Disk = restate.object({
  name: "Disk",
  handlers: {
    create: async (ctx: restate.ObjectContext<DiskState>, req: { labels: Record<string, string> }) => {
      // Restate: object state (ctx.get / ctx.set), private to this key.
      if (await ctx.get("status")) return; // already created

      // Restate: ctx.run around the side effect; the disk id is the idempotency token.
      const { mountToken } = await ctx.run("create disk", () => disks.create(ctx.key, req.labels), retry);
      ctx.set("mountToken", mountToken);
      ctx.set("status", "ready");
      ctx.console.info("disk created");
    },

    /** Authorize a sandbox to mount this disk. Force the mount if the previous holder never released it. */
    assign: async (ctx: restate.ObjectContext<DiskState>, req: { sandboxId: string }) => {
      const previous = await ctx.get("assignedSandboxId");
      ctx.set("assignedSandboxId", req.sandboxId);
      ctx.console.info(`assigned to ${req.sandboxId}${previous ? `, taking over from ${previous}` : ""}`);

      return { mountToken: (await ctx.get("mountToken"))!, forceMount: previous !== null };
    },

    release: async (ctx: restate.ObjectContext<DiskState>, req: { sandboxId: string }) => {
      if ((await ctx.get("assignedSandboxId")) !== req.sandboxId) return;
      ctx.clear("assignedSandboxId");
      ctx.console.info(`released by ${req.sandboxId}`);
    },

    delete: async (ctx: restate.ObjectContext<DiskState>) => {
      if ((await ctx.get("status")) !== "ready") return;

      await ctx.run("delete disk", () => disks.delete(ctx.key), retry);
      ctx.clear("assignedSandboxId");
      ctx.set("status", "deleted");
      ctx.console.info("disk deleted");
    },

    // Restate: shared handler (read-only, never blocked).
    getStatus: restate.handlers.object.shared(statusRead, async (ctx: restate.ObjectSharedContext<DiskState>) => ({
      status: (await ctx.get("status")) ?? "unknown",
      assignedSandboxId: await ctx.get("assignedSandboxId"),
    })),
  },
});
