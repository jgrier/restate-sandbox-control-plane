// Client for the sandbox service (shaped like CubeSandbox). Restate handlers call these
// inside ctx.run, so each call is one journaled, retried step.

import { TerminalError } from "@restatedev/restate-sdk";
import { timeouts, urls } from "../config.js";
import { HttpError, request } from "../lib/http.js";

export type CreateVm = {
  sandboxId: string;
  diskId: string;
  mountToken: string;
  forceMount: boolean;
  labels: Record<string, string>;
};

export const sandboxes = {
  /** Boot a VM with the disk attached. Returns once its agent is healthy. Idempotent per sandboxId. */
  async create(vm: CreateVm): Promise<{ agentUrl: string }> {
    try {
      const res = await request(
        "POST",
        `${urls.sandboxes}/sandboxes`,
        {
          clientToken: vm.sandboxId,
          labels: vm.labels,
          disk: { diskId: vm.diskId, mountToken: vm.mountToken, force: vm.forceMount },
        },
        timeouts.boot + timeouts.http,
      );
      return { agentUrl: res.url };
    } catch (e) {
      // 410: this sandbox id was already used and destroyed. Retrying can't help.
      if (e instanceof HttpError && e.status === 410) throw new TerminalError(e.message);
      throw e;
    }
  },

  async destroy(sandboxId: string): Promise<void> {
    await request("DELETE", `${urls.sandboxes}/sandboxes/${sandboxId}`, undefined, timeouts.shutdown + timeouts.http);
  },
};
