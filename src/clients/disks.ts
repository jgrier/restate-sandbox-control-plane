// Client for the disk service (shaped like Archil).

import { urls } from "../config.js";
import { request } from "../lib/http.js";

export const disks = {
  /** Create the disk (idempotent per diskId) and return its mount token. */
  async create(diskId: string, labels: Record<string, string>): Promise<{ mountToken: string }> {
    const res = await request("POST", `${urls.disks}/disks`, { clientToken: diskId, labels });
    return { mountToken: res.mountToken };
  },

  /** Delete the disk and its contents, even if a dead VM still holds the mount. */
  async delete(diskId: string): Promise<void> {
    await request("DELETE", `${urls.disks}/disks/${diskId}?force=true`);
  },
};
