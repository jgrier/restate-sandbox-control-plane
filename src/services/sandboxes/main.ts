// Dummy sandbox service, shaped like CubeSandbox's E2B-style API. Each "VM" is a child
// process running the fake agent-server. The service owns the messy parts real control
// planes own: idempotent creates, waiting until the agent is healthy, noticing when a VM
// dies for any reason, and reporting that with a `sandbox.stopped` webhook.

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { dataDir, ports, timeouts, urls } from "../../config.js";
import { Router, deliverWebhook, request, sleep, withStatus } from "../../lib/http.js";
import { logger } from "../../lib/log.js";

const log = logger("sandboxes");
const agentMain = new URL("../agent/main.ts", import.meta.url).pathname;
const logDir = path.join(dataDir, "logs");
fs.mkdirSync(logDir, { recursive: true });

const db = new DatabaseSync(path.join(dataDir, "sandboxes.db"));
db.exec(`create table if not exists sandboxes (
  id text primary key, labels text, disk_id text, pid integer, port integer, url text,
  status text, reason text, exit_code integer, created_at integer, destroyed_at integer)`);

type Row = {
  id: string;
  labels: string;
  disk_id: string;
  pid: number | null;
  port: number | null;
  url: string | null;
  status: "starting" | "running" | "destroyed";
  reason: string | null;
  exit_code: number | null;
  created_at: number;
  destroyed_at: number | null;
};

const getRow = (id: string) => db.prepare("select * from sandboxes where id = ?").get(id) as Row | undefined;
const view = (r: Row) => ({
  sandboxID: r.id,
  labels: JSON.parse(r.labels),
  diskId: r.disk_id,
  url: r.url,
  pid: r.pid,
  status: r.status,
  reason: r.reason,
  exitCode: r.exit_code,
  createdAt: r.created_at,
  destroyedAt: r.destroyed_at,
});

const processes = new Map<string, ChildProcess>();
const creating = new Map<string, Promise<unknown>>();
const deleting = new Set<string>();

// ------------------------------------------------------------------ VM lifecycle

async function createVm(body: any) {
  const id: string = body.clientToken;
  const port = await freePort();
  const url = `http://localhost:${port}`;
  db.prepare("insert into sandboxes values (?, ?, ?, null, ?, ?, 'starting', null, null, ?, null)").run(
    id, JSON.stringify(body.labels ?? {}), body.disk.diskId, port, url, Date.now());
  log.info(`booting VM (disk ${body.disk.diskId}${body.disk.force ? ", forced mount" : ""})`, id);

  const out = fs.openSync(path.join(logDir, `${id}.log`), "a");
  const child = spawn(process.execPath, ["--import", "tsx", agentMain], {
    detached: true,
    stdio: ["ignore", out, out],
    env: {
      ...process.env,
      SANDBOX_ID: id,
      SESSION_ID: body.labels?.sessionId ?? "",
      DISK_ID: body.disk.diskId,
      MOUNT_TOKEN: body.disk.mountToken,
      FORCE_MOUNT: String(!!body.disk.force),
      PORT: String(port),
    },
  });
  processes.set(id, child);
  db.prepare("update sandboxes set pid = ? where id = ?").run(child.pid ?? null, id);
  child.on("exit", (code) => onExit(id, code));

  // Wait until the agent answers, or give up after the boot timeout.
  const deadline = Date.now() + timeouts.boot;
  while (Date.now() < deadline) {
    if (getRow(id)?.status === "destroyed") break; // died while booting
    try {
      await request("GET", `${url}/health`, undefined, 1_000);
      db.prepare("update sandboxes set status = 'running' where id = ?").run(id);
      log.info("VM running, agent healthy", id);
      return view(getRow(id)!);
    } catch {
      await sleep(100);
    }
  }
  child.kill("SIGKILL");
  log.error("VM failed to boot", id);
  return withStatus(504, { error: "boot timed out" });
}

function onExit(id: string, exitCode: number | null) {
  processes.delete(id);
  const row = getRow(id);
  if (!row || row.status === "destroyed") return;
  const reason = deleting.delete(id) ? "deleted" : "crashed";
  db.prepare("update sandboxes set status = 'destroyed', reason = ?, exit_code = ?, destroyed_at = ? where id = ?").run(
    reason, exitCode, Date.now(), id);
  (reason === "crashed" ? log.warn : log.info)(`VM ${reason === "crashed" ? "died" : "destroyed"} (exit ${exitCode})`, id);
  void deliverWebhook(`${urls.proxy}/internal/events`, {
    eventId: randomUUID(), type: "sandbox.stopped", sandboxId: id,
    sessionId: JSON.parse(row.labels).sessionId, reason, exitCode,
  }).catch((e) => log.error(`could not report sandbox.stopped: ${e}`, id));
}

async function destroyVm(id: string) {
  const child = processes.get(id);
  const row = getRow(id);
  if (!row || row.status === "destroyed") return;
  deleting.add(id);
  if (!child) return onExit(id, null); // not ours any more (e.g. we restarted); just record it
  child.kill("SIGTERM");
  const exited = new Promise((r) => child.once("exit", r));
  const forced = sleep(5_000).then(() => child.kill("SIGKILL"));
  await Promise.race([exited, forced.then(() => exited)]);
}

// ------------------------------------------------------------------ HTTP

new Router()
  /** Idempotent on clientToken (the sandbox id): a retried create returns the same VM. */
  .on("POST", "/sandboxes", async ({ body }) => {
    const id: string = body.clientToken;
    if (creating.has(id)) return creating.get(id);
    const row = getRow(id);
    if (row?.status === "running") return view(row);
    if (row) return withStatus(410, { error: `sandbox ${id} is ${row.status}` });
    const p = createVm(body).finally(() => creating.delete(id));
    creating.set(id, p);
    return p;
  })
  .on("GET", "/sandboxes", () => (db.prepare("select * from sandboxes order by created_at").all() as Row[]).map(view))
  .on("GET", "/sandboxes/:id", ({ params }) => {
    const row = getRow(params.id);
    return row ? view(row) : withStatus(404, { error: "no such sandbox" });
  })
  .on("DELETE", "/sandboxes/:id", async ({ params }) => {
    await destroyVm(params.id);
    return { ok: true };
  })
  /** Demo only: SIGKILL, as if the VM crashed. */
  .on("POST", "/sandboxes/:id/kill", ({ params }) => {
    processes.get(params.id)?.kill("SIGKILL");
    return { ok: true };
  })
  .listen(ports.sandboxes)
  .then(() => {
    console.log(`sandbox service on :${ports.sandboxes}`);
    reconcile();
  });

/** After a restart: VMs we no longer have a handle on are watched by pid, or reported dead. */
function reconcile() {
  const live = db.prepare("select * from sandboxes where status != 'destroyed'").all() as Row[];
  for (const row of live) {
    if (row.pid && isAlive(row.pid)) {
      const watch = setInterval(() => {
        if (!isAlive(row.pid!)) {
          clearInterval(watch);
          onExit(row.id, null);
        }
      }, 1_000);
    } else onExit(row.id, null);
  }
}

function isAlive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, () => {
      const { port } = srv.address() as net.AddressInfo;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

// When the dev stack shuts down, take the VMs with us. A restart reports them as dead.
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.on(signal, () => {
    for (const child of processes.values()) child.kill("SIGKILL");
    process.exit(0);
  });
