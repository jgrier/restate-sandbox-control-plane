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
import { HttpError, Router, deliverWebhook, request, sleep } from "../../lib/http.js";
import { logger } from "../../lib/log.js";

const log = logger("sandboxes");
const agentMain = new URL("../agent/main.ts", import.meta.url).pathname;
const logDir = path.join(dataDir, "logs");
fs.mkdirSync(logDir, { recursive: true });

const db = new DatabaseSync(path.join(dataDir, "sandboxes.db"));
db.exec(`create table if not exists sandboxes (
  id text primary key, labels text, disk_id text, pid integer, port integer, url text,
  status text, reason text, exit_code integer, created_at integer, destroyed_at integer)`);

const processes = new Map<string, ChildProcess>(); // running VMs we started
const creating = new Map<string, Promise<unknown>>(); // creates in progress, by sandbox id
const deleting = new Set<string>(); // VMs we're destroying on purpose

type CreateRequest = {
  clientToken: string; // the sandbox id
  labels: Record<string, string>;
  disk: { diskId: string; mountToken: string; force?: boolean };
};

// ------------------------------------------------------------------ the API

new Router()
  .on("POST", "/sandboxes", ({ body }) => createSandbox(body as CreateRequest))
  .on("GET", "/sandboxes", () => listSandboxes())
  .on("GET", "/sandboxes/:id", ({ params }) => getSandbox(params.id))
  .on("DELETE", "/sandboxes/:id", ({ params }) => destroySandbox(params.id))
  .on("POST", "/sandboxes/:id/kill", ({ params }) => killSandbox(params.id))
  .listen(ports.sandboxes)
  .then(() => {
    console.log(`sandbox service on :${ports.sandboxes}`);
    reconcile();
  });

// When the dev stack shuts down, take the VMs with us. A restart reports them as dead.
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.on(signal, () => {
    for (const child of processes.values()) child.kill("SIGKILL");
    process.exit(0);
  });

// ------------------------------------------------------------------ operations

/** Idempotent on the client token (the sandbox id): a retried create returns the same VM. */
function createSandbox(req: CreateRequest) {
  const id = req.clientToken;
  if (creating.has(id)) return creating.get(id); // the same create is still booting

  const row = getRow(id);
  if (row?.status === "running") return view(row);
  if (row) throw new HttpError(410, { error: `sandbox ${id} is ${row.status}` }, "create");

  const booting = bootVm(req).finally(() => creating.delete(id));
  creating.set(id, booting);
  return booting;
}

/** Every VM record, including destroyed ones. */
function listSandboxes() {
  return (db.prepare("select * from sandboxes order by created_at").all() as Row[]).map(view);
}

function getSandbox(id: string) {
  const row = getRow(id);
  if (!row) throw new HttpError(404, { error: `no sandbox ${id}` }, "sandboxes");
  return view(row);
}

/** Destroy a VM: SIGTERM, then SIGKILL after 5 s. Destroying twice is fine. */
async function destroySandbox(id: string) {
  const row = getRow(id);
  if (!row || row.status === "destroyed") return { ok: true };

  deleting.add(id);
  const child = processes.get(id);
  if (!child) {
    onExit(id, null); // not ours any more (e.g. we restarted): just record it
    return { ok: true };
  }
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  const forced = sleep(5_000).then(() => child.kill("SIGKILL"));
  await Promise.race([exited, forced.then(() => exited)]);
  return { ok: true };
}

/** Demo only: SIGKILL, as if the VM crashed. */
function killSandbox(id: string) {
  processes.get(id)?.kill("SIGKILL");
  return { ok: true };
}

// ------------------------------------------------------------------ VM lifecycle

/** Start the agent process, then wait until it answers, or give up after the boot timeout. */
async function bootVm(req: CreateRequest) {
  const id = req.clientToken;
  const port = await freePort();
  const url = `http://localhost:${port}`;
  db.prepare("insert into sandboxes values (?, ?, ?, null, ?, ?, 'starting', null, null, ?, null)").run(
    id, JSON.stringify(req.labels ?? {}), req.disk.diskId, port, url, Date.now());
  log.info(`booting VM (disk ${req.disk.diskId}${req.disk.force ? ", forced mount" : ""})`, id);

  const child = spawnAgent(id, port, req);
  processes.set(id, child);
  db.prepare("update sandboxes set pid = ? where id = ?").run(child.pid ?? null, id);
  child.on("exit", (code) => onExit(id, code));

  const deadline = Date.now() + timeouts.boot;
  while (Date.now() < deadline && getRow(id)?.status !== "destroyed") {
    if (await isHealthy(url)) {
      db.prepare("update sandboxes set status = 'running' where id = ?").run(id);
      log.info("VM running, agent healthy", id);
      return view(getRow(id)!);
    }
    await sleep(100);
  }
  child.kill("SIGKILL");
  log.error("VM failed to boot", id);
  throw new HttpError(504, { error: "boot timed out" }, "create");
}

/** The "VM": a detached child process running the agent, configured through its environment. */
function spawnAgent(id: string, port: number, req: CreateRequest): ChildProcess {
  const out = fs.openSync(path.join(logDir, `${id}.log`), "a");
  return spawn(process.execPath, ["--import", "tsx", agentMain], {
    detached: true,
    stdio: ["ignore", out, out],
    env: {
      ...process.env,
      SANDBOX_ID: id,
      SESSION_ID: req.labels?.sessionId ?? "",
      DISK_ID: req.disk.diskId,
      MOUNT_TOKEN: req.disk.mountToken,
      FORCE_MOUNT: String(!!req.disk.force),
      PORT: String(port),
    },
  });
}

/** A VM exited, for any reason: record it and send the `sandbox.stopped` webhook. */
function onExit(id: string, exitCode: number | null) {
  processes.delete(id);
  const row = getRow(id);
  if (!row || row.status === "destroyed") return;

  const reason = deleting.delete(id) ? "deleted" : "crashed";
  db.prepare("update sandboxes set status = 'destroyed', reason = ?, exit_code = ?, destroyed_at = ? where id = ?").run(
    reason, exitCode, Date.now(), id);
  if (reason === "crashed") log.warn(`VM died (exit ${exitCode})`, id);
  else log.info(`VM destroyed (exit ${exitCode})`, id);

  void deliverWebhook(`${urls.proxy}/internal/events`, {
    eventId: randomUUID(), type: "sandbox.stopped", sandboxId: id,
    sessionId: JSON.parse(row.labels).sessionId, reason, exitCode,
  }).catch((e) => log.error(`could not report sandbox.stopped: ${e}`, id));
}

/** After a restart: VMs we no longer have a handle on are watched by pid, or reported dead. */
function reconcile() {
  const live = db.prepare("select * from sandboxes where status != 'destroyed'").all() as Row[];
  for (const row of live) {
    if (!row.pid || !isAlive(row.pid)) {
      onExit(row.id, null);
      continue;
    }
    const watch = setInterval(() => {
      if (isAlive(row.pid!)) return;
      clearInterval(watch);
      onExit(row.id, null);
    }, 1_000);
  }
}

// ------------------------------------------------------------------ helpers

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

function getRow(id: string) {
  return db.prepare("select * from sandboxes where id = ?").get(id) as Row | undefined;
}

/** What the API returns for a sandbox. */
function view(row: Row) {
  return {
    sandboxID: row.id,
    labels: JSON.parse(row.labels),
    diskId: row.disk_id,
    url: row.url,
    pid: row.pid,
    status: row.status,
    reason: row.reason,
    exitCode: row.exit_code,
    createdAt: row.created_at,
    destroyedAt: row.destroyed_at,
  };
}

async function isHealthy(url: string) {
  try {
    await request("GET", `${url}/health`, undefined, 1_000);
    return true;
  } catch {
    return false;
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
