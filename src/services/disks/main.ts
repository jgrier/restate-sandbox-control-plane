// Dummy disk service, shaped like Archil: create, list, delete disks, plus exclusive mounts.
// A disk is just a directory under data/disks/<id>/, and agents read and write it directly.
// Mount ownership is bookkeeping, as with Archil's whole-disk lock: one client at a time,
// with an explicit forced takeover.

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { dataDir, ports } from "../../config.js";
import { HttpError, Router } from "../../lib/http.js";
import { logger } from "../../lib/log.js";

const log = logger("disks");
const diskRoot = path.join(dataDir, "disks");
fs.mkdirSync(diskRoot, { recursive: true });

const db = new DatabaseSync(path.join(dataDir, "disks.db"));
db.exec(`
  create table if not exists disks (
    id text primary key, labels text, mount_token text, status text,
    mounted_by text, mounted_at integer, created_at integer, deleted_at integer);
  create table if not exists events (disk_id text, at integer, kind text, detail text);
`);

// ------------------------------------------------------------------ the API

new Router()
  .on("POST", "/disks", ({ body }) => createDisk(body.clientToken, body.labels ?? {}))
  .on("GET", "/disks", () => listDisks())
  .on("GET", "/disks/:id", ({ params }) => getDisk(params.id))
  .on("DELETE", "/disks/:id", ({ params, query }) => deleteDisk(params.id, query.get("force") === "true"))
  .on("POST", "/disks/:id/mount", ({ params, body }) => mount(params.id, body.token, body.clientId, !!body.force))
  .on("POST", "/disks/:id/unmount", ({ params, body }) => unmount(params.id, body.clientId))
  .on("GET", "/disks/:id/files/*", ({ params }) => readFile(params.id, params.rest))
  .listen(ports.disks)
  .then(() => console.log(`disk service on :${ports.disks}`));

// ------------------------------------------------------------------ operations

/** Create a disk, or return the existing one: the client token is the disk id. */
function createDisk(id: string, labels: Record<string, string>) {
  const existing = getRow(id);
  if (existing) return { id, mountToken: existing.mount_token, path: dirOf(id) };

  const mountToken = `mt_${randomUUID()}`;
  fs.mkdirSync(dirOf(id), { recursive: true });
  db.prepare("insert into disks values (?, ?, ?, 'ready', null, null, ?, null)").run(id, JSON.stringify(labels), mountToken, Date.now());
  recordEvent(id, "created");
  log.info("disk created", id);
  return { id, mountToken, path: dirOf(id) };
}

/** Every disk, including deleted ones. */
function listDisks() {
  return (db.prepare("select * from disks order by created_at").all() as DiskRow[]).map(view);
}

function getDisk(id: string) {
  const row = getRow(id);
  if (!row) throw notFound(id);
  return view(row);
}

/** Delete a disk and its contents. A mounted disk needs `force`. Deleting twice is fine. */
function deleteDisk(id: string, force: boolean) {
  const row = getRow(id);
  if (!row || row.status === "deleted") return { ok: true };
  if (row.mounted_by && !force) throw new HttpError(409, { error: `mounted by ${row.mounted_by}` }, "delete");

  fs.rmSync(dirOf(id), { recursive: true, force: true });
  db.prepare("update disks set status = 'deleted', mounted_by = null, deleted_at = ? where id = ?").run(Date.now(), id);
  recordEvent(id, "deleted");
  log.info("disk deleted", id);
  return { ok: true };
}

/** Exclusive mount. Another holder means 409, unless `force` takes the disk over. */
function mount(id: string, token: string, clientId: string, force: boolean) {
  const row = liveRow(id);
  if (token !== row.mount_token) throw new HttpError(403, { error: "bad mount token" }, "mount");

  const holder = row.mounted_by;
  if (holder && holder !== clientId) {
    if (!force) throw new HttpError(409, { error: `already mounted by ${holder}` }, "mount");
    recordEvent(id, "taken over", `${holder} → ${clientId}`);
    log.warn(`mount taken over from ${holder} by ${clientId}`, id);
  } else {
    log.info(`mounted by ${clientId}`, id);
  }
  db.prepare("update disks set mounted_by = ?, mounted_at = ? where id = ?").run(clientId, Date.now(), id);
  recordEvent(id, "mounted", clientId);
  return { path: dirOf(id) };
}

/** Release the mount, if this client holds it. */
function unmount(id: string, clientId: string) {
  const row = getRow(id);
  if (row?.mounted_by === clientId) {
    db.prepare("update disks set mounted_by = null, mounted_at = null where id = ?").run(id);
    recordEvent(id, "unmounted", clientId);
    log.info(`unmounted by ${clientId}`, id);
  }
  return { ok: true };
}

/** Read a file from a disk, for the page's transcript viewer. */
function readFile(id: string, relativePath: string) {
  liveRow(id);
  const file = path.join(dirOf(id), path.normalize(relativePath).replace(/^(\.\.[/\\])+/, ""));
  return { content: fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "" };
}

// ------------------------------------------------------------------ storage helpers

type DiskRow = {
  id: string;
  labels: string;
  mount_token: string;
  status: "ready" | "deleted";
  mounted_by: string | null;
  mounted_at: number | null;
  created_at: number;
  deleted_at: number | null;
};

function getRow(id: string) {
  return db.prepare("select * from disks where id = ?").get(id) as DiskRow | undefined;
}

/** The disk's row, or a 404 if it doesn't exist or was deleted. */
function liveRow(id: string) {
  const row = getRow(id);
  if (!row || row.status === "deleted") throw notFound(id);
  return row;
}

function notFound(id: string) {
  return new HttpError(404, { error: `no disk ${id}` }, "disks");
}

function dirOf(id: string) {
  return path.join(diskRoot, id);
}

function recordEvent(id: string, kind: string, detail = "") {
  db.prepare("insert into events values (?, ?, ?, ?)").run(id, Date.now(), kind, detail);
}

/** What the API returns for a disk. */
function view(row: DiskRow) {
  const events = db.prepare("select at, kind, detail from events where disk_id = ? order by at desc limit 8").all(row.id);
  return {
    id: row.id,
    labels: JSON.parse(row.labels),
    status: row.status,
    mountedBy: row.mounted_by,
    mountedAt: row.mounted_at,
    createdAt: row.created_at,
    deletedAt: row.deleted_at,
    sizeBytes: row.status === "ready" ? dirSize(dirOf(row.id)) : 0,
    events,
  };
}

function dirSize(dir: string): number {
  if (!fs.existsSync(dir)) return 0;
  return fs.readdirSync(dir, { withFileTypes: true }).reduce((sum, entry) => {
    const p = path.join(dir, entry.name);
    return sum + (entry.isDirectory() ? dirSize(p) : fs.statSync(p).size);
  }, 0);
}
