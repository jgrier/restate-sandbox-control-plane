// Dummy disk service, shaped like Archil: create, list, delete disks, plus exclusive mounts.
// A disk is just a directory under data/disks/<id>/, and agents read and write it directly.
// Mount ownership is bookkeeping, as with Archil's whole-disk lock: one client at a time,
// with an explicit forced takeover.

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { dataDir, ports } from "../../config.js";
import { HttpError, Router, withStatus } from "../../lib/http.js";
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

const getRow = (id: string) => db.prepare("select * from disks where id = ?").get(id) as DiskRow | undefined;
const dirOf = (id: string) => path.join(diskRoot, id);

function recordEvent(id: string, kind: string, detail = "") {
  db.prepare("insert into events values (?, ?, ?, ?)").run(id, Date.now(), kind, detail);
}

function view(row: DiskRow) {
  const events = db
    .prepare("select at, kind, detail from events where disk_id = ? order by at desc limit 8")
    .all(row.id);
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
  return fs.readdirSync(dir, { withFileTypes: true }).reduce((sum, e) => {
    const p = path.join(dir, e.name);
    return sum + (e.isDirectory() ? dirSize(p) : fs.statSync(p).size);
  }, 0);
}

function requireDisk(id: string) {
  const row = getRow(id);
  if (!row || row.status === "deleted") throw new HttpError(404, { error: `no disk ${id}` }, "disk");
  return row;
}

new Router()
  /** Idempotent on clientToken, which is also the disk id. */
  .on("POST", "/disks", ({ body }) => {
    const id: string = body.clientToken;
    const existing = getRow(id);
    if (existing) return { id, mountToken: existing.mount_token, path: dirOf(id) };
    const mountToken = `mt_${randomUUID()}`;
    fs.mkdirSync(dirOf(id), { recursive: true });
    db.prepare("insert into disks values (?, ?, ?, 'ready', null, null, ?, null)").run(
      id, JSON.stringify(body.labels ?? {}), mountToken, Date.now());
    recordEvent(id, "created");
    log.info("disk created", id);
    return { id, mountToken, path: dirOf(id) };
  })
  .on("GET", "/disks", () => (db.prepare("select * from disks order by created_at").all() as DiskRow[]).map(view))
  .on("GET", "/disks/:id", ({ params }) => {
    const row = getRow(params.id);
    return row ? view(row) : withStatus(404, { error: "no such disk" });
  })
  .on("DELETE", "/disks/:id", ({ params, query }) => {
    const row = getRow(params.id);
    if (!row || row.status === "deleted") return { ok: true };
    if (row.mounted_by && query.get("force") !== "true")
      return withStatus(409, { error: `mounted by ${row.mounted_by}` });
    fs.rmSync(dirOf(row.id), { recursive: true, force: true });
    db.prepare("update disks set status = 'deleted', mounted_by = null, deleted_at = ? where id = ?").run(Date.now(), row.id);
    recordEvent(row.id, "deleted");
    log.info("disk deleted", row.id);
    return { ok: true };
  })
  /** Exclusive mount. Another holder means 409, unless `force` takes the disk over. */
  .on("POST", "/disks/:id/mount", ({ params, body }) => {
    const row = getRow(params.id);
    if (!row || row.status === "deleted") return withStatus(404, { error: "no such disk" });
    if (body.token !== row.mount_token) return withStatus(403, { error: "bad mount token" });
    const holder = row.mounted_by;
    if (holder && holder !== body.clientId) {
      if (!body.force) return withStatus(409, { error: `already mounted by ${holder}` });
      recordEvent(row.id, "taken over", `${holder} → ${body.clientId}`);
      log.warn(`mount taken over from ${holder} by ${body.clientId}`, row.id);
    } else {
      log.info(`mounted by ${body.clientId}`, row.id);
    }
    db.prepare("update disks set mounted_by = ?, mounted_at = ? where id = ?").run(body.clientId, Date.now(), row.id);
    recordEvent(row.id, "mounted", body.clientId);
    return { path: dirOf(row.id) };
  })
  .on("POST", "/disks/:id/unmount", ({ params, body }) => {
    const row = getRow(params.id);
    if (row && row.mounted_by === body.clientId) {
      db.prepare("update disks set mounted_by = null, mounted_at = null where id = ?").run(row.id);
      recordEvent(row.id, "unmounted", body.clientId);
      log.info(`unmounted by ${body.clientId}`, row.id);
    }
    return { ok: true };
  })
  /** Read a file, for the page's transcript viewer. */
  .on("GET", "/disks/:id/files/*", ({ params }) => {
    const row = requireDisk(params.id);
    const file = path.join(dirOf(row.id), path.normalize(params.rest).replace(/^(\.\.[/\\])+/, ""));
    return { content: fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "" };
  })
  .listen(ports.disks)
  .then(() => console.log(`disk service on :${ports.disks}`));
