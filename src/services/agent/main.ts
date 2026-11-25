// Fake agent-server: the process that runs inside one sandbox. It mounts the session's disk,
// keeps the conversation in an append-only transcript on that disk, runs one turn at a time
// (an echo "model" plus one simulated tool call), streams the turn over SSE, and reports
// `turn.ended`. When it's handed a turn that a previous VM didn't finish, it repairs the
// transcript and continues, the way real harnesses recover an interrupted turn.

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { turnDelayMs, urls } from "../../config.js";
import { HttpError, Router, deliverWebhook, openSse, request, sleep } from "../../lib/http.js";
import { logger } from "../../lib/log.js";

const sandboxId = process.env.SANDBOX_ID!;
const sessionId = process.env.SESSION_ID!;
const diskId = process.env.DISK_ID!;
const log = logger("agent");

type Entry =
  | { type: "user"; messageId: string; text: string; at: number }
  | { type: "tool_call"; messageId: string; callId: string; name: string; args: unknown; at: number }
  | { type: "tool_result"; messageId: string; callId: string; result?: string; error?: string; at: number }
  | { type: "assistant"; messageId: string; text: string; at: number }
  | { type: "system"; messageId?: string; note: string; at: number }
  | { type: "turn_end"; messageId: string; status: "completed" | "failed" | "interrupted"; at: number };

// ------------------------------------------------------------------ boot: mount the disk

const { path: diskPath } = await request("POST", `${urls.disks}/disks/${diskId}/mount`, {
  token: process.env.MOUNT_TOKEN,
  clientId: sandboxId,
  force: process.env.FORCE_MOUNT === "true",
});
const transcriptFile = path.join(diskPath, "transcript.jsonl");
const transcript: Entry[] = fs.existsSync(transcriptFile)
  ? fs.readFileSync(transcriptFile, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line))
  : [];

let mounted = true;
let currentTurn: string | null = null;
let shuttingDown = false;
const streams = new Set<ReturnType<typeof openSse>>();

// ------------------------------------------------------------------ the API

new Router()
  .on("GET", "/health", () => health())
  .on("POST", "/api/prompt", ({ body }) => prompt(body.messageId, body.text))
  .on("GET", "/api/events", ({ res }) => streamEvents(res))
  .on("POST", "/api/shutdown", () => shutdown())
  .listen(Number(process.env.PORT))
  .then(() => log.info(`agent up, disk mounted, ${transcript.length} transcript entries`, sandboxId));

process.on("SIGTERM", async () => {
  await unmount();
  process.exit(0);
});

// ------------------------------------------------------------------ operations

function health() {
  return { ok: true, sandboxId };
}

/** Take a prompt and start its turn. Idempotent per messageId, using the transcript on disk. */
function prompt(messageId: string, text: string) {
  if (shuttingDown) throw new HttpError(503, { error: "shutting down" }, "prompt");
  if (findTurnEnd(messageId)) return { status: "already-completed" };
  if (currentTurn === messageId) return { status: "admitted" }; // a duplicate POST
  if (currentTurn) throw new HttpError(409, { error: `busy with ${currentTurn}` }, "prompt");

  const earlier = findUserMessage(messageId);
  if (earlier) {
    // The same message, but no turn_end: a previous VM died during this turn.
    log.warn(`recovering interrupted turn ${messageId}`, sandboxId);
    repairInterruptedTurn(messageId);
    void runTurn(messageId, earlier.text, true);
    return { status: "admitted", recovered: true };
  }

  closeOpenTurns();
  append({ type: "user", messageId, text, at: Date.now() });
  broadcast("user", { messageId, text });
  void runTurn(messageId, text, false);
  return { status: "admitted" };
}

/** SSE: a snapshot of the transcript, then every event of every turn as it happens. */
function streamEvents(res: Parameters<typeof openSse>[0]) {
  const stream = openSse(res);
  stream.send("snapshot", { sandboxId, transcript, currentTurn });
  streams.add(stream);
}

/** Graceful shutdown hook: stop taking prompts, flush, unmount. */
async function shutdown() {
  shuttingDown = true;
  await unmount();
  log.info("shutdown hook: unmounted disk", sandboxId);
  return { ok: true };
}

// ------------------------------------------------------------------ a turn

async function runTurn(messageId: string, text: string, recovered: boolean) {
  currentTurn = messageId;
  broadcast("turn.started", { messageId, recovered, sandboxId });
  const turnNumber = transcript.filter((e) => e.type === "user").length;
  const earlier = transcript.filter((e) => (e.type === "user" || e.type === "assistant") && e.messageId !== messageId).length;

  let reply = await streamWords(
    messageId,
    (recovered ? "(My previous sandbox died mid-turn, so I'm picking this turn back up from the transcript.) " : "") +
      `Turn ${turnNumber}, running on ${sandboxId}. I can see ${earlier} earlier messages on my disk. Let me look something up.`,
  );

  // One simulated tool call, so there's a window to crash in mid-tool.
  const words = await countWordsTool(messageId, text);

  reply += " " + (await streamWords(messageId, `The tool says your message has ${words} words. You said: "${text}".`));
  append({ type: "assistant", messageId, text: reply, at: Date.now() });
  append({ type: "turn_end", messageId, status: "completed", at: Date.now() });
  broadcast("turn.ended", { messageId });
  currentTurn = null;
  log.info(`turn finished (${messageId})`, sandboxId);

  await reportTurnEnded(messageId);
}

async function countWordsTool(messageId: string, text: string) {
  const callId = randomUUID().slice(0, 8);
  append({ type: "tool_call", messageId, callId, name: "count_words", args: { text }, at: Date.now() });
  broadcast("tool.started", { messageId, callId, name: "count_words" });
  await sleep(1_500);
  const words = text.trim().split(/\s+/).filter(Boolean).length;
  append({ type: "tool_result", messageId, callId, result: `${words} words`, at: Date.now() });
  broadcast("tool.finished", { messageId, callId, result: `${words} words` });
  return words;
}

async function streamWords(messageId: string, text: string) {
  for (const word of text.split(" ")) {
    broadcast("assistant.delta", { messageId, text: word + " " });
    await sleep(turnDelayMs);
  }
  return text;
}

/** The lifecycle webhook: tell the proxy (and through it, Restate) the turn is over. */
async function reportTurnEnded(messageId: string) {
  await deliverWebhook(`${urls.proxy}/internal/events`, {
    eventId: randomUUID(), type: "turn.ended", sessionId, sandboxId, messageId,
  }).catch((e) => log.error(`could not report turn.ended: ${e}`, sandboxId));
}

// ------------------------------------------------------------------ recovering interrupted turns

/** Close any earlier turn that never finished, before a new one starts. */
function closeOpenTurns() {
  for (const e of transcript)
    if (e.type === "user" && !findTurnEnd(e.messageId)) {
      append({ type: "system", messageId: e.messageId, note: "This turn was interrupted and abandoned.", at: Date.now() });
      append({ type: "turn_end", messageId: e.messageId, status: "interrupted", at: Date.now() });
    }
}

/** Repair an interrupted turn: close dangling tool calls and tell the model what happened. */
function repairInterruptedTurn(messageId: string) {
  const finished = new Set(transcript.filter((e) => e.type === "tool_result").map((e: any) => e.callId));
  for (const e of transcript)
    if (e.type === "tool_call" && e.messageId === messageId && !finished.has(e.callId))
      append({
        type: "tool_result", messageId, callId: e.callId, at: Date.now(),
        error: "interrupted: the sandbox stopped before this tool finished; its outcome is unknown",
      });
  const note =
    "Your previous sandbox stopped unexpectedly during this turn. Continue from the transcript. " +
    "Check the state of any side effects before repeating them.";
  append({ type: "system", messageId, note, at: Date.now() });
  broadcast("system", { messageId, note });
}

// ------------------------------------------------------------------ transcript and streams

/** Every step is written to the disk before it's streamed, so a crash loses nothing written. */
function append(entry: Entry) {
  transcript.push(entry);
  fs.appendFileSync(transcriptFile, JSON.stringify(entry) + "\n");
}

function broadcast(event: string, data: unknown) {
  for (const s of streams) s.closed() ? streams.delete(s) : s.send(event, data);
}

function findTurnEnd(messageId: string) {
  return transcript.find((e) => e.type === "turn_end" && e.messageId === messageId);
}

function findUserMessage(messageId: string) {
  return transcript.find((e): e is Extract<Entry, { type: "user" }> => e.type === "user" && e.messageId === messageId);
}

async function unmount() {
  if (!mounted) return;
  mounted = false;
  await request("POST", `${urls.disks}/disks/${diskId}/unmount`, { clientId: sandboxId }).catch(() => {});
}
