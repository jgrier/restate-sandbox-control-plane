// Fake agent-server: the process that runs inside one sandbox. It mounts the session's disk,
// keeps the conversation in an append-only transcript on that disk, runs one turn at a time
// (an echo "model" plus one simulated tool call), streams the turn over SSE, and reports
// `turn.ended`. When it's handed a turn that a previous VM didn't finish, it repairs the
// transcript and continues, the way real harnesses recover an interrupted turn.

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { turnDelayMs, urls } from "../../config.js";
import { Router, deliverWebhook, openSse, request, sleep, withStatus } from "../../lib/http.js";
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
  ? fs.readFileSync(transcriptFile, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l))
  : [];
let mounted = true;

const streams = new Set<ReturnType<typeof openSse>>();
let currentTurn: string | null = null;
let shuttingDown = false;

/** Every step is written to the disk before it's streamed, so a crash loses nothing written. */
function append(entry: Entry) {
  transcript.push(entry);
  fs.appendFileSync(transcriptFile, JSON.stringify(entry) + "\n");
}
function broadcast(event: string, data: unknown) {
  for (const s of streams) s.closed() ? streams.delete(s) : s.send(event, data);
}
const now = () => Date.now();

// ------------------------------------------------------------------ turns

const turnEnd = (messageId: string) => transcript.find((e) => e.type === "turn_end" && e.messageId === messageId);
const userMessage = (messageId: string) =>
  transcript.find((e): e is Extract<Entry, { type: "user" }> => e.type === "user" && e.messageId === messageId);

/** Close any earlier turn that never finished, before a new one starts. */
function closeOpenTurns() {
  for (const e of transcript)
    if (e.type === "user" && !turnEnd(e.messageId)) {
      append({ type: "system", messageId: e.messageId, note: "This turn was interrupted and abandoned.", at: now() });
      append({ type: "turn_end", messageId: e.messageId, status: "interrupted", at: now() });
    }
}

/** Repair an interrupted turn: close dangling tool calls and tell the model what happened. */
function repairInterruptedTurn(messageId: string) {
  const results = new Set(transcript.filter((e) => e.type === "tool_result").map((e: any) => e.callId));
  for (const e of transcript)
    if (e.type === "tool_call" && e.messageId === messageId && !results.has(e.callId))
      append({
        type: "tool_result", messageId, callId: e.callId, at: now(),
        error: "interrupted: the sandbox stopped before this tool finished; its outcome is unknown",
      });
  const note =
    "Your previous sandbox stopped unexpectedly during this turn. Continue from the transcript. " +
    "Check the state of any side effects before repeating them.";
  append({ type: "system", messageId, note, at: now() });
  broadcast("system", { messageId, note });
}

async function streamWords(messageId: string, text: string) {
  for (const word of text.split(" ")) {
    broadcast("assistant.delta", { messageId, text: word + " " });
    await sleep(turnDelayMs);
  }
  return text;
}

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
  const callId = randomUUID().slice(0, 8);
  append({ type: "tool_call", messageId, callId, name: "count_words", args: { text }, at: now() });
  broadcast("tool.started", { messageId, callId, name: "count_words" });
  await sleep(1_500);
  const words = text.trim().split(/\s+/).filter(Boolean).length;
  append({ type: "tool_result", messageId, callId, result: `${words} words`, at: now() });
  broadcast("tool.finished", { messageId, callId, result: `${words} words` });

  reply += " " + (await streamWords(messageId, `The tool says your message has ${words} words. You said: "${text}".`));
  append({ type: "assistant", messageId, text: reply, at: now() });
  append({ type: "turn_end", messageId, status: "completed", at: now() });
  broadcast("turn.ended", { messageId });
  currentTurn = null;
  log.info(`turn finished (${messageId})`, sandboxId);

  await deliverWebhook(`${urls.proxy}/internal/events`, {
    eventId: randomUUID(), type: "turn.ended", sessionId, sandboxId, messageId,
  }).catch((e) => log.error(`could not report turn.ended: ${e}`, sandboxId));
}

// ------------------------------------------------------------------ HTTP

new Router()
  .on("GET", "/health", () => ({ ok: true, sandboxId }))
  .on("POST", "/api/prompt", ({ body }) => {
    const { messageId, text } = body as { messageId: string; text: string };
    if (shuttingDown) return withStatus(503, { error: "shutting down" });
    if (turnEnd(messageId)) return { status: "already-completed" };
    if (currentTurn === messageId) return { status: "admitted" }; // a duplicate POST
    if (currentTurn) return withStatus(409, { error: `busy with ${currentTurn}` });

    const existing = userMessage(messageId);
    if (existing) {
      // The same message, but no turn_end: a previous VM died during this turn.
      log.warn(`recovering interrupted turn ${messageId}`, sandboxId);
      repairInterruptedTurn(messageId);
      void runTurn(messageId, existing.text, true);
      return { status: "admitted", recovered: true };
    }
    closeOpenTurns();
    append({ type: "user", messageId, text, at: now() });
    broadcast("user", { messageId, text });
    void runTurn(messageId, text, false);
    return { status: "admitted" };
  })
  .on("GET", "/api/events", ({ res }) => {
    const stream = openSse(res);
    stream.send("snapshot", { sandboxId, transcript, currentTurn });
    streams.add(stream);
  })
  /** Graceful shutdown hook: stop taking prompts, flush, unmount. */
  .on("POST", "/api/shutdown", async () => {
    shuttingDown = true;
    await unmount();
    log.info("shutdown hook: unmounted disk", sandboxId);
    return { ok: true };
  })
  .listen(Number(process.env.PORT))
  .then(() => log.info(`agent up, disk mounted, ${transcript.length} transcript entries`, sandboxId));

async function unmount() {
  if (!mounted) return;
  mounted = false;
  await request("POST", `${urls.disks}/disks/${diskId}/unmount`, { clientId: sandboxId }).catch(() => {});
}

process.on("SIGTERM", async () => {
  await unmount();
  process.exit(0);
});
