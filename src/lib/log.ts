// The control-plane log shown on the demo page. Every process sends short, human-readable
// lines to the proxy, which keeps the recent ones and streams them to the browser.

import { urls } from "../config.js";

export type LogLine = {
  at: number;
  source: string; // "Session", "Sandbox", "disks", "agent", ...
  subject?: string; // the key or id the line is about
  level: "info" | "warn" | "error";
  message: string;
};

/** Fire-and-forget. Logging must never slow down or break the thing being logged. */
export function emitLog(line: Omit<LogLine, "at">) {
  const body = JSON.stringify({ at: Date.now(), ...line });
  fetch(`${urls.proxy}/internal/log`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
    signal: AbortSignal.timeout(2_000),
  }).catch(() => {});
}

export const logger = (source: string) => ({
  info: (message: string, subject?: string) => emitLog({ source, subject, level: "info", message }),
  warn: (message: string, subject?: string) => emitLog({ source, subject, level: "warn", message }),
  error: (message: string, subject?: string) => emitLog({ source, subject, level: "error", message }),
});
