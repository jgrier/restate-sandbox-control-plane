import type { LoggerTransport } from "@restatedev/restate-sdk";
import { timeouts } from "../config.js";
import { emitLog } from "../lib/log.js";

/** Every ctx.run step retries transient failures, but not forever. Restate: RunOptions. */
export const retry = { maxRetryDuration: timeouts.stepRetry };

/**
 * Options for getStatus handlers: read-only and polled constantly by the demo page, so Restate
 * keeps nothing once they finish. Both are needed: journalRetention drops the journal, and
 * idempotencyRetention drops the completed-invocation record that the Restate UI lists.
 */
// Restate: per-handler options (retention of finished invocations).
export const statusRead = { journalRetention: 0, idempotencyRetention: 0 };

/**
 * Handlers log with `ctx.console`. This transport also forwards those lines to the demo page's
 * control-plane log. Restate suppresses them while replaying a journal, so each line appears once.
 */
// Restate: a custom LoggerTransport for the endpoint; meta.replaying marks replayed log calls.
export const controlPlaneLog: LoggerTransport = (meta, message, ...rest) => {
  if (meta.replaying) return;
  const text = [message, ...rest].map(String).join(" ");
  console.log(`[${meta.level}] ${meta.context?.invocationTarget ?? ""} ${text}`);
  if (meta.source !== "USER" || !meta.context) return;
  emitLog({
    source: meta.context.serviceName,
    subject: meta.context.key,
    level: meta.level === "warn" || meta.level === "error" ? meta.level : "info",
    message: `${meta.context.handlerName}: ${text}`,
  });
};
