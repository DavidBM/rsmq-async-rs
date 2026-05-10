// Async worker helper: polls registered queues, dispatches each message to a
// per-queue handler, runs the handler under a visibility heartbeat, optionally
// routes failures to a dead-letter queue.
//
// Design rules:
//   - createWorker(opts) returns a plain object with { run, runUntil }.
//   - All state is held in the closure. No singletons, no module mutation.
//   - The Redis instance is dependency-injected via opts.redis or via a
//     pre-built rbmq client passed in opts.client. We never open or close it.

import { createClient } from "./client.js";

/**
 * @typedef {Object} WorkerDlq
 * @property {string} queue
 * @property {number|bigint} maxFailures
 */

/**
 * @typedef {(msg: import("./client.js").RbmqMessage) => Promise<void>} Handler
 */

/**
 * @typedef {Object} WorkerOptions
 * @property {import("ioredis").Redis} [redis]   Required if `client` not provided.
 * @property {ReturnType<import("./client.js").createClient>} [client] Pre-built rbmq client.
 * @property {string} [ns]
 * @property {Object<string, Handler>} routes   Queue name -> async handler.
 * @property {number} [pollIntervalMs=1000]
 * @property {number} [heartbeatIntervalMs=10000]
 * @property {number} [visibilityExtensionMs=30000]
 * @property {WorkerDlq} [dlq]                    Global default DLQ.
 * @property {Object<string, WorkerDlq>} [routeDlqs] Per-route override.
 * @property {{warn?: Function, error?: Function}} [log]
 */

/**
 * @param {WorkerOptions} opts
 */
export function createWorker(opts) {
    if (!opts || typeof opts !== "object") {
        throw new TypeError("createWorker requires an options object");
    }
    if (!opts.routes || Object.keys(opts.routes).length === 0) {
        throw new TypeError("createWorker requires at least one route");
    }
    const client =
        opts.client ||
        (opts.redis ? createClient(opts.redis, { ns: opts.ns }) : null);
    if (!client) {
        throw new TypeError("createWorker requires either `redis` or `client`");
    }

    const routes = { ...opts.routes };
    const pollIntervalMs = opts.pollIntervalMs ?? 1000;
    const heartbeatIntervalMs = opts.heartbeatIntervalMs ?? 10_000;
    const visibilityExtensionMs = opts.visibilityExtensionMs ?? 30_000;
    const dlq = opts.dlq;
    const routeDlqs = opts.routeDlqs || {};
    const log = opts.log || console;

    // Reject self-loops up front (route X with DLQ X).
    for (const route of Object.keys(routes)) {
        const target = routeDlqs[route] || dlq;
        if (target && target.queue === route) {
            throw new Error(`DLQ for route ${JSON.stringify(route)} is the same queue (would loop)`);
        }
    }

    function dlqFor(qname) {
        return routeDlqs[qname] || dlq || null;
    }

    let running = false;

    async function pollOnce() {
        let didWork = false;
        for (const [qname, handler] of Object.entries(routes)) {
            const msg = await client.receiveMessage(qname);
            if (!msg) continue;
            didWork = true;
            await dispatch(qname, handler, msg);
        }
        return didWork;
    }

    async function dispatch(qname, handler, msg) {
        const heartbeat = setInterval(() => {
            client
                .changeMessageVisibility(qname, msg.id, visibilityExtensionMs)
                .catch((e) => {
                    if (log.warn) log.warn(`rbmq worker: heartbeat failed for ${qname}/${msg.id}: ${e.message}`);
                });
        }, heartbeatIntervalMs);

        try {
            try {
                await handler(msg);
                clearInterval(heartbeat);
                await client.deleteMessage(qname, msg.id);
            } catch (err) {
                clearInterval(heartbeat);
                const target = dlqFor(qname);
                if (target && msg.rc > BigInt(target.maxFailures)) {
                    try {
                        const moved = await client.moveMessage(qname, msg.id, target.queue);
                        if (moved && log.warn) {
                            log.warn(
                                `rbmq worker: routed ${qname}/${msg.id} to DLQ ${target.queue} after ${msg.rc} failure(s): ${err && err.message}`,
                            );
                        }
                    } catch (moveErr) {
                        if (log.error) log.error(
                            `rbmq worker: failed to move ${qname}/${msg.id} to DLQ ${target.queue}: ${moveErr.message} (handler error: ${err && err.message})`,
                        );
                    }
                } else {
                    if (log.warn) log.warn(
                        `rbmq worker: handler failed for ${qname}/${msg.id} (rc=${msg.rc}): ${err && err.message} (will redeliver)`,
                    );
                    // Leave the message hidden; it'll be redelivered after vt.
                }
            }
        } finally {
            clearInterval(heartbeat);
        }
    }

    async function run() {
        await runUntil(new AbortController().signal);
    }

    async function runUntil(signal) {
        if (running) throw new Error("worker is already running");
        running = true;
        try {
            while (!signal.aborted) {
                const did = await pollOnce();
                if (signal.aborted) break;
                if (!did) {
                    // Idle wait — interruptible by the abort signal.
                    await waitOrAbort(pollIntervalMs, signal);
                }
            }
        } finally {
            running = false;
        }
    }

    return { run, runUntil };
}

/** Sleep for `ms`, or resolve early when `signal` aborts. */
function waitOrAbort(ms, signal) {
    return new Promise((resolve) => {
        const timer = setTimeout(() => {
            signal.removeEventListener("abort", onAbort);
            resolve();
        }, ms);
        function onAbort() {
            clearTimeout(timer);
            signal.removeEventListener("abort", onAbort);
            resolve();
        }
        if (signal.aborted) {
            clearTimeout(timer);
            resolve();
            return;
        }
        signal.addEventListener("abort", onAbort, { once: true });
    });
}
