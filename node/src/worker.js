// Async worker helper: polls registered queues, dispatches each message to a
// per-queue handler, runs the handler under a visibility heartbeat, optionally
// routes failures to a dead-letter queue.
//
// Design rules:
//   - createWorker(opts) returns a plain object with { run, runUntil }.
//   - All state is held in the closure. No singletons, no module mutation.
//   - The Redis instance is dependency-injected via opts.redis or via a
//     pre-built rbmq client passed in opts.client. We never open or close it.

import { createClient, MAX_DURATION_MS } from "./client.js";

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

    const routes = Object.entries(opts.routes);
    const pollIntervalMs = opts.pollIntervalMs ?? 1000;
    const heartbeatIntervalMs = opts.heartbeatIntervalMs ?? 10_000;
    const visibilityExtensionMs = opts.visibilityExtensionMs ?? 30_000;
    if (!Number.isInteger(heartbeatIntervalMs) || heartbeatIntervalMs < 1
        || heartbeatIntervalMs > 2_147_483_647
        || !Number.isInteger(visibilityExtensionMs) || visibilityExtensionMs > MAX_DURATION_MS
        || visibilityExtensionMs <= heartbeatIntervalMs) {
        throw new RangeError("heartbeat interval must be at least 1ms and shorter than visibility extension");
    }
    const dlq = opts.dlq;
    const routeDlqs = opts.routeDlqs || {};
    const log = opts.log || console;

    // Reject self-loops up front (route X with DLQ X).
    for (const [route] of routes) {
        const target = routeDlqs[route] || dlq;
        if (target && target.queue === route) {
            throw new Error(`DLQ for route ${JSON.stringify(route)} is the same queue (would loop)`);
        }
    }

    function dlqFor(qname) {
        return routeDlqs[qname] || dlq || null;
    }

    let running = false;

    const claim = { minimumVisibilityMs: visibilityExtensionMs };

    async function pollOnce() {
        let didWork = false;
        for (const [qname, handler] of routes) {
            const msg = await client.receiveMessage(qname, claim);
            if (!msg) continue;
            didWork = true;
            await dispatch(qname, handler, msg);
        }
        return didWork;
    }

    async function dispatch(qname, handler, msg) {
        const id = msg.id;
        const receipt = { receiveCount: msg.rc };
        let heartbeatRequest;
        const heartbeat = setInterval(() => {
            if (heartbeatRequest) return;
            heartbeatRequest = client
                .changeMessageVisibility(qname, id, visibilityExtensionMs, receipt)
                .then((renewed) => {
                    if (renewed === false) clearInterval(heartbeat);
                })
                .catch((error) => {
                    log.warn?.(`rbmq worker: heartbeat failed for ${qname}/${id}: ${error.message}`);
                })
                .finally(() => {
                    heartbeatRequest = undefined;
                });
        }, heartbeatIntervalMs);

        try {
            try {
                await handler(msg);
            } finally {
                clearInterval(heartbeat);
                await heartbeatRequest;
            }
        } catch (error) {
            await retryOrDeadLetter(qname, id, receipt, error);
            return;
        }
        await client.deleteMessage(qname, id, receipt);
    }

    async function retryOrDeadLetter(qname, id, receipt, error) {
        const target = dlqFor(qname);
        if (target && receipt.receiveCount > BigInt(target.maxFailures)) {
            try {
                if (await client.moveMessage(qname, id, target.queue, receipt)) {
                    log.warn?.(
                        `rbmq worker: routed ${qname}/${id} to DLQ ${target.queue} after ${receipt.receiveCount} failure(s): ${error}`,
                    );
                }
                return;
            } catch (moveError) {
                log.error?.(
                    `rbmq worker: failed to move ${qname}/${id} to DLQ ${target.queue}: ${moveError.message} (handler error: ${error})`,
                );
            }
        } else {
            log.warn?.(`rbmq worker: handler failed for ${qname}/${id} (rc=${receipt.receiveCount}): ${error}`);
        }

        try {
            await client.changeMessageVisibility(qname, id, null, receipt);
        } catch (retryError) {
            log.warn?.(`rbmq worker: could not schedule retry for ${qname}/${id}: ${retryError.message}`);
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
