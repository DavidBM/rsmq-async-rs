import { describe, it, expect, afterEach } from "vitest";
import { makeClient, sleep } from "./helper.js";
import { createWorker } from "../src/index.js";

const SILENT = { warn: () => {}, error: () => {} };

describe("worker helper", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("processes a message and deletes on success", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "hello");

        let calls = 0;
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 20,
            heartbeatIntervalMs: 100,
            visibilityExtensionMs: 5000,
            routes: {
                q: async (msg) => {
                    calls++;
                    expect(msg.message.toString()).toBe("hello");
                },
            },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        // Wait until handled.
        for (let i = 0; i < 50; i++) {
            if (calls > 0) break;
            await sleep(20);
        }
        ac.abort();
        await run;
        expect(calls).toBe(1);
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.msgs).toBe(0n);
    });

    it("routes by queue name", async () => {
        env = makeClient();
        await env.rbmq.createQueue("a");
        await env.rbmq.createQueue("b");
        await env.rbmq.sendMessage("a", "from-a");
        await env.rbmq.sendMessage("b", "from-b");

        let aCount = 0, bCount = 0;
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 20,
            routes: {
                a: async () => { aCount++; },
                b: async () => { bCount++; },
            },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        for (let i = 0; i < 100; i++) {
            if (aCount && bCount) break;
            await sleep(20);
        }
        ac.abort();
        await run;
        expect(aCount).toBe(1);
        expect(bCount).toBe(1);
    });

    it("handler error leaves the message for redelivery (no DLQ)", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 100 });
        await env.rbmq.sendMessage("q", "boom");

        let calls = 0;
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 20,
            routes: {
                q: async () => {
                    calls++;
                    throw new Error("nope");
                },
            },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        await sleep(500);
        ac.abort();
        await run;
        expect(calls).toBeGreaterThanOrEqual(2);
    });

    it("DLQ routes after maxFailures exceeded", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 100 });
        await env.rbmq.createQueue("dead");
        await env.rbmq.sendMessage("q", "fail");

        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 20,
            dlq: { queue: "dead", maxFailures: 0 },
            routes: {
                q: async () => { throw new Error("boom"); },
            },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        await sleep(300);
        ac.abort();
        await run;
        const a = await env.rbmq.getQueueAttributes("dead");
        expect(a.msgs).toBe(1n);
    });

    it("self-loop DLQ rejected at construction", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        expect(() => createWorker({
            client: env.rbmq,
            routes: { q: async () => {} },
            dlq: { queue: "q", maxFailures: 0 },
        })).toThrow();
    });

    it("empty routes rejected", async () => {
        env = makeClient();
        expect(() => createWorker({ client: env.rbmq, routes: {} })).toThrow();
    });

    it("shutdown signal exits promptly even mid-poll-wait", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 60_000, // would idle for a minute
            routes: { q: async () => {} },
        });
        const ac = new AbortController();
        const start = Date.now();
        const run = worker.runUntil(ac.signal);
        setTimeout(() => ac.abort(), 50);
        await run;
        expect(Date.now() - start).toBeLessThan(2000);
    });

    it("decode-tolerant: handlers receive raw Buffer in m.message", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", Buffer.from([0xff, 0x00, 0x42]));

        let captured;
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 20,
            routes: {
                q: async (m) => { captured = m.message; },
            },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        for (let i = 0; i < 50; i++) {
            if (captured) break;
            await sleep(20);
        }
        ac.abort();
        await run;
        expect(captured.equals(Buffer.from([0xff, 0x00, 0x42]))).toBe(true);
    });

    it("running concurrently rejected", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 60_000,
            routes: { q: async () => {} },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        await expect(worker.runUntil(ac.signal)).rejects.toThrow(/already running/);
        ac.abort();
        await run;
    });

    it("global DLQ used when no per-route override", async () => {
        env = makeClient();
        await env.rbmq.createQueue("a", { vt: 100 });
        await env.rbmq.createQueue("b", { vt: 100 });
        await env.rbmq.createQueue("global");
        await env.rbmq.sendMessage("a", "x");
        await env.rbmq.sendMessage("b", "y");
        const worker = createWorker({
            client: env.rbmq,
            log: SILENT,
            pollIntervalMs: 20,
            dlq: { queue: "global", maxFailures: 0 },
            routes: {
                a: async () => { throw new Error("boom"); },
                b: async () => { throw new Error("boom"); },
            },
        });
        const ac = new AbortController();
        const run = worker.runUntil(ac.signal);
        await sleep(300);
        ac.abort();
        await run;
        const a = await env.rbmq.getQueueAttributes("global");
        expect(a.msgs).toBe(2n);
    });
});
