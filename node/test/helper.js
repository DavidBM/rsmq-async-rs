// Shared test helpers. No singletons — every test gets its own ns + client.

import Redis from "ioredis";
import { randomBytes } from "node:crypto";
import { createClient } from "../src/index.js";

const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT || 6379);

/**
 * Build an ioredis instance and an rbmq client tied to a unique namespace.
 * Returns { redis, rbmq, ns, cleanup } — call cleanup() in afterEach.
 *
 * @param {{ realtime?: boolean, ns?: string }} [opts]
 */
export function makeClient(opts = {}) {
    const ns = opts.ns || `rbmqtest_${randomBytes(8).toString("hex")}`;
    const redis = new Redis({
        host: REDIS_HOST,
        port: REDIS_PORT,
        maxRetriesPerRequest: 1,
        lazyConnect: false,
    });
    const rbmq = createClient(redis, { ns, realtime: !!opts.realtime });
    return {
        redis,
        rbmq,
        ns,
        async cleanup() {
            // Best-effort: drop any keys we created under this namespace.
            try {
                const keys = await redis.keys(`${ns}:*`);
                if (keys.length > 0) await redis.del(...keys);
            } catch {
                // Ignore — connection may already be closed.
            }
            await redis.quit().catch(() => {});
        },
    };
}

/** Sleep for `ms` milliseconds. */
export function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}
