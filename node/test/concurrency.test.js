import { describe, it, expect, afterEach } from "vitest";
import { makeClient, sleep } from "./helper.js";

describe("concurrency and uniqueness", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("two parallel receivers never see the same message", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const N = 100;
        for (let i = 0; i < N; i++) await env.rbmq.sendMessage("q", `m${i}`);

        const env2 = makeClient({ ns: env.ns });
        try {
            const worker = async (client) => {
                const got = [];
                for (let i = 0; i < N; i++) {
                    const m = await client.receiveMessage("q");
                    if (m) got.push(m.message.toString());
                }
                return got;
            };
            const [a, b] = await Promise.all([worker(env.rbmq), worker(env2.rbmq)]);
            const all = [...a, ...b];
            expect(new Set(all).size).toBe(all.length);
        } finally {
            await env2.cleanup();
        }
    });

    it("parallel senders all messages land", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const env2 = makeClient({ ns: env.ns });
        try {
            const PER = 50;
            await Promise.all([
                (async () => {
                    for (let i = 0; i < PER; i++) await env.rbmq.sendMessage("q", `a${i}`);
                })(),
                (async () => {
                    for (let i = 0; i < PER; i++) await env2.rbmq.sendMessage("q", `b${i}`);
                })(),
            ]);
            const a = await env.rbmq.getQueueAttributes("q");
            expect(a.totalsent).toBe(BigInt(PER * 2));
            expect(a.msgs).toBe(BigInt(PER * 2));
        } finally {
            await env2.cleanup();
        }
    });

    it("totalsent and totalrecv stay consistent under interleaving", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 0 });
        for (let i = 0; i < 30; i++) await env.rbmq.sendMessage("q", `m${i}`);

        let received = 0;
        for (let i = 0; i < 30; i++) {
            const m = await env.rbmq.receiveMessage("q");
            if (m) {
                received++;
                await env.rbmq.deleteMessage("q", m.id);
            }
        }
        expect(received).toBe(30);
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.totalsent).toBe(30n);
        expect(a.totalrecv).toBe(30n);
        expect(a.msgs).toBe(0n);
    });

    it("delete unknown id returns false", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        expect(await env.rbmq.deleteMessage("q", "f".repeat(32))).toBe(false);
    });

    it("delete twice returns false the second time", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "x");
        const m = await env.rbmq.receiveMessage("q");
        expect(await env.rbmq.deleteMessage("q", m.id)).toBe(true);
        expect(await env.rbmq.deleteMessage("q", m.id)).toBe(false);
    });

    it("sequential sends deliver in score order", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        for (let i = 0; i < 30; i++) {
            await env.rbmq.sendMessage("q", `m-${String(i).padStart(3, "0")}`);
            // tiny gap so microsecond scores differ
            await sleep(1);
        }
        for (let i = 0; i < 30; i++) {
            const m = await env.rbmq.receiveMessage("q");
            expect(m.message.toString()).toBe(`m-${String(i).padStart(3, "0")}`);
            await env.rbmq.deleteMessage("q", m.id);
        }
    });
});
