import { describe, it, expect, afterEach } from "vitest";
import { makeClient } from "./helper.js";
import { MessageTooLongError, QueueNotFoundError } from "../src/index.js";

describe("batch send/receive", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("sendMessageBatch returns one id per input", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const ids = await env.rbmq.sendMessageBatch("q", ["a", "b", "c"]);
        expect(ids).toHaveLength(3);
        for (const id of ids) {
            expect(id).toMatch(/^[0-9a-f]{32}$/);
        }
    });

    it("sendMessageBatch with empty array returns empty list", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        expect(await env.rbmq.sendMessageBatch("q", [])).toEqual([]);
    });

    it("receiveMessageBatch up to N", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessageBatch("q", ["a", "b", "c"]);
        const got = await env.rbmq.receiveMessageBatch("q", 3);
        expect(got).toHaveLength(3);
        const bodies = got.map((m) => m.message.toString()).sort();
        expect(bodies).toEqual(["a", "b", "c"]);
    });

    it("receiveMessageBatch with maxCount=0 returns []", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "hi");
        expect(await env.rbmq.receiveMessageBatch("q", 0)).toEqual([]);
    });

    it("receiveMessageBatch returns fewer messages than maxCount when queue has fewer", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "only-one");
        const got = await env.rbmq.receiveMessageBatch("q", 10);
        expect(got).toHaveLength(1);
    });

    it("batch with one oversized message: nothing lands (atomic)", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { maxsize: 1024 });
        const payloads = ["ok-1", "ok-2", "x".repeat(2000), "ok-3"];
        await expect(env.rbmq.sendMessageBatch("q", payloads)).rejects.toThrow(MessageTooLongError);
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.totalsent).toBe(0n);
    });

    it("batch send to non-existent queue errors", async () => {
        env = makeClient();
        await expect(env.rbmq.sendMessageBatch("ghost", ["a"])).rejects.toThrow(QueueNotFoundError);
    });

    it("batch send increments totalsent atomically", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessageBatch("q", Array(10).fill("x"));
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.totalsent).toBe(10n);
        expect(a.msgs).toBe(10n);
    });
});
