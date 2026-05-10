import { describe, it, expect, afterEach } from "vitest";
import { makeClient, sleep } from "./helper.js";
import { QueueNotFoundError, QueueExistsError, MessageTooLongError, InvalidFormatError } from "../src/index.js";

describe("basic send/receive/delete", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("creates a queue, sends a message, receives, deletes", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const id = await env.rbmq.sendMessage("q", "hello");
        const msg = await env.rbmq.receiveMessage("q");
        expect(msg).not.toBeNull();
        expect(msg.id).toBe(id);
        expect(msg.message.toString()).toBe("hello");
        expect(msg.rc).toBe(1n);
        expect(await env.rbmq.deleteMessage("q", msg.id)).toBe(true);
        expect(await env.rbmq.deleteMessage("q", msg.id)).toBe(false);
    });

    it("returns null on receive when queue is empty", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const m = await env.rbmq.receiveMessage("q");
        expect(m).toBeNull();
    });

    it("pop deletes and returns the message", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "hi");
        const m = await env.rbmq.popMessage("q");
        expect(m).not.toBeNull();
        expect(m.message.toString()).toBe("hi");
        expect(await env.rbmq.popMessage("q")).toBeNull();
    });

    it("creates the same queue twice errors with QueueExistsError", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await expect(env.rbmq.createQueue("q")).rejects.toThrow(/exists/i);
    });

    it("send to unknown queue errors with QueueNotFoundError", async () => {
        env = makeClient();
        await expect(env.rbmq.sendMessage("ghost", "x")).rejects.toThrow(QueueNotFoundError);
    });

    it("receive from unknown queue errors with QueueNotFoundError", async () => {
        env = makeClient();
        await expect(env.rbmq.receiveMessage("ghost")).rejects.toThrow(QueueNotFoundError);
    });

    it("oversized message rejected with MessageTooLongError", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { maxsize: 1024 });
        await expect(env.rbmq.sendMessage("q", "x".repeat(2000))).rejects.toThrow(MessageTooLongError);
    });

    it("invalid queue name rejected", async () => {
        env = makeClient();
        await expect(env.rbmq.createQueue("bad name")).rejects.toThrow(InvalidFormatError);
        await expect(env.rbmq.createQueue("bad.name")).rejects.toThrow(InvalidFormatError);
        await expect(env.rbmq.createQueue("")).rejects.toThrow(InvalidFormatError);
    });

    it("get_queue_attributes reports counters", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 0 });
        for (let i = 0; i < 5; i++) await env.rbmq.sendMessage("q", "x");
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.totalsent).toBe(5n);
        expect(a.msgs).toBe(5n);
    });

    it("delete queue removes everything", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "x");
        await env.rbmq.deleteQueue("q");
        await expect(env.rbmq.getQueueAttributes("q")).rejects.toThrow(QueueNotFoundError);
    });

    it("list_queues lists current queues", async () => {
        env = makeClient();
        await env.rbmq.createQueue("a");
        await env.rbmq.createQueue("b");
        const names = (await env.rbmq.listQueues()).sort();
        expect(names).toEqual(["a", "b"]);
    });

    it("rc increments and fr stays stable on redelivery", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 0 });
        await env.rbmq.sendMessage("q", "hi");
        const m1 = await env.rbmq.receiveMessage("q");
        const m2 = await env.rbmq.receiveMessage("q");
        const m3 = await env.rbmq.receiveMessage("q");
        expect(m1.rc).toBe(1n);
        expect(m2.rc).toBe(2n);
        expect(m3.rc).toBe(3n);
        expect(m1.fr).toBe(m2.fr);
        expect(m2.fr).toBe(m3.fr);
    });

    it("sent timestamp is microseconds since epoch", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "hi");
        const m = await env.rbmq.receiveMessage("q");
        // microseconds: 16 digits.
        expect(m.sent).toBeGreaterThan(1_000_000_000_000_000n);
    });

    it("change_message_visibility shortens hidden window to zero", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 60_000 });
        await env.rbmq.sendMessage("q", "hi");
        const m = await env.rbmq.receiveMessage("q");
        await env.rbmq.changeMessageVisibility("q", m.id, 0);
        const m2 = await env.rbmq.receiveMessage("q");
        expect(m2).not.toBeNull();
        expect(m2.id).toBe(m.id);
    });

    it("delayed send is invisible until delay passes", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "hi", { delay: 200 });
        const m1 = await env.rbmq.receiveMessage("q");
        expect(m1).toBeNull();
        await sleep(250);
        const m2 = await env.rbmq.receiveMessage("q");
        expect(m2).not.toBeNull();
    });

    it("delete message returns true then false", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "hi");
        const m = await env.rbmq.receiveMessage("q");
        expect(await env.rbmq.deleteMessage("q", m.id)).toBe(true);
        expect(await env.rbmq.deleteMessage("q", m.id)).toBe(false);
    });

    it("set_queue_attributes updates fields independently", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const a1 = await env.rbmq.setQueueAttributes("q", { vt: 60_000 });
        expect(a1.vt).toBe(60_000n);
        const a2 = await env.rbmq.setQueueAttributes("q", { delay: 5000 });
        expect(a2.delay).toBe(5000n);
        expect(a2.vt).toBe(60_000n);
        const a3 = await env.rbmq.setQueueAttributes("q", { maxsize: 2048 });
        expect(a3.maxsize).toBe(2048n);
    });

    it("body with newlines and binary bytes round-trips", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { maxsize: -1 });
        const body = Buffer.from([0x00, 0xff, 0x0a, 0x0a, 0x42]);
        await env.rbmq.sendMessage("q", body);
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.equals(body)).toBe(true);
    });
});
