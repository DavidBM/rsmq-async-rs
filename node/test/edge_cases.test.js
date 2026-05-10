import { describe, it, expect, afterEach } from "vitest";
import { makeClient } from "./helper.js";
import { InvalidFormatError, InvalidValueError } from "../src/index.js";

describe("edge cases", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("empty body round-trips", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "");
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.toString()).toBe("");
    });

    it("body that looks like packed format round-trips", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const body = "999\n123\n456\nfake-body";
        await env.rbmq.sendMessage("q", body);
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.toString()).toBe(body);
    });

    it("body with all 256 byte values", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const body = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
        await env.rbmq.sendMessage("q", body);
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.equals(body)).toBe(true);
    });

    it("body with unicode round-trips", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "héllo 世界 🦀");
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.toString("utf8")).toBe("héllo 世界 🦀");
    });

    it("body at exact maxsize accepted", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { maxsize: 1024 });
        await env.rbmq.sendMessage("q", "x".repeat(1024));
    });

    it("body one over maxsize rejected", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { maxsize: 1024 });
        await expect(env.rbmq.sendMessage("q", "x".repeat(1025))).rejects.toThrow();
    });

    it("queue name 160 chars accepted, 161 rejected", async () => {
        env = makeClient();
        await env.rbmq.createQueue("a".repeat(160));
        await expect(env.rbmq.createQueue("a".repeat(161))).rejects.toThrow(InvalidFormatError);
    });

    it("queue name with - and _ accepted", async () => {
        env = makeClient();
        await env.rbmq.createQueue("my-queue_v2");
        await env.rbmq.sendMessage("my-queue_v2", "ok");
        const m = await env.rbmq.receiveMessage("my-queue_v2");
        expect(m.message.toString()).toBe("ok");
    });

    it("queue name with dot rejected", async () => {
        env = makeClient();
        await expect(env.rbmq.createQueue("bad.name")).rejects.toThrow(InvalidFormatError);
    });

    it("queue name with colon rejected", async () => {
        env = makeClient();
        await expect(env.rbmq.createQueue("bad:name")).rejects.toThrow(InvalidFormatError);
    });

    it("maxsize -1 accepts huge messages", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { maxsize: -1 });
        const body = Buffer.alloc(500_000, 0x42);
        await env.rbmq.sendMessage("q", body);
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.length).toBe(500_000);
    });

    it("maxsize below 1024 rejected", async () => {
        env = makeClient();
        await expect(env.rbmq.createQueue("q", { maxsize: 1023 })).rejects.toThrow(InvalidValueError);
    });

    it("maxsize above 65536 rejected", async () => {
        env = makeClient();
        await expect(env.rbmq.createQueue("q", { maxsize: 65537 })).rejects.toThrow(InvalidValueError);
    });

    it("vt=0 makes message immediately redeliverable", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 0 });
        await env.rbmq.sendMessage("q", "hi");
        const m1 = await env.rbmq.receiveMessage("q");
        const m2 = await env.rbmq.receiveMessage("q");
        expect(m1.id).toBe(m2.id);
        expect(m2.rc).toBe(2n);
    });

    it("explicit vt overrides queue default", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 60_000 });
        await env.rbmq.sendMessage("q", "hi");
        const m1 = await env.rbmq.receiveMessage("q", { vt: 0 });
        expect(m1).not.toBeNull();
        const m2 = await env.rbmq.receiveMessage("q");
        expect(m2).not.toBeNull();
    });

    it("ID alphabet is lowercase hex", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        for (let i = 0; i < 50; i++) {
            const id = await env.rbmq.sendMessage("q", "x");
            expect(id).toMatch(/^[0-9a-f]{32}$/);
        }
    });

    it("IDs are unique across many sends", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const ids = new Set();
        for (let i = 0; i < 500; i++) {
            ids.add(await env.rbmq.sendMessage("q", "x"));
        }
        expect(ids.size).toBe(500);
    });

    it("set_queue_attributes on missing queue errors", async () => {
        env = makeClient();
        await expect(env.rbmq.setQueueAttributes("ghost", { vt: 1000 })).rejects.toThrow();
    });

    it("get_queue_attributes hidden vs total counts", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 60_000 });
        for (let i = 0; i < 5; i++) await env.rbmq.sendMessage("q", "x");
        await env.rbmq.receiveMessage("q");
        await env.rbmq.receiveMessage("q");
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.msgs).toBe(5n);
        expect(a.hiddenmsgs).toBe(2n);
    });

    it("delete_queue then re-create gives fresh state", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        for (let i = 0; i < 3; i++) await env.rbmq.sendMessage("q", "x");
        await env.rbmq.deleteQueue("q");
        await env.rbmq.createQueue("q");
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.totalsent).toBe(0n);
        expect(a.msgs).toBe(0n);
    });
});
