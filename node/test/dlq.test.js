import { describe, it, expect, afterEach } from "vitest";
import { makeClient } from "./helper.js";
import { InvalidFormatError, QueueNotFoundError } from "../src/index.js";

describe("move_message and receive_message_or_dlq", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("moveMessage transfers preserving rc and fr", async () => {
        env = makeClient();
        await env.rbmq.createQueue("src", { vt: 0 });
        await env.rbmq.createQueue("dst");
        await env.rbmq.sendMessage("src", "hi");
        await env.rbmq.receiveMessage("src");
        await env.rbmq.receiveMessage("src");
        const m = await env.rbmq.receiveMessage("src");
        expect(m.rc).toBe(3n);
        const fr = m.fr;

        expect(await env.rbmq.moveMessage("src", m.id, "dst")).toBe(true);
        const dm = await env.rbmq.receiveMessage("dst");
        // rc continues from src + 1 receive on dst.
        expect(dm.rc).toBe(4n);
        expect(dm.fr).toBe(fr);
    });

    it("moveMessage to self rejected", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "x");
        const m = await env.rbmq.receiveMessage("q");
        await expect(env.rbmq.moveMessage("q", m.id, "q")).rejects.toThrow(InvalidFormatError);
    });

    it("moveMessage on unknown id returns false", async () => {
        env = makeClient();
        await env.rbmq.createQueue("src");
        await env.rbmq.createQueue("dst");
        const r = await env.rbmq.moveMessage("src", "ffffffffffffffffffffffffffffffff", "dst");
        expect(r).toBe(false);
    });

    it("receiveMessageOrDlq with maxReceives=0 routes immediately", async () => {
        env = makeClient();
        await env.rbmq.createQueue("src");
        await env.rbmq.createQueue("dlq");
        await env.rbmq.sendMessage("src", "hi");
        const m = await env.rbmq.receiveMessageOrDlq("src", "dlq", 0);
        expect(m).toBeNull();
        const d = await env.rbmq.receiveMessage("dlq");
        expect(d.message.toString()).toBe("hi");
    });

    it("receiveMessageOrDlq with maxReceives=N delivers N times then routes", async () => {
        env = makeClient();
        await env.rbmq.createQueue("src", { vt: 0 });
        await env.rbmq.createQueue("dlq");
        await env.rbmq.sendMessage("src", "hi");
        for (let i = 0; i < 3; i++) {
            const m = await env.rbmq.receiveMessageOrDlq("src", "dlq", 3);
            expect(m).not.toBeNull();
        }
        const m4 = await env.rbmq.receiveMessageOrDlq("src", "dlq", 3);
        expect(m4).toBeNull();
        const d = await env.rbmq.receiveMessage("dlq");
        expect(d.message.toString()).toBe("hi");
    });

    it("receiveMessageOrDlq self-loop rejected", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await expect(env.rbmq.receiveMessageOrDlq("q", "q", 1)).rejects.toThrow(InvalidFormatError);
    });

    it("receiveMessageOrDlq from unknown source errors", async () => {
        env = makeClient();
        await env.rbmq.createQueue("dlq");
        await expect(env.rbmq.receiveMessageOrDlq("ghost", "dlq", 1)).rejects.toThrow(QueueNotFoundError);
    });
});
