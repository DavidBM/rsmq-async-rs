import { describe, it, expect, afterEach } from "vitest";
import { makeClient, sleep } from "./helper.js";

describe("visibility timing", () => {
    let env;
    afterEach(async () => env && env.cleanup());

    it("vt actually hides for the requested duration", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 150 });
        await env.rbmq.sendMessage("q", "hi");
        const m1 = await env.rbmq.receiveMessage("q");
        expect(m1).not.toBeNull();
        const m2 = await env.rbmq.receiveMessage("q");
        expect(m2).toBeNull();
        await sleep(200);
        const m3 = await env.rbmq.receiveMessage("q");
        expect(m3).not.toBeNull();
    });

    it("delayed send stays invisible until delay elapses", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "later", { delay: 200 });
        expect(await env.rbmq.receiveMessage("q")).toBeNull();
        await sleep(250);
        expect(await env.rbmq.receiveMessage("q")).not.toBeNull();
    });

    it("change_message_visibility extends in-flight window", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 100 });
        await env.rbmq.sendMessage("q", "hi");
        const m = await env.rbmq.receiveMessage("q");
        await env.rbmq.changeMessageVisibility("q", m.id, 5000);
        await sleep(150);
        expect(await env.rbmq.receiveMessage("q")).toBeNull();
    });

    it("change_message_visibility on unknown id is silent no-op", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        // Should not error.
        await env.rbmq.changeMessageVisibility("q", "f".repeat(32), 1000);
    });

    it("vt is independent per message", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 60_000 });
        await env.rbmq.sendMessage("q", "a");
        await env.rbmq.sendMessage("q", "b");
        const ma = await env.rbmq.receiveMessage("q");
        const mb = await env.rbmq.receiveMessage("q");
        await env.rbmq.changeMessageVisibility("q", ma.id, 0);
        const got = await env.rbmq.receiveMessage("q");
        expect(got.id).toBe(ma.id);
        // mb still hidden
        expect(await env.rbmq.receiveMessage("q")).toBeNull();
        expect(mb).not.toBeNull();
    });

    it("pop does not leave message for redelivery", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 0 });
        await env.rbmq.sendMessage("q", "x");
        const m = await env.rbmq.popMessage("q");
        expect(m).not.toBeNull();
        expect(await env.rbmq.receiveMessage("q")).toBeNull();
    });
});
