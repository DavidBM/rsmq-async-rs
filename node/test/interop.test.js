// Cross-language interop: Rust producer ↔ Node consumer (and vice versa).
// Both sides talk to the same Redis through the same Lua scripts; what we're
// proving is wire compatibility, not behavioral equivalence (the same scripts
// guarantee the latter for free).
//
// We spawn a single rust helper process (examples/interop_helper.rs) once per
// test run and pipe newline-delimited JSON commands through it.

import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { existsSync } from "node:fs";
import { makeClient, sleep } from "./helper.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const RUST_BIN_DEBUG = join(__dirname, "..", "..", "target", "debug", "examples", "interop_helper");
const RUST_BIN_RELEASE = join(__dirname, "..", "..", "target", "release", "examples", "interop_helper");

function locateRustHelper() {
    if (existsSync(RUST_BIN_RELEASE)) return RUST_BIN_RELEASE;
    if (existsSync(RUST_BIN_DEBUG)) return RUST_BIN_DEBUG;
    return null;
}

class RustHelper {
    constructor(binPath) {
        this.proc = spawn(binPath, [], {
            stdio: ["pipe", "pipe", "pipe"],
            env: { ...process.env },
        });
        this.queue = [];
        this.buffer = "";
        this.proc.stdout.setEncoding("utf8");
        this.proc.stdout.on("data", (chunk) => {
            this.buffer += chunk;
            let idx;
            while ((idx = this.buffer.indexOf("\n")) >= 0) {
                const line = this.buffer.slice(0, idx).trim();
                this.buffer = this.buffer.slice(idx + 1);
                if (!line) continue;
                const next = this.queue.shift();
                if (next) {
                    try {
                        next.resolve(JSON.parse(line));
                    } catch (err) {
                        next.reject(err);
                    }
                }
            }
        });
        this.proc.on("exit", (code) => {
            for (const p of this.queue) {
                p.reject(new Error(`rust helper exited prematurely (code ${code})`));
            }
            this.queue = [];
        });
    }

    async call(req) {
        return new Promise((resolve, reject) => {
            this.queue.push({ resolve, reject });
            this.proc.stdin.write(JSON.stringify(req) + "\n");
        });
    }

    async stop() {
        this.proc.stdin.end();
        await new Promise((resolve) => {
            this.proc.once("exit", () => resolve());
            // Belt-and-suspenders timeout.
            setTimeout(() => {
                this.proc.kill("SIGTERM");
                resolve();
            }, 2000);
        });
    }
}

describe("Rust ↔ Node interop", () => {
    let helper;
    beforeAll(() => {
        const bin = locateRustHelper();
        if (!bin) {
            throw new Error(
                `interop_helper binary not found. Run 'cargo build --example interop_helper --features serde,tokio-comp' from the repo root.`,
            );
        }
        helper = new RustHelper(bin);
    });
    afterAll(async () => helper && helper.stop());

    let env;
    afterEach(async () => env && env.cleanup());

    it("rust helper responds to ping", async () => {
        const r = await helper.call({ op: "ping" });
        expect(r.ok).toBe(true);
    });

    it("rust producer → node consumer: simple body", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        const sent = await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "from-rust" });
        expect(sent.ok).toBe(true);
        const msg = await env.rbmq.receiveMessage("q");
        expect(msg.message.toString()).toBe("from-rust");
        expect(msg.id).toBe(sent.id);
    });

    it("node producer → rust consumer: simple body", async () => {
        env = makeClient();
        const r1 = await helper.call({ op: "create_queue", ns: env.ns, qname: "q" });
        expect(r1.ok).toBe(true);
        const id = await env.rbmq.sendMessage("q", "from-node");
        const got = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(got.ok).toBe(true);
        expect(got.id).toBe(id);
        expect(got.message).toBe("from-node");
    });

    it("rust producer → node consumer: empty body", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "" });
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.toString()).toBe("");
    });

    it("node producer → rust consumer: queue created by node", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 5000 });
        await env.rbmq.sendMessage("q", "shared-state");
        const got = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(got.ok).toBe(true);
        expect(got.message).toBe("shared-state");
    });

    it("rust creates queue, node sends, rust receives", async () => {
        env = makeClient();
        await helper.call({ op: "create_queue", ns: env.ns, qname: "q" });
        await env.rbmq.sendMessage("q", "node→rust");
        const got = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(got.message).toBe("node→rust");
    });

    it("totalsent counter agrees across languages", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        for (let i = 0; i < 3; i++) {
            await env.rbmq.sendMessage("q", `node-${i}`);
        }
        for (let i = 0; i < 4; i++) {
            await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: `rust-${i}` });
        }
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.totalsent).toBe(7n);
        const ra = await helper.call({ op: "get_queue_attributes", ns: env.ns, qname: "q" });
        expect(ra.totalsent).toBe(7);
    });

    it("delete_message: node receives, rust deletes", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "shared");
        const m = await env.rbmq.receiveMessage("q");
        const r = await helper.call({ op: "delete_message", ns: env.ns, qname: "q", id: m.id });
        expect(r.deleted).toBe(true);
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.msgs).toBe(0n);
    });

    it("rust pop deletes, node sees empty queue afterwards", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "x");
        const popped = await helper.call({ op: "pop_message", ns: env.ns, qname: "q" });
        expect(popped.message).toBe("x");
        const m = await env.rbmq.receiveMessage("q");
        expect(m).toBeNull();
    });

    it("rc and fr metadata preserved across languages", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 0 });
        await env.rbmq.sendMessage("q", "tracked");
        // Rust receives twice — rc -> 2.
        const r1 = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        const r2 = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(r1.rc).toBe(1);
        expect(r2.rc).toBe(2);
        expect(r1.fr).toBe(r2.fr);
        // Node sees rc=3 next.
        const m = await env.rbmq.receiveMessage("q");
        expect(m.rc).toBe(3n);
        expect(m.fr.toString()).toBe(r1.fr);
    });

    it("queue attributes match across languages", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { vt: 7000, delay: 1500, maxsize: 4096 });
        const node = await env.rbmq.getQueueAttributes("q");
        const rust = await helper.call({ op: "get_queue_attributes", ns: env.ns, qname: "q" });
        expect(rust.vt_ms).toBe(Number(node.vt));
        expect(rust.delay_ms).toBe(Number(node.delay));
        expect(rust.maxsize).toBe(Number(node.maxsize));
    });

    it("move_message: rust moves, node reads from destination", async () => {
        env = makeClient();
        await env.rbmq.createQueue("src");
        await env.rbmq.createQueue("dst");
        await env.rbmq.sendMessage("src", "moveme");
        const m = await env.rbmq.receiveMessage("src");
        const r = await helper.call({ op: "move_message", ns: env.ns, src: "src", dst: "dst", id: m.id });
        expect(r.moved).toBe(true);
        const dm = await env.rbmq.receiveMessage("dst");
        expect(dm.message.toString()).toBe("moveme");
    });

    it("queue created by rust accepts node sends and vice versa repeatedly", async () => {
        env = makeClient();
        await helper.call({ op: "create_queue", ns: env.ns, qname: "q" });
        await env.rbmq.sendMessage("q", "n1");
        await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "r1" });
        await env.rbmq.sendMessage("q", "n2");
        await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "r2" });
        // Drain alternately.
        const m1 = await env.rbmq.receiveMessage("q");
        const m2 = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        const m3 = await env.rbmq.receiveMessage("q");
        const m4 = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        const collected = [m1.message.toString(), m2.message, m3.message.toString(), m4.message].sort();
        expect(collected).toEqual(["n1", "n2", "r1", "r2"]);
    });

    it("delete_queue from rust prevents future node sends", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "x");
        const r = await helper.call({ op: "delete_queue", ns: env.ns, qname: "q" });
        expect(r.ok).toBe(true);
        await expect(env.rbmq.sendMessage("q", "y")).rejects.toThrow();
    });

    it("delete_queue from node prevents future rust sends", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.deleteQueue("q");
        const r = await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "x" });
        expect(r.ok).toBe(false);
        expect(r.error).toMatch(/QueueNotFound/);
    });

    it("body with newlines round-trips rust→node", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "line1\nline2\n" });
        const m = await env.rbmq.receiveMessage("q");
        expect(m.message.toString()).toBe("line1\nline2\n");
    });

    it("body with newlines round-trips node→rust", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        await env.rbmq.sendMessage("q", "a\nb\nc");
        const r = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(r.message).toBe("a\nb\nc");
    });

    it("rust creates queue with custom maxsize, node enforces it", async () => {
        env = makeClient();
        await helper.call({ op: "create_queue", ns: env.ns, qname: "q", maxsize: 1024 });
        await expect(env.rbmq.sendMessage("q", "x".repeat(2048))).rejects.toThrow();
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.maxsize).toBe(1024n);
    });

    it("node creates queue with delay, rust observes delayed visibility", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q", { delay: 200 });
        await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: "later" });
        const tooEarly = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(tooEarly.empty).toBe(true);
        await sleep(250);
        const got = await helper.call({ op: "receive_message", ns: env.ns, qname: "q" });
        expect(got.message).toBe("later");
    });

    it("100 message round trip rust↔node preserves count", async () => {
        env = makeClient();
        await env.rbmq.createQueue("q");
        // Half from rust, half from node.
        for (let i = 0; i < 50; i++) {
            await helper.call({ op: "send_message", ns: env.ns, qname: "q", body: `r${i}` });
        }
        for (let i = 0; i < 50; i++) {
            await env.rbmq.sendMessage("q", `n${i}`);
        }
        const a = await env.rbmq.getQueueAttributes("q");
        expect(a.msgs).toBe(100n);
        // Drain via pop; both languages consume.
        const seen = new Set();
        for (let i = 0; i < 50; i++) {
            const m = await env.rbmq.popMessage("q");
            seen.add(m.message.toString());
        }
        for (let i = 0; i < 50; i++) {
            const r = await helper.call({ op: "pop_message", ns: env.ns, qname: "q" });
            seen.add(r.message);
        }
        expect(seen.size).toBe(100);
    });
});
