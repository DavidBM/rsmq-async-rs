// rbmq client: a thin typed wrapper over Lua scripts shared with the Rust crate.
//
// Design rules (so this file doesn't drift):
//   - createClient(redis, options) returns a plain object with methods.
//   - No singletons, no module-level state, no monkey-patching.
//   - The Redis connection is dependency-injected; we never open or close it.
//   - Each public method is one EVALSHA via ioredis defineCommand.

import { randomBytes } from "node:crypto";
import { attachScripts } from "./scripts.js";
import {
    mapRedisError,
    QueueNotFoundError,
    InvalidFormatError,
    InvalidValueError,
} from "./errors.js";

const USE_DEFAULT = "-1";
// Defensive cap on duration arguments (~100 years).
const MAX_DURATION_MS = 100 * 365 * 24 * 60 * 60 * 1000;
const QNAME_RE = /^[A-Za-z0-9_-]+$/;

/**
 * @typedef {Object} RbmqOptions
 * @property {string} [ns="rbmq"]      Key namespace.
 * @property {boolean} [realtime=false] If true, emits a PUBLISH on every send.
 */

/**
 * @typedef {Object} RbmqMessage
 * @property {string} id            32-char lowercase hex.
 * @property {Buffer} message       Raw body bytes. Decode in user code.
 * @property {bigint} rc            Receive count.
 * @property {bigint} fr            First-received timestamp (microseconds since epoch).
 * @property {bigint} sent          Send timestamp (microseconds since epoch).
 */

/**
 * @typedef {Object} RbmqQueueAttributes
 * @property {bigint} vt
 * @property {bigint} delay
 * @property {bigint} maxsize
 * @property {bigint} totalrecv
 * @property {bigint} totalsent
 * @property {bigint} created
 * @property {bigint} modified
 * @property {bigint} msgs
 * @property {bigint} hiddenmsgs
 */

function validateName(name) {
    if (typeof name !== "string" || name.length === 0 || name.length > 160) {
        throw new InvalidFormatError(name);
    }
    if (!QNAME_RE.test(name)) throw new InvalidFormatError(name);
}

function checkRange(value, min, max) {
    if (value < min || value > max) throw new InvalidValueError(value, min, max);
}

function durationToMs(d, defaultMs) {
    if (d == null) return defaultMs;
    if (typeof d === "number") {
        if (!Number.isFinite(d) || d < 0) throw new InvalidValueError(d, 0, MAX_DURATION_MS);
        return Math.floor(d);
    }
    if (typeof d === "bigint") return Number(d);
    throw new InvalidValueError(d, 0, MAX_DURATION_MS);
}

function durationArg(d) {
    return d == null ? USE_DEFAULT : String(durationToMs(d, 0));
}

function makeId() {
    return randomBytes(16).toString("hex");
}

function bodyToBuffer(message) {
    if (Buffer.isBuffer(message)) return message;
    if (typeof message === "string") return Buffer.from(message, "utf8");
    if (message instanceof Uint8Array) return Buffer.from(message);
    throw new TypeError("message must be a Buffer, Uint8Array, or string");
}

// Parses the script-returned 6-tuple [found, id, body, rc, fr, sent] into an
// RbmqMessage or null. `body`, `fr`, `sent` may come back as buffers or strings
// depending on the connection; ioredis can be configured for buffers per-call.
function parseMessageReply(reply) {
    if (!reply) return null;
    const [found, id, body, rc, fr, sent] = reply;
    // `found` is 1/0 as integer or "1"/"0" as bulk string in some configs.
    const truthy = found === 1 || found === 1n || found === "1" || found === true;
    if (!truthy) return null;
    return {
        id: typeof id === "string" ? id : id.toString("utf8"),
        message: Buffer.isBuffer(body) ? body : Buffer.from(String(body)),
        rc: BigInt(rc),
        fr: BigInt(typeof fr === "string" ? fr : fr.toString()),
        sent: BigInt(typeof sent === "string" ? sent : sent.toString()),
    };
}

/**
 * Create an rbmq client backed by a user-provided ioredis instance.
 *
 * The redis instance MUST be configured to return Buffers (`{ keyPrefix: ..., showFriendlyErrorStack: ... }`
 * is fine, but for binary-safe message bodies create the Redis connection without
 * stringifying — i.e. don't use ioredis's default UTF-8 mode if you store binary
 * payloads). The client uses .callBuffer() variants where binary safety matters.
 *
 * @param {import("ioredis").Redis} redis
 * @param {RbmqOptions} [options]
 */
export function createClient(redis, options = {}) {
    const ns = options.ns || "rbmq";
    const realtime = !!options.realtime;
    attachScripts(redis);

    const cfgKey = (q) => `${ns}:${q}:cfg`;
    const zsetKey = (q) => `${ns}:${q}`;
    const rtChannel = (q) => `${ns}:rt:${q}`;
    const queuesKey = () => `${ns}:QUEUES`;

    return {
        ns,

        async createQueue(qname, opts = {}) {
            validateName(qname);
            const hiddenMs = durationToMs(opts.vt, 30_000);
            const delayMs = durationToMs(opts.delay, 0);
            const maxsize = opts.maxsize == null ? 65536 : Number(opts.maxsize);

            checkRange(hiddenMs, 0, MAX_DURATION_MS);
            checkRange(delayMs, 0, MAX_DURATION_MS);
            if (maxsize !== -1) checkRange(maxsize, 1024, 65536);

            let result;
            try {
                result = await redis.rbmqCreateQueue(
                    cfgKey(qname),
                    queuesKey(),
                    qname,
                    String(hiddenMs),
                    String(delayMs),
                    String(maxsize),
                );
            } catch (err) {
                throw mapRedisError(err);
            }
            if (Number(result) === 0) {
                const e = new Error("queue exists");
                e.name = "QueueExistsError";
                throw e;
            }
        },

        async deleteQueue(qname) {
            let result;
            try {
                result = await redis.rbmqDeleteQueue(zsetKey(qname), queuesKey(), qname);
            } catch (err) {
                throw mapRedisError(err);
            }
            if (Number(result) === 0) throw new QueueNotFoundError(qname);
        },

        async listQueues() {
            const list = await redis.smembers(queuesKey());
            return list || [];
        },

        async deleteMessage(qname, id) {
            try {
                const r = await redis.rbmqDeleteMessage(zsetKey(qname), id);
                return Number(r) === 1;
            } catch (err) {
                throw mapRedisError(err);
            }
        },

        async getQueueAttributes(qname) {
            let result;
            try {
                result = await redis.rbmqGetQueueAttributes(cfgKey(qname), zsetKey(qname));
            } catch (err) {
                throw mapRedisError(err);
            }
            // Returns a flat array: [sec, usec, vt, delay, maxsize, totalrecv, totalsent, created, modified, msgs, hiddenmsgs]
            const [, , vt, delay, maxsize, totalrecv, totalsent, created, modified, msgs, hiddenmsgs] = result;
            if (vt == null) throw new QueueNotFoundError(qname);
            const big = (v) => BigInt(v == null ? 0 : v);
            return {
                vt: big(vt),
                delay: big(delay),
                maxsize: big(maxsize),
                totalrecv: big(totalrecv),
                totalsent: big(totalsent),
                created: big(created),
                modified: big(modified),
                msgs: big(msgs),
                hiddenmsgs: big(hiddenmsgs),
            };
        },

        async setQueueAttributes(qname, attrs) {
            const vtArg = attrs.vt == null ? "" : String(durationToMs(attrs.vt, 0));
            const delayArg = attrs.delay == null ? "" : String(durationToMs(attrs.delay, 0));
            let maxArg = "";
            if (attrs.maxsize != null) {
                const m = Number(attrs.maxsize);
                if (m !== -1) checkRange(m, 1024, 65536);
                maxArg = String(m);
            }
            if (vtArg !== "") checkRange(Number(vtArg), 0, MAX_DURATION_MS);
            if (delayArg !== "") checkRange(Number(delayArg), 0, MAX_DURATION_MS);

            let exists;
            try {
                exists = await redis.rbmqSetQueueAttributes(cfgKey(qname), vtArg, delayArg, maxArg);
            } catch (err) {
                throw mapRedisError(err);
            }
            if (Number(exists) === 0) throw new QueueNotFoundError(qname);
            return this.getQueueAttributes(qname);
        },

        async changeMessageVisibility(qname, messageId, hidden) {
            const ms = durationToMs(hidden, 30_000);
            checkRange(ms, 0, MAX_DURATION_MS);
            try {
                await redis.rbmqChangeMessageVisibility(zsetKey(qname), messageId, String(ms));
            } catch (err) {
                throw mapRedisError(err);
            }
        },

        async sendMessage(qname, message, opts = {}) {
            const body = bodyToBuffer(message);
            const id = makeId();
            const delay = opts.delay == null ? USE_DEFAULT : String(durationToMs(opts.delay, 0));
            if (delay !== USE_DEFAULT) checkRange(Number(delay), 0, MAX_DURATION_MS);
            const realtimeFlag = realtime ? "1" : "0";

            try {
                // Use callBuffer-style invocation to keep the body binary-safe.
                await redis.rbmqSendMessageBuffer(
                    Buffer.from(zsetKey(qname)),
                    Buffer.from(rtChannel(qname)),
                    Buffer.from(id),
                    Buffer.from(delay),
                    Buffer.from(realtimeFlag),
                    body,
                );
            } catch (err) {
                throw mapRedisError(err);
            }
            return id;
        },

        async sendMessageBatch(qname, messages, opts = {}) {
            if (!Array.isArray(messages) || messages.length === 0) return [];
            const delay = opts.delay == null ? USE_DEFAULT : String(durationToMs(opts.delay, 0));
            if (delay !== USE_DEFAULT) checkRange(Number(delay), 0, MAX_DURATION_MS);
            const realtimeFlag = realtime ? "1" : "0";

            const ids = messages.map(() => makeId());
            const args = [
                Buffer.from(zsetKey(qname)),
                Buffer.from(rtChannel(qname)),
                Buffer.from(delay),
                Buffer.from(realtimeFlag),
            ];
            for (let i = 0; i < messages.length; i++) {
                args.push(Buffer.from(ids[i]));
                args.push(bodyToBuffer(messages[i]));
            }
            try {
                await redis.rbmqSendMessageBatchBuffer(...args);
            } catch (err) {
                throw mapRedisError(err);
            }
            return ids;
        },

        async receiveMessage(qname, opts = {}) {
            const hidden = durationArg(opts.vt);
            try {
                const reply = await redis.rbmqReceiveMessageBuffer(
                    Buffer.from(zsetKey(qname)),
                    Buffer.from(hidden),
                    Buffer.from("false"),
                );
                return parseMessageReply(reply);
            } catch (err) {
                throw mapRedisError(err);
            }
        },

        async popMessage(qname) {
            try {
                const reply = await redis.rbmqReceiveMessageBuffer(
                    Buffer.from(zsetKey(qname)),
                    Buffer.from(USE_DEFAULT),
                    Buffer.from("true"),
                );
                return parseMessageReply(reply);
            } catch (err) {
                throw mapRedisError(err);
            }
        },

        async receiveMessageBatch(qname, maxCount, opts = {}) {
            if (maxCount === 0) return [];
            const hidden = durationArg(opts.vt);
            let raw;
            try {
                raw = await redis.rbmqReceiveMessageBatchBuffer(
                    Buffer.from(zsetKey(qname)),
                    Buffer.from(hidden),
                    Buffer.from("false"),
                    Buffer.from(String(maxCount)),
                );
            } catch (err) {
                throw mapRedisError(err);
            }
            if (!raw) return [];
            return raw
                .map(parseMessageRowReply)
                .filter((m) => m !== null);
        },

        async receiveMessageOrDlq(qname, dlq, maxReceives, opts = {}) {
            validateName(qname);
            validateName(dlq);
            if (qname === dlq) throw new InvalidFormatError(`qname == dlq: ${qname}`);
            const hidden = durationArg(opts.vt);
            try {
                const reply = await redis.rbmqReceiveMessageOrDlqBuffer(
                    Buffer.from(zsetKey(qname)),
                    Buffer.from(zsetKey(dlq)),
                    Buffer.from(hidden),
                    Buffer.from(String(maxReceives)),
                );
                return parseMessageReply(reply);
            } catch (err) {
                throw mapRedisError(err);
            }
        },

        async moveMessage(src, messageId, dst) {
            validateName(src);
            validateName(dst);
            if (src === dst) throw new InvalidFormatError(`src == dst: ${src}`);
            try {
                const r = await redis.rbmqMoveMessage(zsetKey(src), zsetKey(dst), messageId);
                return Number(r) === 1;
            } catch (err) {
                throw mapRedisError(err);
            }
        },
    };
}

// receiveMessageBatch returns an array of [id, body, rc, fr, sent] rows (not the
// 6-tuple receiveMessage shape). Parse one row.
function parseMessageRowReply(row) {
    if (!row || row.length < 5) return null;
    const [id, body, rc, fr, sent] = row;
    return {
        id: typeof id === "string" ? id : id.toString("utf8"),
        message: Buffer.isBuffer(body) ? body : Buffer.from(String(body)),
        rc: BigInt(rc),
        fr: BigInt(typeof fr === "string" ? fr : fr.toString()),
        sent: BigInt(typeof sent === "string" ? sent : sent.toString()),
    };
}
