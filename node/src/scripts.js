// Loads the shared Lua scripts from /scripts at the repo root and registers
// them on an ioredis client via defineCommand. Each script becomes a method
// on the client (e.g. client.rbmqSendMessage(...)).
//
// Why defineCommand: ioredis hashes the script once, calls EVALSHA, and falls
// back to EVAL automatically on NOSCRIPT. No manual cache management needed.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SCRIPTS_DIR = join(__dirname, "..", "..", "scripts");

function load(name) {
    return readFileSync(join(SCRIPTS_DIR, `${name}.lua`), "utf8");
}

/**
 * Each entry: [methodName, scriptName, numKeys].
 * methodName is the snake_case_-style name we attach to the redis client via
 * defineCommand. numKeys is fixed (Lua scripts have a stable key arity).
 *
 * sendMessageBatch uses numKeys=2 but variadic ARGV; ioredis copes via
 * defineCommand without a fixed argv count.
 */
const SCRIPT_DEFS = [
    ["rbmqCreateQueue", "createQueue", 2],
    ["rbmqDeleteQueue", "deleteQueue", 2],
    ["rbmqDeleteMessage", "deleteMessage", 1],
    ["rbmqSetQueueAttributes", "setQueueAttributes", 1],
    ["rbmqGetQueueAttributes", "getQueueAttributes", 2],
    ["rbmqChangeMessageVisibility", "changeMessageVisibility", 1],
    ["rbmqSendMessage", "sendMessage", 2],
    ["rbmqSendMessageBatch", "sendMessageBatch", 2],
    ["rbmqReceiveMessage", "receiveMessage", 1],
    ["rbmqReceiveMessageBatch", "receiveMessageBatch", 1],
    ["rbmqReceiveMessageOrDlq", "receiveMessageOrDlq", 2],
    ["rbmqMoveMessage", "moveMessage", 2],
];

/**
 * Register all rbmq scripts on the given ioredis instance. Idempotent.
 *
 * @param {import("ioredis").Redis} redis
 */
export function attachScripts(redis) {
    for (const [methodName, scriptName, numberOfKeys] of SCRIPT_DEFS) {
        if (typeof redis[methodName] === "function") continue; // already attached
        redis.defineCommand(methodName, {
            numberOfKeys,
            lua: load(scriptName),
        });
    }
}
