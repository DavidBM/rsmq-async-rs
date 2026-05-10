// Error classes used across the rbmq client. Plain ES classes — easy to catch
// with `instanceof` and easy to inspect in a debugger.

export class RbmqError extends Error {
    constructor(message) {
        super(message);
        this.name = "RbmqError";
    }
}

export class QueueNotFoundError extends RbmqError {
    constructor(qname) {
        super(`Queue not found: ${qname}`);
        this.name = "QueueNotFoundError";
        this.qname = qname;
    }
}

export class QueueExistsError extends RbmqError {
    constructor(qname) {
        super(`Queue already exists: ${qname}`);
        this.name = "QueueExistsError";
        this.qname = qname;
    }
}

export class MessageTooLongError extends RbmqError {
    constructor() {
        super("Message exceeds the queue's maxsize");
        this.name = "MessageTooLongError";
    }
}

export class InvalidFormatError extends RbmqError {
    constructor(value) {
        super(`Invalid format: ${value}`);
        this.name = "InvalidFormatError";
        this.value = value;
    }
}

export class InvalidValueError extends RbmqError {
    constructor(value, min, max) {
        super(`Value ${value} out of range [${min}, ${max}]`);
        this.name = "InvalidValueError";
        this.value = value;
        this.min = min;
        this.max = max;
    }
}

/**
 * Translate ioredis errors back into typed rbmq errors. Lua's error_reply prefixes
 * with "ERR " when there's no space, so the marker name lands in the message.
 *
 * @param {Error} err
 * @returns {Error}
 */
export function mapRedisError(err) {
    const msg = err && err.message ? err.message : "";
    if (msg.includes("QueueNotFound")) return new QueueNotFoundError("");
    if (msg.includes("QueueExists")) return new QueueExistsError("");
    if (msg.includes("MessageTooLong")) return new MessageTooLongError();
    return err;
}
