<!-- cargo-sync-readme start -->

# RSMQ in async Rust

RSMQ port to async rust. RSMQ is a simple redis queue system that works in any
redis v2.6+. It contains the same methods as the original one
in [https://github.com/smrchy/rsmq](https://github.com/smrchy/rsmq)

This crate uses async in the implementation. If you want to use it in your sync
code you can use tokio/async_std "block_on" method. Async was used in order to
simplify the code and allow 1-to-1 port oft he JS code.

[![Crates.io](https://img.shields.io/crates/v/rsmq_async)](https://crates.io/crates/rsmq_async)
[![Crates.io](https://img.shields.io/crates/l/rsmq_async)](https://choosealicense.com/licenses/mit/)
[![dependency status](https://deps.rs/crate/rsmq_async/2.1.0/status.svg)](https://deps.rs/crate/rsmq_async)
[![Docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square)](https://docs.rs/rsmq_async)

```rust,no_run

use rsmq_async::{Rsmq, RsmqError, RsmqConnection};

let mut rsmq = Rsmq::new(Default::default()).await?;

let message = rsmq.receive_message("myqueue", None).await?;

if let Some(message) = message {
    rsmq.delete_message("myqueue", &message.id).await?;
}


```

Main object documentation are in: <a href="struct.Rsmq.html">Rsmq</a> and
<a href="struct.PooledRsmq.html">PooledRsmq</a> and they both implement the trait
<a href="trait.RsmqConnection.html">RsmqConnection</a> where you can see all the RSMQ
methods. Make sure you always import the trait <a href="trait.RsmqConnection.html">RsmqConnection</a>.

## Installation

Check [https://crates.io/crates/rsmq_async](https://crates.io/crates/rsmq_async)

## Realtime

When [initializing](#initialize) RSMQ you can enable the realtime PUBLISH for
new messages. On every new message that gets sent to RSQM via `sendMessage` a
Redis PUBLISH will be issued to `{rsmq.ns}:rt:{qname}`. So, you can subscribe 
to it using redis-rs library directly.

### How to use the realtime option

Besides the PUBLISH when a new message is sent to RSMQ nothing else will happen.
Your app could use the Redis SUBSCRIBE command to be notified of new messages
and issue a `receiveMessage` then. However make sure not to listen with multiple
workers for new messages with SUBSCRIBE to prevent multiple simultaneous
`receiveMessage` calls.

## Guarantees

If you want to implement "at least one delivery" guarantee, you need to receive
the messages using "receive_message" and then, once the message is successfully
processed, delete it with "delete_message".

## Connection Pool

If you want to use a connection pool, just use <a href="struct.PooledRsmq.html">PooledRsmq</a>
instad of Rsmq. It implements the RsmqConnection trait as the normal Rsmq.

If you want to accept any of both implementation, just accept the trait 
<a href="trait.RsmqConnection.html">RsmqConnection</a>

## Executor compatibility

Since version 0.16 [where this pull request was merged](https://github.com/mitsuhiko/redis-rs/issues/280)
redis-rs dependency supports tokio and async_std executors. By default it will 
guess what you are using when creating the connection. You can check 
[redis-rs](https://github.com/mitsuhiko/redis-rs/blob/master/Cargo.toml) `Cargo.tolm` for
the flags `async-std-comp` and `tokio-comp` in order to choose one or the other. If you don't select 
any it should be able to automatically choose the correct one. 

## Example

```rust,no_run
use rsmq_async::{Rsmq, RsmqConnection};

async fn it_works() {
    let mut rsmq = Rsmq::new(Default::default())
        .await
        .expect("connection failed");

    rsmq.create_queue("myqueue", None, None, None)
        .await
        .expect("failed to create queue");

    rsmq.send_message("myqueue", "testmessage", None)
        .await
        .expect("failed to send message");

    let message = rsmq
        .receive_message("myqueue", None)
        .await
        .expect("cannot receive message");

    if let Some(message) = message {
        rsmq.delete_message("myqueue", &message.id).await;
    }
}

```


<!-- cargo-sync-readme end -->
