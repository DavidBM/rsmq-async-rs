//! Retry pattern using `rc` (receive count) and `change_message_visibility`.
//!
//! When processing fails, we **don't** delete the message. Instead we push
//! its visibility further out so it reappears later — and we use `msg.rc`
//! (incremented by Redis on every receive) to compute exponential backoff
//! and to give up after a few tries.
//!
//!     cargo run --example retry_with_backoff

use rbmq::{Rbmq, RbmqConnection, RbmqError};
use std::time::Duration;

const MAX_ATTEMPTS: u64 = 3;

async fn process(payload: &str) -> Result<(), &'static str> {
    if payload.contains("bad") {
        Err("simulated failure")
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), RbmqError> {
    let mut rbmq = Rbmq::new(Default::default()).await?;
    let qname = "example_retry";

    let _ = rbmq.delete_queue(qname).await;
    rbmq.create_queue(qname, Some(Duration::from_secs(2)), None, None)
        .await?;

    rbmq.send_message(qname, "good-1", None).await?;
    rbmq.send_message(qname, "bad-2", None).await?;

    let mut idle = 0;
    loop {
        let Some(msg) = rbmq.receive_message::<String>(qname, None).await? else {
            idle += 1;
            if idle > 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        idle = 0;

        match process(&msg.message).await {
            Ok(()) => {
                println!("ok: {}", msg.message);
                rbmq.delete_message(qname, &msg.id).await?;
            }
            Err(e) if msg.rc >= MAX_ATTEMPTS => {
                println!(
                    "giving up on {} after {} attempts: {e}",
                    msg.message, msg.rc
                );
                rbmq.delete_message(qname, &msg.id).await?;
            }
            Err(e) => {
                let backoff = Duration::from_secs(2u64.pow(msg.rc as u32));
                println!(
                    "retry {} after {e}: visible again in {:?}",
                    msg.message, backoff
                );
                rbmq.change_message_visibility(qname, &msg.id, backoff)
                    .await?;
            }
        }
    }

    rbmq.delete_queue(qname).await?;
    Ok(())
}
