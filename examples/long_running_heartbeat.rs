//! Extending a message's visibility timeout while it's being processed.
//!
//! If your worker takes longer than the queue's `vt`, the message will be
//! redelivered to another worker — likely producing a duplicate. The fix is
//! to call `change_message_visibility` periodically while you work, like a
//! heartbeat.
//!
//!     cargo run --example long_running_heartbeat

use rbmq::{Rbmq, RbmqConnection, RbmqError};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), RbmqError> {
    let mut rbmq = Rbmq::new(Default::default()).await?;
    let qname = "example_heartbeat";

    let _ = rbmq.delete_queue(qname).await;
    rbmq.create_queue(qname, Some(Duration::from_secs(5)), None, None)
        .await?;
    rbmq.send_message(qname, "long-job", None).await?;

    let msg = rbmq
        .receive_message::<String>(qname, None)
        .await?
        .expect("message should be present");

    let total_work = Duration::from_secs(12);
    let heartbeat_every = Duration::from_secs(3);
    let extend_to = Duration::from_secs(10);

    let work = tokio::time::sleep(total_work);
    tokio::pin!(work);

    let mut ticker = tokio::time::interval(heartbeat_every);
    ticker.tick().await; // skip the immediate tick

    loop {
        tokio::select! {
            _ = &mut work => break,
            _ = ticker.tick() => {
                println!("heartbeat: extending visibility by {extend_to:?}");
                rbmq.change_message_visibility(qname, &msg.id, extend_to).await?;
            }
        }
    }

    println!("done — deleting {}", msg.id);
    rbmq.delete_message(qname, &msg.id).await?;

    rbmq.delete_queue(qname).await?;
    Ok(())
}
