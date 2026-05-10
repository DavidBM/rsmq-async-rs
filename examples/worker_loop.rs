//! Minimal polling worker.
//!
//! Sends a few messages, then loops `receive_message` + `delete_message`
//! until the queue is empty. The `receive_message` call hides the message
//! for `vt` seconds; calling `delete_message` after successful processing
//! is what acknowledges the work.
//!
//! Run with a Redis at `localhost:6379`:
//!
//!     cargo run --example worker_loop

use rsmq_async::{Rsmq, RsmqConnection, RsmqError};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), RsmqError> {
    let mut rsmq = Rsmq::new(Default::default()).await?;
    let qname = "example_worker_loop";

    let _ = rsmq.delete_queue(qname).await;
    rsmq.create_queue(qname, Some(Duration::from_secs(30)), None, None)
        .await?;

    for i in 0..5 {
        rsmq.send_message(qname, format!("job-{i}"), None).await?;
    }

    loop {
        match rsmq.receive_message::<String>(qname, None).await? {
            Some(msg) => {
                println!("processing {} (received {} time(s))", msg.message, msg.rc);
                rsmq.delete_message(qname, &msg.id).await?;
            }
            None => {
                println!("queue drained");
                break;
            }
        }
    }

    rsmq.delete_queue(qname).await?;
    Ok(())
}
