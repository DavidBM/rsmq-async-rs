//! Worker helper: queue-name router, automatic heartbeat, optional realtime wake-up.
//!
//! Sends a few messages on two different queues, then runs a [`Worker`] with one handler
//! per queue for ~3 seconds. The handlers print, the worker deletes successful messages,
//! and `run_until` exits cleanly.
//!
//!     cargo run --example worker_helper

use rsmq_async::{Rsmq, RsmqConnection, RsmqError, RsmqMessage, RsmqOptions, Worker};
use std::convert::Infallible;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), RsmqError> {
    let mut producer = Rsmq::new(Default::default()).await?;

    let _ = producer.delete_queue("emails").await;
    let _ = producer.delete_queue("billing").await;
    producer.create_queue("emails", None, None, None).await?;
    producer.create_queue("billing", None, None, None).await?;

    for i in 0..3 {
        producer
            .send_message("emails", format!("subject-{i}"), None)
            .await?;
    }
    for i in 0..2 {
        producer
            .send_message("billing", format!("invoice-{i}"), None)
            .await?;
    }

    let worker = Worker::builder(RsmqOptions::default())
        .poll_interval(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_secs(5))
        .visibility_extension(Duration::from_secs(30))
        // .use_realtime(true) // opt in if you sent the messages with `RsmqOptions { realtime: true, .. }`
        .route("emails", |msg: RsmqMessage<String>| async move {
            println!("[emails]  {}", msg.message);
            Ok::<(), Infallible>(())
        })
        .route("billing", |msg: RsmqMessage<String>| async move {
            println!("[billing] {}", msg.message);
            Ok::<(), Infallible>(())
        })
        .build()
        .await?;

    println!("worker running for ~3s...");
    worker
        .run_until(tokio::time::sleep(Duration::from_secs(3)))
        .await?;

    let _ = producer.delete_queue("emails").await;
    let _ = producer.delete_queue("billing").await;
    Ok(())
}
