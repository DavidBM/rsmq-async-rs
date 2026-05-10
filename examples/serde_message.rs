//! Sending and receiving typed messages via the `serde` feature.
//!
//! Two equivalent styles are demonstrated:
//!
//! 1. `Json<T>` wrapper composed with the existing `send_message` / `receive_message` API.
//! 2. The `RbmqJsonExt` extension trait's `send_json` / `receive_json` methods.
//!
//! Run with the `serde` feature enabled:
//!
//!     cargo run --example serde_message --features serde

use rbmq::{Json, Rbmq, RbmqConnection, RbmqError, RbmqJsonExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
    description: String,
}

#[tokio::main]
async fn main() -> Result<(), RbmqError> {
    let mut rbmq = Rbmq::new(Default::default()).await?;
    let qname = "example_serde";

    let _ = rbmq.delete_queue(qname).await;
    rbmq.create_queue(qname, None, None, None).await?;

    // (1) Json<T> wrapper
    rbmq.send_message(
        qname,
        Json(Job {
            id: 1,
            description: "via wrapper".into(),
        }),
        None,
    )
    .await?;

    if let Some(msg) = rbmq.receive_message::<Json<Job>>(qname, None).await? {
        println!("wrapper:   {:?}", msg.message.0);
        rbmq.delete_message(qname, &msg.id).await?;
    }

    // (2) Extension trait
    rbmq.send_json(
        qname,
        &Job {
            id: 2,
            description: "via ext trait".into(),
        },
        None,
    )
    .await?;

    if let Some(msg) = rbmq.receive_json::<Job>(qname, None).await? {
        println!("ext trait: {:?}", msg.message);
        rbmq.delete_message(qname, &msg.id).await?;
    }

    rbmq.delete_queue(qname).await?;
    Ok(())
}
