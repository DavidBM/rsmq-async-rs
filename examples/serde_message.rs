//! Sending and receiving typed messages via the `serde` feature.
//!
//! Two equivalent styles are demonstrated:
//!
//! 1. `Json<T>` wrapper composed with the existing `send_message` / `receive_message` API.
//! 2. The `RsmqJsonExt` extension trait's `send_json` / `receive_json` methods.
//!
//! Run with the `serde` feature enabled:
//!
//!     cargo run --example serde_message --features serde

use rsmq_async::{Json, Rsmq, RsmqConnection, RsmqError, RsmqJsonExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
    description: String,
}

#[tokio::main]
async fn main() -> Result<(), RsmqError> {
    let mut rsmq = Rsmq::new(Default::default()).await?;
    let qname = "example_serde";

    let _ = rsmq.delete_queue(qname).await;
    rsmq.create_queue(qname, None, None, None).await?;

    // (1) Json<T> wrapper
    rsmq.send_message(
        qname,
        Json(Job {
            id: 1,
            description: "via wrapper".into(),
        }),
        None,
    )
    .await?;

    if let Some(msg) = rsmq.receive_message::<Json<Job>>(qname, None).await? {
        println!("wrapper:   {:?}", msg.message.0);
        rsmq.delete_message(qname, &msg.id).await?;
    }

    // (2) Extension trait
    rsmq.send_json(
        qname,
        &Job {
            id: 2,
            description: "via ext trait".into(),
        },
        None,
    )
    .await?;

    if let Some(msg) = rsmq.receive_json::<Job>(qname, None).await? {
        println!("ext trait: {:?}", msg.message);
        rsmq.delete_message(qname, &msg.id).await?;
    }

    rsmq.delete_queue(qname).await?;
    Ok(())
}
