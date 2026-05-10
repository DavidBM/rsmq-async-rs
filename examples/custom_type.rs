//! Sending and receiving a custom message type.
//!
//! Implement `Into<RedisBytes>` to send and `TryFrom<RedisBytes, Error = Vec<u8>>`
//! to receive. The wire format here is plain JSON encoded by hand, so the
//! example stays dependency-free; in real code you'd typically use serde_json.
//!
//!     cargo run --example custom_type

use rbmq::{RedisBytes, Rbmq, RbmqConnection, RbmqError};

#[derive(Debug)]
struct Job {
    id: u64,
    payload: String,
}

impl From<Job> for RedisBytes {
    fn from(job: Job) -> RedisBytes {
        let payload = job.payload.replace('"', "\\\"");
        format!(r#"{{"id":{},"payload":"{}"}}"#, job.id, payload).into()
    }
}

impl TryFrom<RedisBytes> for Job {
    type Error = Vec<u8>;

    fn try_from(bytes: RedisBytes) -> Result<Self, Self::Error> {
        let raw = bytes.into_bytes();
        let s = std::str::from_utf8(&raw).map_err(|_| raw.clone())?;
        let id_start = s.find(r#""id":"#).ok_or_else(|| raw.clone())? + 5;
        let id_end = s[id_start..].find(',').ok_or_else(|| raw.clone())? + id_start;
        let id: u64 = s[id_start..id_end].parse().map_err(|_| raw.clone())?;
        let payload_start = s.find(r#""payload":""#).ok_or_else(|| raw.clone())? + 11;
        let payload_end = s.rfind('"').ok_or_else(|| raw.clone())?;
        Ok(Job {
            id,
            payload: s[payload_start..payload_end].replace("\\\"", "\""),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), RbmqError> {
    let mut rbmq = Rbmq::new(Default::default()).await?;
    let qname = "example_custom_type";

    let _ = rbmq.delete_queue(qname).await;
    rbmq.create_queue(qname, None, None, None).await?;

    rbmq.send_message(
        qname,
        Job {
            id: 42,
            payload: "hello".into(),
        },
        None,
    )
    .await?;

    if let Some(msg) = rbmq.receive_message::<Job>(qname, None).await? {
        println!("received: {:?}", msg.message);
        rbmq.delete_message(qname, &msg.id).await?;
    }

    rbmq.delete_queue(qname).await?;
    Ok(())
}
