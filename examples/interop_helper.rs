//! Long-running JSON-line RPC binary used by the Node-side interop tests.
//!
//! Protocol: one JSON request per stdin line, one JSON response per stdout line.
//! Connects to Redis from REDIS_HOST / REDIS_PORT (defaults 127.0.0.1:6379).
//! Each request must include `ns` (the rbmq namespace) so a single helper
//! process can serve multiple test fixtures.
//!
//! Requests (selected — see match below for the full list):
//!   {"op": "create_queue", "ns": "...", "qname": "..."}
//!   {"op": "send_message", "ns": "...", "qname": "...", "body": "..."}
//!   {"op": "receive_message", "ns": "...", "qname": "..."}
//!   {"op": "ping"}
//!
//! Responses are always {"ok": true, ...} or {"ok": false, "error": "..."}

use rbmq::{Rbmq, RbmqConnection, RbmqOptions};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request {
    Ping,
    CreateQueue {
        ns: String,
        qname: String,
        #[serde(default)]
        vt_ms: Option<u64>,
        #[serde(default)]
        delay_ms: Option<u64>,
        #[serde(default)]
        maxsize: Option<i64>,
    },
    DeleteQueue {
        ns: String,
        qname: String,
    },
    SendMessage {
        ns: String,
        qname: String,
        body: String,
    },
    ReceiveMessage {
        ns: String,
        qname: String,
    },
    PopMessage {
        ns: String,
        qname: String,
    },
    DeleteMessage {
        ns: String,
        qname: String,
        id: String,
    },
    GetQueueAttributes {
        ns: String,
        qname: String,
    },
    MoveMessage {
        ns: String,
        src: String,
        dst: String,
        id: String,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
enum Reply {
    Ok(serde_json::Value),
    Err { ok: bool, error: String },
}

async fn rbmq_for(ns: &str) -> Result<Rbmq, String> {
    let host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);
    Rbmq::new(RbmqOptions {
        host,
        port,
        ns: ns.to_string(),
        ..Default::default()
    })
    .await
    .map_err(|e| format!("{e:?}"))
}

async fn handle(req: Request) -> Reply {
    use serde_json::json;
    match req {
        Request::Ping => Reply::Ok(json!({ "ok": true, "pong": true })),
        Request::CreateQueue { ns, qname, vt_ms, delay_ms, maxsize } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            let vt = vt_ms.map(std::time::Duration::from_millis);
            let delay = delay_ms.map(std::time::Duration::from_millis);
            match r.create_queue(&qname, vt, delay, maxsize).await {
                Ok(()) => Reply::Ok(json!({ "ok": true })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::DeleteQueue { ns, qname } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.delete_queue(&qname).await {
                Ok(()) => Reply::Ok(json!({ "ok": true })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::SendMessage { ns, qname, body } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.send_message(&qname, body, None).await {
                Ok(id) => Reply::Ok(json!({ "ok": true, "id": id })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::ReceiveMessage { ns, qname } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.receive_message::<String>(&qname, None).await {
                Ok(Some(m)) => Reply::Ok(json!({
                    "ok": true,
                    "id": m.id,
                    "message": m.message,
                    "rc": m.rc,
                    "fr": m.fr.to_string(),
                    "sent": m.sent.to_string(),
                })),
                Ok(None) => Reply::Ok(json!({ "ok": true, "empty": true })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::PopMessage { ns, qname } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.pop_message::<String>(&qname).await {
                Ok(Some(m)) => Reply::Ok(json!({
                    "ok": true,
                    "id": m.id,
                    "message": m.message,
                    "rc": m.rc,
                    "fr": m.fr.to_string(),
                    "sent": m.sent.to_string(),
                })),
                Ok(None) => Reply::Ok(json!({ "ok": true, "empty": true })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::DeleteMessage { ns, qname, id } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.delete_message(&qname, &id).await {
                Ok(deleted) => Reply::Ok(json!({ "ok": true, "deleted": deleted })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::GetQueueAttributes { ns, qname } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.get_queue_attributes(&qname).await {
                Ok(a) => Reply::Ok(json!({
                    "ok": true,
                    "vt_ms": a.vt.as_millis() as u64,
                    "delay_ms": a.delay.as_millis() as u64,
                    "maxsize": a.maxsize,
                    "totalrecv": a.totalrecv,
                    "totalsent": a.totalsent,
                    "msgs": a.msgs,
                    "hiddenmsgs": a.hiddenmsgs,
                })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
        Request::MoveMessage { ns, src, dst, id } => {
            let mut r = match rbmq_for(&ns).await {
                Ok(r) => r,
                Err(e) => return Reply::Err { ok: false, error: e },
            };
            match r.move_message(&src, &id, &dst).await {
                Ok(moved) => Reply::Ok(serde_json::json!({ "ok": true, "moved": moved })),
                Err(e) => Reply::Err { ok: false, error: format!("{e:?}") },
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Request>(trimmed) {
            Ok(req) => handle(req).await,
            Err(e) => Reply::Err { ok: false, error: format!("parse: {e}") },
        };
        let json = serde_json::to_string(&reply)?;
        writeln!(stdout, "{json}")?;
        stdout.flush()?;
    }
    Ok(())
}
