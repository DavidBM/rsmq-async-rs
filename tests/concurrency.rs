mod support;

use rbmq::{Rbmq, RbmqConnection as _};
use std::collections::HashSet;
use std::time::Duration;
use support::*;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

async fn new_rbmq(ctx: &TestContext) -> Rbmq {
    Rbmq::new_with_connection(ctx.async_connection().await.unwrap(), false, Some(&ctx.ns))
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Two consumers can never receive the same message
// ---------------------------------------------------------------------------

#[test]
fn parallel_receivers_never_deliver_duplicates() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        const N: usize = 100;
        for i in 0..N {
            q.send_message("q", format!("msg-{i}"), None).await.unwrap();
        }

        // Two independent connections, both receiving in a tight loop.
        let mut q1 = new_rbmq(&ctx).await;
        let mut q2 = new_rbmq(&ctx).await;

        let h1 = tokio::spawn(async move {
            let mut got: Vec<String> = Vec::new();
            for _ in 0..N {
                if let Some(m) = q1.receive_message::<String>("q", None).await.unwrap() {
                    got.push(m.message);
                }
            }
            got
        });
        let h2 = tokio::spawn(async move {
            let mut got: Vec<String> = Vec::new();
            for _ in 0..N {
                if let Some(m) = q2.receive_message::<String>("q", None).await.unwrap() {
                    got.push(m.message);
                }
            }
            got
        });

        let mut all = h1.await.unwrap();
        all.extend(h2.await.unwrap());
        let unique: HashSet<_> = all.iter().collect();
        // Every receive returned a distinct message; together they covered all N.
        assert_eq!(unique.len(), all.len(), "duplicates: {} vs {}", unique.len(), all.len());
        assert_eq!(unique.len(), N);
    });
}

#[test]
fn parallel_senders_all_messages_land() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        const PER: usize = 50;
        let q1 = new_rbmq(&ctx).await;
        let q2 = new_rbmq(&ctx).await;

        let h1 = tokio::spawn(async move {
            let mut q = q1;
            for i in 0..PER {
                q.send_message("q", format!("a-{i}"), None).await.unwrap();
            }
        });
        let h2 = tokio::spawn(async move {
            let mut q = q2;
            for i in 0..PER {
                q.send_message("q", format!("b-{i}"), None).await.unwrap();
            }
        });
        h1.await.unwrap();
        h2.await.unwrap();

        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, (PER * 2) as u64);
        assert_eq!(attrs.msgs, (PER * 2) as u64);
    });
}

#[test]
fn parallel_send_and_receive_no_loss() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();

        const N: usize = 50;

        let sender_q = new_rbmq(&ctx).await;
        let h_send = tokio::spawn(async move {
            let mut q = sender_q;
            for i in 0..N {
                q.send_message("q", format!("m-{i}"), None).await.unwrap();
            }
        });

        let mut q_recv = new_rbmq(&ctx).await;
        let mut received = HashSet::new();
        // Drain in a loop until we've seen N distinct messages or sender is done + queue empty.
        let mut tries = 0;
        while received.len() < N && tries < 1000 {
            if let Some(m) = q_recv.receive_message::<String>("q", None).await.unwrap() {
                received.insert(m.message);
                q_recv.delete_message("q", &m.id).await.unwrap();
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                tries += 1;
            }
        }
        h_send.await.unwrap();
        assert_eq!(received.len(), N);
    });
}

// ---------------------------------------------------------------------------
// Atomic batch send: all-or-none
// ---------------------------------------------------------------------------

#[test]
fn batch_send_with_oversize_member_lands_nothing() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(1024)).await.unwrap();

        let mut payloads: Vec<String> = (0..5).map(|i| format!("ok-{i}")).collect();
        payloads.push("x".repeat(2048)); // oversized

        let r = q.send_message_batch("q", payloads, None).await;
        assert!(r.is_err());
        let attrs = q.get_queue_attributes("q").await.unwrap();
        // Atomic: nothing should have been written.
        assert_eq!(attrs.msgs, 0);
        assert_eq!(attrs.totalsent, 0);
    });
}

// ---------------------------------------------------------------------------
// FIFO ordering under sequential sends (same-microsecond is exceedingly rare with us scores)
// ---------------------------------------------------------------------------

#[test]
fn sequential_sends_deliver_in_order() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        for i in 0..50 {
            q.send_message("q", format!("m-{i:03}"), None).await.unwrap();
        }

        for i in 0..50 {
            let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
            assert_eq!(m.message, format!("m-{i:03}"));
            q.delete_message("q", &m.id).await.unwrap();
        }
    });
}

#[test]
fn batch_send_then_receive_returns_same_set() {
    // Within a batch all messages share the same score (same TIME inside the script);
    // ZRANGE ties are broken lexicographically by the id, so receive order does NOT
    // match input order. The set of payloads and the count must match exactly.
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        let payloads: Vec<String> = (0..30).map(|i| format!("b-{i:03}")).collect();
        q.send_message_batch("q", payloads.clone(), None).await.unwrap();

        let received = q.receive_message_batch::<String>("q", None, 30).await.unwrap();
        assert_eq!(received.len(), 30);
        let got: HashSet<String> = received.into_iter().map(|m| m.message).collect();
        let want: HashSet<String> = payloads.into_iter().collect();
        assert_eq!(got, want);
    });
}

// ---------------------------------------------------------------------------
// Counters under parallel work
// ---------------------------------------------------------------------------

#[test]
fn totalsent_and_totalrecv_are_consistent_after_parallel_work() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();

        let qa = new_rbmq(&ctx).await;
        let qb = new_rbmq(&ctx).await;
        let h1 = tokio::spawn(async move {
            let mut q = qa;
            for i in 0..50 {
                q.send_message("q", format!("a{i}"), None).await.unwrap();
            }
        });
        let h2 = tokio::spawn(async move {
            let mut q = qb;
            for i in 0..50 {
                q.send_message("q", format!("b{i}"), None).await.unwrap();
            }
        });
        h1.await.unwrap();
        h2.await.unwrap();

        let mut got = 0;
        for _ in 0..200 {
            if q.receive_message::<String>("q", None).await.unwrap().is_some() {
                got += 1;
            } else {
                break;
            }
        }
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, 100);
        assert_eq!(attrs.totalrecv, got);
    });
}

// ---------------------------------------------------------------------------
// Delete races
// ---------------------------------------------------------------------------

#[test]
fn double_delete_returns_false_second_time() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert!(q.delete_message("q", &m.id).await.unwrap());
        assert!(!q.delete_message("q", &m.id).await.unwrap());
    });
}

#[test]
fn delete_unknown_id_returns_false() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        assert!(!q.delete_message("q", "ffffffffffffffffffffffffffffffff").await.unwrap());
    });
}

// ---------------------------------------------------------------------------
// Receive on empty queue is None, not error
// ---------------------------------------------------------------------------

#[test]
fn receive_on_empty_queue_returns_none() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m.is_none());
    });
}

#[test]
fn pop_on_empty_queue_returns_none() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let m = q.pop_message::<String>("q").await.unwrap();
        assert!(m.is_none());
    });
}

#[test]
fn receive_batch_on_empty_queue_returns_empty_vec() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let v = q.receive_message_batch::<String>("q", None, 10).await.unwrap();
        assert!(v.is_empty());
    });
}

#[test]
fn receive_batch_with_max_count_zero_returns_empty_without_redis_call() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let v = q.receive_message_batch::<String>("q", None, 0).await.unwrap();
        assert!(v.is_empty());
    });
}

#[test]
fn send_batch_with_empty_input_returns_empty_ids() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let ids = q.send_message_batch::<String>("q", Vec::new(), None).await.unwrap();
        assert!(ids.is_empty());
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, 0);
    });
}

// ---------------------------------------------------------------------------
// IDs are unique across many sends
// ---------------------------------------------------------------------------

#[test]
fn ids_unique_across_thousand_sends() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        let mut ids = HashSet::new();
        for i in 0..1000 {
            let id = q.send_message("q", format!("m{i}"), None).await.unwrap();
            assert_eq!(id.len(), 32, "ID should be 32 hex chars");
            assert!(id.chars().all(|c| c.is_ascii_hexdigit() && c.is_ascii_lowercase() || c.is_ascii_digit()));
            ids.insert(id);
        }
        assert_eq!(ids.len(), 1000);
    });
}

#[test]
fn ids_unique_across_batched_sends() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let payloads: Vec<String> = (0..500).map(|i| format!("m{i}")).collect();
        let ids = q.send_message_batch("q", payloads, None).await.unwrap();
        let unique: HashSet<_> = ids.iter().cloned().collect();
        assert_eq!(unique.len(), ids.len());
    });
}
