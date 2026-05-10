mod support;

use rand::RngExt;
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

// Property: every send is eventually receivable.
#[test]
fn property_every_send_is_receivable() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, Some(-1)).await.unwrap();

        let mut rng = rand::rng();
        let mut sent: HashSet<Vec<u8>> = HashSet::new();
        for _ in 0..200 {
            let len = rng.random_range(0..512);
            let body: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            sent.insert(body.clone());
            q.send_message("q", body, None).await.unwrap();
        }

        let mut received: HashSet<Vec<u8>> = HashSet::new();
        while let Some(m) = q.receive_message::<Vec<u8>>("q", None).await.unwrap() {
            received.insert(m.message);
            q.delete_message("q", &m.id).await.unwrap();
        }
        // Random bodies have a tiny chance of duplicating (esp. for short empty bodies),
        // so sent.len() may differ from the number of sends; what matters is set inclusion.
        assert!(sent.is_subset(&received));
    });
}

// Property: totalsent + msgs accounting.
#[test]
fn property_msgs_count_equals_sent_minus_deleted_plus_inflight() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();

        let mut rng = rand::rng();
        let n_sent = rng.random_range(20..50);
        for _ in 0..n_sent {
            q.send_message("q", "x", None).await.unwrap();
        }

        let n_received = rng.random_range(0..n_sent);
        let mut received_ids = Vec::new();
        for _ in 0..n_received {
            let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
            received_ids.push(m.id);
        }

        let n_deleted = rng.random_range(0..received_ids.len().max(1)).min(received_ids.len());
        for id in received_ids.iter().take(n_deleted) {
            q.delete_message("q", id).await.unwrap();
        }

        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent as u32, n_sent);
        assert_eq!(attrs.msgs as usize, n_sent as usize - n_deleted);
    });
}

// Property: delete idempotency
#[test]
fn property_delete_is_idempotent_after_success() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        for i in 0..30 {
            q.send_message("q", format!("m{i}"), None).await.unwrap();
        }
        let received = q.receive_message_batch::<String>("q", None, 30).await.unwrap();
        for m in &received {
            assert!(q.delete_message("q", &m.id).await.unwrap());
            for _ in 0..3 {
                assert!(!q.delete_message("q", &m.id).await.unwrap());
            }
        }
    });
}

// Property: rc strictly increases for the same message under repeated receives.
#[test]
fn property_rc_strictly_increases_on_redelivery() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        let _ = q.send_message("q", "x", None).await.unwrap();

        let mut last = 0u64;
        for _ in 0..10 {
            let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
            assert!(m.rc > last);
            last = m.rc;
        }
    });
}

// Property: fr stays constant after first delivery.
#[test]
fn property_fr_stays_constant() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        q.send_message("q", "x", None).await.unwrap();

        let m0 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let fr = m0.fr;
        for _ in 0..10 {
            let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
            assert_eq!(m.fr, fr);
        }
    });
}

// Property: id length and alphabet
#[test]
fn property_ids_are_valid_lowercase_hex_32_chars() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        for _ in 0..200 {
            let id = q.send_message("q", "x", None).await.unwrap();
            assert_eq!(id.len(), 32);
            for c in id.chars() {
                assert!(matches!(c, '0'..='9' | 'a'..='f'), "non-hex char {c} in {id}");
            }
        }
    });
}

// Property: batch send returns one id per input.
#[test]
fn property_batch_send_ids_match_input_count() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        let mut rng = rand::rng();
        for _ in 0..10 {
            let n = rng.random_range(1..50);
            let payloads: Vec<String> = (0..n).map(|i| format!("m{i}")).collect();
            let ids = q.send_message_batch("q", payloads.clone(), None).await.unwrap();
            assert_eq!(ids.len(), payloads.len());
            // Drain via pop (deletes as it reads).
            while q.pop_message::<String>("q").await.unwrap().is_some() {}
        }
    });
}

// Property: pop deletes; subsequent receive returns None for that id.
#[test]
fn property_pop_returns_then_disappears() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        for _ in 0..20 {
            q.send_message("q", "x", None).await.unwrap();
        }
        for _ in 0..20 {
            let m = q.pop_message::<String>("q").await.unwrap().unwrap();
            // Trying to delete the popped message again returns false (already gone).
            assert!(!q.delete_message("q", &m.id).await.unwrap());
        }
        // Queue is empty.
        assert!(q.pop_message::<String>("q").await.unwrap().is_none());
    });
}

// Property: receive_message_batch with max_count larger than queue returns all available.
#[test]
fn property_receive_batch_returns_min_of_available_and_max() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        let mut rng = rand::rng();
        for _ in 0..5 {
            let n_sent = rng.random_range(1..30);
            let max_count = rng.random_range(1..50);
            for i in 0..n_sent {
                q.send_message("q", format!("x{i}"), None).await.unwrap();
            }
            let received = q
                .receive_message_batch::<String>("q", Some(Duration::from_secs(60)), max_count)
                .await
                .unwrap();
            let expected = (n_sent as u32).min(max_count) as usize;
            assert_eq!(received.len(), expected);
            // Cleanup: delete the received ones, then pop any remaining (which weren't received).
            for m in &received {
                q.delete_message("q", &m.id).await.unwrap();
            }
            while q.pop_message::<String>("q").await.unwrap().is_some() {}
        }
    });
}

// Property: delete_queue followed by re-create gives a fresh queue.
#[test]
fn property_delete_then_create_gives_fresh_state() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;

        for cycle in 0..5 {
            q.create_queue("q", None, None, None).await.unwrap();
            for i in 0..10 {
                q.send_message("q", format!("c{cycle}-{i}"), None).await.unwrap();
            }
            let attrs = q.get_queue_attributes("q").await.unwrap();
            assert_eq!(attrs.totalsent, 10);
            q.delete_queue("q").await.unwrap();
        }
    });
}
