mod support;

use rbmq::{Rbmq, RbmqConnection as _};
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

// Tests around the packed message format ("<rc>\n<fr>\n<sent>\n<body>") and how
// receive scripts parse it. Body content can be anything — including newlines
// and zero bytes — because rc/fr/sent are decimal-only and we split on the first
// three newlines only.

#[test]
fn body_starting_with_digit_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body = "12345 some payload";
        q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, body);
    });
}

#[test]
fn body_starting_with_newlines_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body = "\n\nhello";
        q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, body);
    });
}

#[test]
fn body_ending_with_newlines_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body = "hello\n\n";
        q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, body);
    });
}

#[test]
fn body_containing_packed_layout_lookalike_round_trips() {
    // Body that *itself* looks like a packed payload — should be preserved verbatim.
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body = "999\n123\n456\nfake-body-inside";
        q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, body);
    });
}

#[test]
fn rc_and_fr_correct_after_redelivery_with_body_containing_newlines() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        q.send_message("q", "line1\nline2", None).await.unwrap();
        let m1 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let m2 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let m3 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m1.message, "line1\nline2");
        assert_eq!(m1.rc, 1);
        assert_eq!(m2.rc, 2);
        assert_eq!(m3.rc, 3);
        assert_eq!(m1.fr, m2.fr);
        assert_eq!(m2.fr, m3.fr);
        assert_eq!(m1.message, m3.message);
    });
}

#[test]
fn binary_body_with_all_byte_values_round_trips_via_batch() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let bodies: Vec<Vec<u8>> = (0..3).map(|_| (0u8..=255).collect()).collect();
        q.send_message_batch("q", bodies.clone(), None).await.unwrap();
        let received = q.receive_message_batch::<Vec<u8>>("q", None, 3).await.unwrap();
        assert_eq!(received.len(), 3);
        for m in &received {
            assert_eq!(m.message, bodies[0]);
        }
    });
}

#[test]
fn very_long_body_round_trips_with_unlimited_maxsize() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(-1)).await.unwrap();
        let body: Vec<u8> = (0..2_000_000).map(|i| (i % 256) as u8).collect();
        q.send_message("q", body.clone(), None).await.unwrap();
        let m = q.receive_message::<Vec<u8>>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message.len(), 2_000_000);
        assert_eq!(m.message, body);
    });
}

#[test]
fn pop_returns_packed_metadata_then_destroys_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m = q.pop_message::<String>("q").await.unwrap().unwrap();
        assert_eq!(m.message, "hi");
        assert_eq!(m.rc, 1);
        assert!(m.fr > 0);
        assert!(m.sent > 0);
        // Subsequent receive returns None.
        assert!(q.receive_message::<String>("q", None).await.unwrap().is_none());
    });
}

#[test]
fn pop_does_not_increment_fr_on_subsequent_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "a", None).await.unwrap();
        q.send_message("q", "b", None).await.unwrap();
        let m1 = q.pop_message::<String>("q").await.unwrap().unwrap();
        let m2 = q.pop_message::<String>("q").await.unwrap().unwrap();
        assert_ne!(m1.id, m2.id);
        assert_eq!(m1.rc, 1);
        assert_eq!(m2.rc, 1);
        assert!(m1.fr > 0);
        assert!(m2.fr > 0);
    });
}

#[test]
fn id_is_lowercase_hex_32_chars() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let id = q.send_message("q", "x", None).await.unwrap();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
    });
}

#[test]
fn maxsize_compares_against_body_length_not_packed_length() {
    // The packed value adds a few dozen bytes of header; maxsize must apply only to body.
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(1024)).await.unwrap();
        let body = "x".repeat(1024); // exactly maxsize
        q.send_message("q", body, None).await.unwrap();
    });
}

#[test]
fn batch_send_packs_each_message_independently() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        // Mix bodies with newlines, empties, etc.
        let bodies: Vec<String> = vec![
            "alpha".into(),
            "with\nnewline".into(),
            "".into(),
            "trailing\n".into(),
            "1\n2\n3\n4".into(),
        ];
        q.send_message_batch("q", bodies.clone(), None).await.unwrap();
        let received = q.receive_message_batch::<String>("q", None, 5).await.unwrap();
        assert_eq!(received.len(), 5);
        let mut got: Vec<String> = received.into_iter().map(|m| m.message).collect();
        got.sort();
        let mut want = bodies.clone();
        want.sort();
        assert_eq!(got, want);
    });
}
