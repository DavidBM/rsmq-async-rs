mod support;

use rbmq::{Rbmq, RbmqConnection as _, RbmqError};
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
// Body edge cases
// ---------------------------------------------------------------------------

#[test]
fn empty_string_body_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let id = q.send_message("q", "", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.id, id);
        assert_eq!(m.message, "");
    });
}

#[test]
fn empty_bytes_body_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body: Vec<u8> = Vec::new();
        let id = q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<Vec<u8>>("q", None).await.unwrap().unwrap();
        assert_eq!(m.id, id);
        assert!(m.message.is_empty());
    });
}

#[test]
fn body_with_newlines_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let payload = "line1\nline2\nline3\n";
        q.send_message("q", payload, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, payload);
    });
}

#[test]
fn body_with_only_newlines_preserves_packed_layout() {
    // Stresses the Lua packed-value parser: body itself is just newlines.
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "\n\n\n\n", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, "\n\n\n\n");
    });
}

#[test]
fn body_with_null_bytes_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body = vec![0u8, 0, 1, 0, 2];
        q.send_message("q", body.clone(), None).await.unwrap();
        let m = q.receive_message::<Vec<u8>>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, body);
    });
}

#[test]
fn body_with_unicode_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let payload = "héllo 世界 🦀 emoji✨";
        q.send_message("q", payload, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, payload);
    });
}

#[test]
fn body_with_high_bytes_round_trips() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body: Vec<u8> = (0u8..=255).collect();
        q.send_message("q", body.clone(), None).await.unwrap();
        let m = q.receive_message::<Vec<u8>>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, body);
    });
}

#[test]
fn body_at_exact_default_maxsize_is_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let body = "a".repeat(65536);
        q.send_message("q", body.clone(), None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message.len(), 65536);
        assert_eq!(m.message, body);
    });
}

#[test]
fn body_one_over_maxsize_rejected_at_each_boundary() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q1", None, None, Some(1024)).await.unwrap();
        q.create_queue("q2", None, None, Some(8192)).await.unwrap();
        q.create_queue("q3", None, None, Some(65536)).await.unwrap();

        for (qn, n) in [("q1", 1025), ("q2", 8193), ("q3", 65537)] {
            assert!(matches!(
                q.send_message(qn, "x".repeat(n), None).await,
                Err(RbmqError::MessageTooLong)
            ));
        }
    });
}

#[test]
fn body_at_exact_custom_maxsize_is_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(2048)).await.unwrap();
        let body = "x".repeat(2048);
        q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message.len(), 2048);
    });
}

// ---------------------------------------------------------------------------
// vt / delay / visibility edge cases
// ---------------------------------------------------------------------------

#[test]
fn vt_zero_makes_message_immediately_redeliverable() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        let id = q.send_message("q", "hi", None).await.unwrap();
        let m1 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let m2 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m1.id, id);
        assert_eq!(m2.id, id);
        assert_eq!(m2.rc, 2);
    });
}

#[test]
fn delay_zero_message_is_visible_immediately() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, Some(Duration::ZERO), None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m.is_some());
    });
}

#[test]
fn delay_overrides_queue_default() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, Some(Duration::from_secs(60)), None).await.unwrap();
        // Override with delay=0 via send arg
        q.send_message("q", "hi", Some(Duration::ZERO)).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m.is_some(), "explicit delay=0 should override queue default of 60s");
    });
}

#[test]
fn hidden_overrides_queue_vt() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        let id = q.send_message("q", "hi", None).await.unwrap();
        // Receive with vt=0 explicitly: message is visible to next receive immediately.
        let m1 = q.receive_message::<String>("q", Some(Duration::ZERO)).await.unwrap().unwrap();
        assert_eq!(m1.id, id);
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_some(), "vt=0 should override queue's 60s default");
    });
}

#[test]
fn change_visibility_to_zero_makes_message_immediately_visible() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        let _ = q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        q.change_message_visibility("q", &m.id, Duration::ZERO).await.unwrap();
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_some());
    });
}

// ---------------------------------------------------------------------------
// Queue name validation
// ---------------------------------------------------------------------------

#[test]
fn queue_name_max_length_is_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        let name = "a".repeat(160);
        q.create_queue(&name, None, None, None).await.unwrap();
    });
}

#[test]
fn queue_name_one_over_max_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        let name = "a".repeat(161);
        assert!(matches!(
            q.create_queue(&name, None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_with_dash_and_underscore_is_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("my-queue_v2", None, None, None).await.unwrap();
        q.send_message("my-queue_v2", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("my-queue_v2", None).await.unwrap().unwrap();
        assert_eq!(m.message, "hi");
    });
}

#[test]
fn queue_name_with_dot_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("bad.name", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_with_space_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("bad name", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_with_slash_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("bad/name", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_with_colon_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("bad:name", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_unicode_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("queue🦀", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

// ---------------------------------------------------------------------------
// Maxsize edge cases
// ---------------------------------------------------------------------------

#[test]
fn maxsize_below_1024_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("q", None, None, Some(1023)).await,
            Err(RbmqError::InvalidValue(..))
        ));
    });
}

#[test]
fn maxsize_above_65536_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("q", None, None, Some(65537)).await,
            Err(RbmqError::InvalidValue(..))
        ));
    });
}

#[test]
fn maxsize_unlimited_accepts_huge_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(-1)).await.unwrap();
        let body = "x".repeat(1_000_000);
        q.send_message("q", body, None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message.len(), 1_000_000);
    });
}

#[test]
fn maxsize_minus_two_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("q", None, None, Some(-2)).await,
            Err(RbmqError::InvalidValue(..))
        ));
    });
}

// ---------------------------------------------------------------------------
// rc / fr / sent semantics
// ---------------------------------------------------------------------------

#[test]
fn rc_starts_at_one_after_first_receive() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.rc, 1);
    });
}

#[test]
fn rc_increments_on_each_receive() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        for expected in 1..=5 {
            let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
            assert_eq!(m.rc, expected);
        }
    });
}

#[test]
fn fr_set_to_first_receive_then_stable() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m1 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let m2 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let m3 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m1.fr, m2.fr);
        assert_eq!(m2.fr, m3.fr);
        assert!(m1.fr > 0);
    });
}

#[test]
fn sent_field_is_microseconds_not_milliseconds() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        // Microsecond timestamps are 16 digits as of 2026 (~1.7e15).
        // If sent were millis, it would be 13 digits.
        assert!(m.sent > 1_000_000_000_000_000, "sent should be us not ms (got {})", m.sent);
    });
}
