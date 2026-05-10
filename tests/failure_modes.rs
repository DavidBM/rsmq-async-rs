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

#[test]
fn send_to_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(q.send_message("ghost", "x", None).await, Err(RbmqError::QueueNotFound)));
    });
}

#[test]
fn send_batch_to_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.send_message_batch("ghost", vec!["a", "b"], None).await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn receive_from_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.receive_message::<String>("ghost", None).await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn receive_batch_from_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.receive_message_batch::<String>("ghost", None, 5).await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn pop_from_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.pop_message::<String>("ghost").await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn change_visibility_for_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.change_message_visibility("ghost", "id", Duration::from_secs(10)).await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn move_message_from_nonexistent_source_returns_false() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("dst", None, None, None).await.unwrap();
        // Source has no message; move returns false (not an error).
        let r = q.move_message("ghost", "fffffff", "dst").await.unwrap();
        assert!(!r);
    });
}

#[test]
fn dlq_routing_with_nonexistent_source_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("dlq", None, None, None).await.unwrap();
        assert!(matches!(
            q.receive_message_or_dlq::<String>("ghost", None, "dlq", 1).await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn delete_message_on_nonexistent_queue_returns_false() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        // No QueueNotFound — delete_message just no-ops on missing queue.
        let r = q.delete_message("ghost", "fffffff").await.unwrap();
        assert!(!r);
    });
}

#[test]
fn get_attributes_on_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.get_queue_attributes("ghost").await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn set_attributes_on_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.set_queue_attributes("ghost", Some(Duration::from_secs(60)), None, None).await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

#[test]
fn invalid_qname_send_rejected_before_redis_call() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        // Empty name → InvalidFormat from Rust-side validation, never hits Redis.
        assert!(matches!(
            q.create_queue("", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn move_with_invalid_src_or_dst_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.move_message("", "id", "dst").await,
            Err(RbmqError::InvalidFormat(_))
        ));
        assert!(matches!(
            q.move_message("src", "id", "").await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn dlq_with_invalid_dlq_name_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("src", None, None, None).await.unwrap();
        assert!(matches!(
            q.receive_message_or_dlq::<String>("src", None, "", 1).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn message_too_long_does_not_increment_totalsent() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(1024)).await.unwrap();
        let _ = q.send_message("q", "x".repeat(2000), None).await;
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, 0);
        assert_eq!(attrs.msgs, 0);
    });
}

#[test]
fn batch_with_one_oversized_message_no_partial_inserts() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, Some(1024)).await.unwrap();
        let mut payloads: Vec<String> = (0..3).map(|i| format!("ok-{i}")).collect();
        payloads.push("x".repeat(2000));
        payloads.push("ok-after".into());
        let r = q.send_message_batch("q", payloads, None).await;
        assert!(matches!(r, Err(RbmqError::MessageTooLong)));
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, 0);
    });
}

#[test]
fn create_queue_with_extreme_negative_maxsize_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("q", None, None, Some(i64::MIN)).await,
            Err(RbmqError::InvalidValue(..))
        ));
    });
}

#[test]
fn create_queue_with_extreme_positive_maxsize_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(
            q.create_queue("q", None, None, Some(i64::MAX)).await,
            Err(RbmqError::InvalidValue(..))
        ));
    });
}

#[test]
fn delete_queue_then_send_then_recreate_then_send_works() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "first", None).await.unwrap();
        q.delete_queue("q").await.unwrap();
        // Send to deleted: error.
        assert!(q.send_message("q", "x", None).await.is_err());
        q.create_queue("q", None, None, None).await.unwrap();
        // Now works again.
        q.send_message("q", "second", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.message, "second");
    });
}

#[test]
fn change_visibility_with_excessive_duration_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "x", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        // 200 years exceeds the 100-year defensive cap.
        let r = q
            .change_message_visibility("q", &m.id, Duration::from_secs(200 * 365 * 24 * 3600))
            .await;
        assert!(r.is_err());
    });
}

#[test]
fn delete_message_with_invalid_qname_format_in_redis_returns_false() {
    // Valid qname format check is done client-side; passing an invalid qname *to certain ops*
    // doesn't always go through the validator (the script will just no-op on non-existent keys).
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        let r = q.delete_message("nonexistent", "ffffffffffffffffffffffffffffffff").await.unwrap();
        assert!(!r);
    });
}
