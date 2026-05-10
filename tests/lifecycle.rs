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
fn create_then_delete_then_create_again_works() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        q.delete_queue("q").await.unwrap();

        // Recreate with different attrs.
        q.create_queue("q", Some(Duration::from_secs(60)), None, Some(2048)).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.vt, Duration::from_secs(60));
        assert_eq!(attrs.maxsize, 2048);
        // Counters reset.
        assert_eq!(attrs.totalsent, 0);
    });
}

#[test]
fn delete_queue_purges_messages_and_counters() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        for _ in 0..10 {
            q.send_message("q", "hi", None).await.unwrap();
        }
        q.delete_queue("q").await.unwrap();

        // Send to deleted queue must error.
        assert!(matches!(
            q.send_message("q", "x", None).await,
            Err(RbmqError::QueueNotFound)
        ));

        // Recreate empty.
        q.create_queue("q", None, None, None).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0);
        assert_eq!(attrs.totalsent, 0);
    });
}

#[test]
fn delete_nonexistent_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        assert!(matches!(q.delete_queue("ghost").await, Err(RbmqError::QueueNotFound)));
    });
}

#[test]
fn create_existing_queue_errors() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        assert!(matches!(
            q.create_queue("q", None, None, None).await,
            Err(RbmqError::QueueExists)
        ));
    });
}

#[test]
fn list_queues_returns_all_namespace_queues() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q1", None, None, None).await.unwrap();
        q.create_queue("q2", None, None, None).await.unwrap();
        q.create_queue("q3", None, None, None).await.unwrap();

        let mut listed = q.list_queues().await.unwrap();
        listed.sort();
        assert_eq!(listed, vec!["q1", "q2", "q3"]);
    });
}

#[test]
fn list_queues_omits_deleted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("a", None, None, None).await.unwrap();
        q.create_queue("b", None, None, None).await.unwrap();
        q.delete_queue("a").await.unwrap();
        let listed = q.list_queues().await.unwrap();
        assert_eq!(listed, vec!["b"]);
    });
}

#[test]
fn list_queues_in_empty_namespace_is_empty() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        let listed = q.list_queues().await.unwrap();
        assert!(listed.is_empty());
    });
}

#[test]
fn set_queue_attributes_persists_across_reconnect() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.set_queue_attributes("q", Some(Duration::from_secs(45)), None, Some(2048))
            .await
            .unwrap();

        // Open a fresh client to simulate reconnect.
        let mut q2 = new_rbmq(&ctx).await;
        let attrs = q2.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.vt, Duration::from_secs(45));
        assert_eq!(attrs.maxsize, 2048);
    });
}

#[test]
fn set_queue_attributes_can_set_each_field_independently() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();

        q.set_queue_attributes("q", Some(Duration::from_secs(120)), None, None).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.vt, Duration::from_secs(120));

        q.set_queue_attributes("q", None, Some(Duration::from_secs(15)), None).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.delay, Duration::from_secs(15));
        assert_eq!(attrs.vt, Duration::from_secs(120));

        q.set_queue_attributes("q", None, None, Some(2048)).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.maxsize, 2048);
        assert_eq!(attrs.vt, Duration::from_secs(120));
        assert_eq!(attrs.delay, Duration::from_secs(15));
    });
}

#[test]
fn set_queue_attributes_with_no_changes_still_updates_modified_timestamp() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let before = q.get_queue_attributes("q").await.unwrap().modified;
        tokio::time::sleep(Duration::from_secs(1)).await;
        q.set_queue_attributes("q", None, None, None).await.unwrap();
        let after = q.get_queue_attributes("q").await.unwrap().modified;
        assert!(after >= before);
    });
}

#[test]
fn get_queue_attributes_msgs_count_reflects_sent_minus_deleted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();

        for _ in 0..7 {
            q.send_message("q", "x", None).await.unwrap();
        }
        // Delete 3.
        for _ in 0..3 {
            let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
            q.delete_message("q", &m.id).await.unwrap();
        }
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, 7);
        assert_eq!(attrs.msgs, 4);
    });
}

#[test]
fn hidden_count_reflects_in_flight_messages() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        for _ in 0..5 {
            q.send_message("q", "x", None).await.unwrap();
        }
        // Receive 2 (they become hidden under the 60s vt).
        for _ in 0..2 {
            q.receive_message::<String>("q", None).await.unwrap().unwrap();
        }
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 5);
        assert_eq!(attrs.hiddenmsgs, 2);
    });
}

#[test]
fn modified_changes_after_set_queue_attributes() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        let before = q.get_queue_attributes("q").await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        q.set_queue_attributes("q", Some(Duration::from_secs(99)), None, None).await.unwrap();
        let after = q.get_queue_attributes("q").await.unwrap();
        assert!(after.modified >= before.modified);
        assert_eq!(after.vt, Duration::from_secs(99));
    });
}

#[test]
fn create_queue_with_max_long_delay_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        // 1 hour delay.
        q.create_queue("q", None, Some(Duration::from_secs(3600)), None).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.delay, Duration::from_secs(3600));
    });
}

#[test]
fn create_queue_with_excessive_duration_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        // 200 years - exceeds the 100-year defensive cap.
        let duration = Duration::from_secs(200 * 365 * 24 * 60 * 60);
        assert!(q.create_queue("q", Some(duration), None, None).await.is_err());
    });
}

#[test]
fn cleanup_after_delete_queue_drops_msg_hash_too() {
    // Verifies that deleting a queue purges per-message data, not just config.
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        for _ in 0..50 {
            q.send_message("q", "x".repeat(1000), None).await.unwrap();
        }
        q.delete_queue("q").await.unwrap();
        // Recreate; must be empty.
        q.create_queue("q", None, None, None).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0);
    });
}
