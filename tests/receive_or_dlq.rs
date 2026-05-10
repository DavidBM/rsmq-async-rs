mod support;

use rsmq_async::{Rsmq, RsmqConnection as _, RsmqError};
use support::*;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

async fn new_rsmq(ctx: &TestContext) -> Rsmq {
    Rsmq::new_with_connection(ctx.async_connection().await.unwrap(), false, Some(&ctx.ns))
        .await
        .unwrap()
}

#[test]
fn delivers_when_rc_within_budget() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dlq", None, None, None).await.unwrap();
        rsmq.send_message("src", "hello".to_string(), None)
            .await
            .unwrap();

        // max_receives=3 → first delivery has rc=1, well within budget.
        let received = rsmq
            .receive_message_or_dlq::<String>("src", None, "dlq", 3)
            .await
            .unwrap()
            .expect("should deliver");
        assert_eq!(received.message, "hello");
        assert_eq!(received.rc, 1);

        let dlq_attrs = rsmq.get_queue_attributes("dlq").await.unwrap();
        assert_eq!(dlq_attrs.msgs, 0);

        rsmq.delete_message("src", &received.id).await.unwrap();
        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dlq").await.unwrap();
    });
}

#[test]
fn routes_to_dlq_when_rc_exceeds_max_receives() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dlq", None, None, None).await.unwrap();
        let id = rsmq
            .send_message("src", "doomed".to_string(), None)
            .await
            .unwrap();

        // max_receives=0 → first receive (rc=1) is already > 0 → straight to DLQ.
        let result = rsmq
            .receive_message_or_dlq::<String>("src", None, "dlq", 0)
            .await
            .unwrap();
        assert!(result.is_none(), "should not deliver");

        // Source drained, DLQ has the message with preserved rc.
        let src_attrs = rsmq.get_queue_attributes("src").await.unwrap();
        assert_eq!(src_attrs.msgs, 0);
        let dlq_msg = rsmq
            .receive_message::<String>("dlq", None)
            .await
            .unwrap()
            .expect("DLQ should have it");
        assert_eq!(dlq_msg.id, id);
        assert_eq!(dlq_msg.message, "doomed");
        // dlq_msg.rc is from the *DLQ* receive (now 1 there), but the rc bumped before
        // routing was 1. We can verify the bump happened by checking src totalrecv.
        let src_attrs2 = rsmq.get_queue_attributes("src").await.unwrap();
        assert_eq!(
            src_attrs2.totalrecv, 1,
            "the DLQ-routed message was 'received' once"
        );

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dlq").await.unwrap();
    });
}

#[test]
fn delivers_then_dlqs_after_budget_exhausted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dlq", None, None, None).await.unwrap();
        rsmq.send_message("src", "twice".to_string(), None)
            .await
            .unwrap();

        // max_receives=1: first receive returns the message (rc=1, 1 > 1 false).
        let first = rsmq
            .receive_message_or_dlq::<String>("src", Some(std::time::Duration::ZERO), "dlq", 1)
            .await
            .unwrap()
            .expect("should deliver first time");
        assert_eq!(first.rc, 1);

        // Without delete, message becomes visible immediately (vt=0). Next call → rc=2 > 1 → DLQ.
        let second = rsmq
            .receive_message_or_dlq::<String>("src", Some(std::time::Duration::ZERO), "dlq", 1)
            .await
            .unwrap();
        assert!(second.is_none(), "should route to DLQ this round");

        // Verify it's actually in the DLQ.
        let in_dlq = rsmq.receive_message::<String>("dlq", None).await.unwrap();
        assert!(in_dlq.is_some(), "message should now be in DLQ");

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dlq").await.unwrap();
    });
}

#[test]
fn skips_dlq_messages_and_returns_next_eligible() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dlq", None, None, None).await.unwrap();

        // First message: pre-bump its rc so it lands in DLQ on next receive.
        rsmq.send_message("src", "doomed".to_string(), None)
            .await
            .unwrap();
        let _ = rsmq
            .receive_message::<String>("src", Some(std::time::Duration::ZERO))
            .await
            .unwrap();

        // Second message: fresh, rc=0.
        rsmq.send_message("src", "fresh".to_string(), None)
            .await
            .unwrap();

        // max_receives=1 → "doomed" already has rc=1, next receive bumps to 2 → DLQ.
        // Then "fresh" gets bumped to rc=1 → delivered.
        let received = rsmq
            .receive_message_or_dlq::<String>("src", None, "dlq", 1)
            .await
            .unwrap()
            .expect("should skip doomed and deliver fresh");
        assert_eq!(received.message, "fresh");

        // doomed ended up in DLQ.
        let dlq_msg = rsmq
            .receive_message::<String>("dlq", None)
            .await
            .unwrap()
            .expect("DLQ should have doomed");
        assert_eq!(dlq_msg.message, "doomed");

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dlq").await.unwrap();
    });
}

#[test]
fn returns_none_on_empty_queue() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dlq", None, None, None).await.unwrap();

        let result = rsmq
            .receive_message_or_dlq::<String>("src", None, "dlq", 3)
            .await
            .unwrap();
        assert!(result.is_none());

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dlq").await.unwrap();
    });
}

#[test]
fn rejects_self_loop() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();

        let err = rsmq
            .receive_message_or_dlq::<String>("q", None, "q", 3)
            .await
            .unwrap_err();
        assert!(matches!(err, RsmqError::InvalidFormat(_)));

        rsmq.delete_queue("q").await.unwrap();
    });
}
