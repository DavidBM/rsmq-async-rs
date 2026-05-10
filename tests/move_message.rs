mod support;

use rsmq_async::{Rsmq, RsmqConnection as _, RsmqError};
use std::time::Duration;
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
fn move_message_relocates_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dst", None, None, None).await.unwrap();

        let msg_id = rsmq
            .send_message("src", "payload".to_string(), None)
            .await
            .unwrap();

        let moved = rsmq.move_message("src", &msg_id, "dst").await.unwrap();
        assert!(moved);

        // Source is empty, destination has the message with the same id.
        let none = rsmq.receive_message::<String>("src", None).await.unwrap();
        assert!(none.is_none());

        let received = rsmq
            .receive_message::<String>("dst", None)
            .await
            .unwrap()
            .expect("message should have moved to dst");
        assert_eq!(received.id, msg_id);
        assert_eq!(received.message, "payload");

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dst").await.unwrap();
    });
}

#[test]
fn move_message_returns_false_when_id_missing() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", None, None, None).await.unwrap();
        rsmq.create_queue("dst", None, None, None).await.unwrap();

        let moved = rsmq
            .move_message("src", "nope-not-a-real-id", "dst")
            .await
            .unwrap();
        assert!(!moved);

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dst").await.unwrap();
    });
}

#[test]
fn move_message_preserves_rc_and_fr() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("src", Some(Duration::from_secs(1)), None, None)
            .await
            .unwrap();
        rsmq.create_queue("dst", None, None, None).await.unwrap();

        let _id = rsmq
            .send_message("src", "x".to_string(), None)
            .await
            .unwrap();

        // Receive twice so rc=2, then wait for vt and receive once more so rc=3.
        let r1 = rsmq
            .receive_message::<String>("src", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1.rc, 1);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let r2 = rsmq
            .receive_message::<String>("src", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2.rc, 2);

        // Move while it's hidden.
        assert!(rsmq.move_message("src", &r2.id, "dst").await.unwrap());

        // Now in dst — receive it (won't increment rc on src obviously, but rc on dst now == 3).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let r3 = rsmq
            .receive_message::<String>("dst", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r3.rc, 3, "rc should have been preserved across the move");

        rsmq.delete_queue("src").await.unwrap();
        rsmq.delete_queue("dst").await.unwrap();
    });
}

#[test]
fn move_message_rejects_self_loop() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();
        let id = rsmq.send_message("q", "x".to_string(), None).await.unwrap();

        let err = rsmq.move_message("q", &id, "q").await.unwrap_err();
        assert!(matches!(err, RsmqError::InvalidFormat(_)));

        // The message is still there.
        let attrs = rsmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 1);

        rsmq.delete_queue("q").await.unwrap();
    });
}
