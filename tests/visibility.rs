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

#[test]
fn change_visibility_extends_in_flight_window() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_millis(200)), None, None).await.unwrap();
        let _ = q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();

        // Bump visibility well into the future.
        q.change_message_visibility("q", &m.id, Duration::from_secs(60)).await.unwrap();

        // Wait past the original 200ms vt; message must NOT be redelivered.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_none(), "message should remain hidden under extended vt");
    });
}

#[test]
fn change_visibility_can_shorten_to_zero() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        let _ = q.send_message("q", "hi", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        q.change_message_visibility("q", &m.id, Duration::ZERO).await.unwrap();

        let m2 = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m2.id, m.id);
        assert_eq!(m2.rc, 2);
    });
}

#[test]
fn change_visibility_for_unknown_id_is_silent_noop() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        // Should not error, just no-op.
        q.change_message_visibility(
            "q",
            "ffffffffffffffffffffffffffffffff",
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    });
}

#[test]
fn vt_actually_hides_for_specified_duration() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_millis(150)), None, None).await.unwrap();
        q.send_message("q", "hi", None).await.unwrap();
        let m1 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m1.is_some());

        // Immediately try again — must be hidden.
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_none());

        // After vt elapses, redeliver.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let m3 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m3.is_some());
    });
}

#[test]
fn delayed_send_is_invisible_until_delay_elapses() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "hi", Some(Duration::from_millis(150))).await.unwrap();

        let m1 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m1.is_none(), "should be invisible during delay");

        tokio::time::sleep(Duration::from_millis(200)).await;
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_some());
    });
}

#[test]
fn vt_is_independent_per_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        q.send_message("q", "a", None).await.unwrap();
        q.send_message("q", "b", None).await.unwrap();

        let ma = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        let mb = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        // Make 'a' immediately visible.
        q.change_message_visibility("q", &ma.id, Duration::ZERO).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        assert_eq!(m.id, ma.id);
        // 'b' is still hidden.
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_none());
        let _ = mb;
    });
}

#[test]
fn batch_receive_respects_explicit_hidden() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        q.send_message_batch("q", vec!["a", "b", "c"], None).await.unwrap();
        // Receive with hidden=0; messages must be available again immediately.
        let first = q.receive_message_batch::<String>("q", Some(Duration::ZERO), 3).await.unwrap();
        assert_eq!(first.len(), 3);
        let second = q.receive_message_batch::<String>("q", None, 3).await.unwrap();
        // All visible again because we used hidden=0 in the first call.
        assert_eq!(second.len(), 3);
    });
}

#[test]
fn pop_does_not_leave_message_for_redelivery() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::ZERO), None, None).await.unwrap();
        q.send_message("q", "x", None).await.unwrap();
        let m = q.pop_message::<String>("q").await.unwrap();
        assert!(m.is_some());
        let m2 = q.receive_message::<String>("q", None).await.unwrap();
        assert!(m2.is_none());
    });
}

#[test]
fn delete_after_vt_expires_returns_false_only_if_not_in_zset() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", Some(Duration::from_millis(100)), None, None).await.unwrap();
        q.send_message("q", "x", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        // Wait for vt to elapse — message becomes visible again, still in zset.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Delete still works because the message is still in the zset.
        assert!(q.delete_message("q", &m.id).await.unwrap());
    });
}

#[test]
fn change_visibility_max_value_doesnt_overflow() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut q = new_rbmq(&ctx).await;
        q.create_queue("q", None, None, None).await.unwrap();
        q.send_message("q", "x", None).await.unwrap();
        let m = q.receive_message::<String>("q", None).await.unwrap().unwrap();
        // 1 year is well below the 100-year defensive cap.
        q.change_message_visibility("q", &m.id, Duration::from_secs(365 * 24 * 3600)).await.unwrap();
        let attrs = q.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.hiddenmsgs, 1);
    });
}
