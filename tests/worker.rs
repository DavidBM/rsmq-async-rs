#![cfg(feature = "worker")]

mod support;

use rbmq::{Rbmq, RbmqConnection as _, RbmqError, RbmqMessage, RbmqOptions, Worker};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

fn opts(ns: &str) -> RbmqOptions {
    RbmqOptions {
        ns: ns.to_string(),
        ..Default::default()
    }
}

#[test]
fn processes_message_and_deletes_on_success() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();
        rbmq.send_message("q", "hello".to_string(), None)
            .await
            .unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .route("q", move |msg: RbmqMessage<String>| {
                let c = c.clone();
                async move {
                    assert_eq!(msg.message, "hello");
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .build()
            .await
            .unwrap();

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(400)))
            .await
            .unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "handler called exactly once"
        );

        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0, "message should be deleted");

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn handler_error_leaves_message_in_queue() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", Some(Duration::from_secs(60)), None, None)
            .await
            .unwrap();
        rbmq.send_message("q", "boom".to_string(), None)
            .await
            .unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .route("q", |_msg: RbmqMessage<String>| async {
                Err::<(), HandlerErr>(HandlerErr)
            })
            .build()
            .await
            .unwrap();

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(300)))
            .await
            .unwrap();

        // Long vt (60s) keeps it hidden, but it's still in the queue's sorted set.
        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 1, "failed message should remain for redelivery");

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn router_dispatches_by_queue_name() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("emails", None, None, None).await.unwrap();
        rbmq.create_queue("billing", None, None, None)
            .await
            .unwrap();
        rbmq.send_message("emails", "subject".to_string(), None)
            .await
            .unwrap();
        rbmq.send_message("billing", vec![1u8, 2, 3], None)
            .await
            .unwrap();

        let emails = Arc::new(AtomicUsize::new(0));
        let billing = Arc::new(AtomicUsize::new(0));
        let e = emails.clone();
        let b = billing.clone();
        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .route("emails", move |msg: RbmqMessage<String>| {
                let e = e.clone();
                async move {
                    assert_eq!(msg.message, "subject");
                    e.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .route("billing", move |msg: RbmqMessage<Vec<u8>>| {
                let b = b.clone();
                async move {
                    assert_eq!(msg.message, vec![1, 2, 3]);
                    b.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .build()
            .await
            .unwrap();

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(500)))
            .await
            .unwrap();

        assert_eq!(emails.load(Ordering::SeqCst), 1);
        assert_eq!(billing.load(Ordering::SeqCst), 1);

        rbmq.delete_queue("emails").await.unwrap();
        rbmq.delete_queue("billing").await.unwrap();
    });
}

#[test]
fn empty_routes_fail_to_build() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let result = Worker::builder(opts(&ctx.ns)).build().await;
        assert!(matches!(result, Err(RbmqError::NoAttributeSupplied)));
    });
}

#[test]
fn heartbeat_keeps_slow_handler_safe() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        // Tiny vt so the heartbeat is the only thing keeping the message hidden.
        rbmq.create_queue("q", Some(Duration::from_millis(100)), None, None)
            .await
            .unwrap();
        rbmq.send_message("q", "slow".to_string(), None)
            .await
            .unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .heartbeat_interval(Duration::from_millis(50))
            .visibility_extension(Duration::from_millis(500))
            .route("q", |_msg: RbmqMessage<String>| async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                Ok::<(), Infallible>(())
            })
            .build()
            .await
            .unwrap();

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(800)))
            .await
            .unwrap();

        // Handler completed → delete_message ran → queue is empty (rc==1, not redelivered).
        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0, "message should be deleted exactly once");
        assert_eq!(
            attrs.totalrecv, 1,
            "should have been received once, not redelivered"
        );

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn dlq_max_failures_zero_routes_on_first_failure() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("work", None, None, None).await.unwrap();
        rbmq.create_queue("dead", None, None, None).await.unwrap();
        rbmq.send_message("work", "doomed".to_string(), None)
            .await
            .unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .dlq("dead", 0)
            .route("work", |_msg: RbmqMessage<String>| async {
                Err::<(), HandlerErr>(HandlerErr)
            })
            .build()
            .await
            .unwrap();
        worker
            .run_until(tokio::time::sleep(Duration::from_millis(300)))
            .await
            .unwrap();

        // Source drained, message ended up in the DLQ.
        let work_attrs = rbmq.get_queue_attributes("work").await.unwrap();
        assert_eq!(work_attrs.msgs, 0, "source queue should be empty");
        let dead_msg = rbmq
            .receive_message::<String>("dead", None)
            .await
            .unwrap()
            .expect("DLQ should have the message");
        assert_eq!(dead_msg.message, "doomed");

        rbmq.delete_queue("work").await.unwrap();
        rbmq.delete_queue("dead").await.unwrap();
    });
}

#[test]
fn dlq_per_route_override_takes_precedence_over_global() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("work", None, None, None).await.unwrap();
        rbmq.create_queue("global_dead", None, None, None)
            .await
            .unwrap();
        rbmq.create_queue("work_dead", None, None, None)
            .await
            .unwrap();
        rbmq.send_message("work", "x".to_string(), None)
            .await
            .unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .dlq("global_dead", 0)
            .dlq_for("work", "work_dead", 0)
            .route("work", |_msg: RbmqMessage<String>| async {
                Err::<(), HandlerErr>(HandlerErr)
            })
            .build()
            .await
            .unwrap();
        worker
            .run_until(tokio::time::sleep(Duration::from_millis(300)))
            .await
            .unwrap();

        // Per-route override wins.
        let global_attrs = rbmq.get_queue_attributes("global_dead").await.unwrap();
        assert_eq!(global_attrs.msgs, 0);
        let routed = rbmq
            .receive_message::<String>("work_dead", None)
            .await
            .unwrap();
        assert!(routed.is_some(), "message should be in the per-route DLQ");

        rbmq.delete_queue("work").await.unwrap();
        rbmq.delete_queue("global_dead").await.unwrap();
        rbmq.delete_queue("work_dead").await.unwrap();
    });
}

#[test]
fn dlq_max_failures_one_keeps_message_until_second_failure() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        // Tiny vt so the message is redelivered quickly inside the test window.
        rbmq.create_queue("work", Some(Duration::from_millis(100)), None, None)
            .await
            .unwrap();
        rbmq.create_queue("dead", None, None, None).await.unwrap();
        rbmq.send_message("work", "retry".to_string(), None)
            .await
            .unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            // We want to exercise the DLQ path on rc=2, but the worker's heartbeat would keep
            // extending vt and prevent redelivery. Set a heartbeat much longer than the test
            // so it never fires, and a tiny visibility_extension just in case it does.
            .heartbeat_interval(Duration::from_secs(10))
            .visibility_extension(Duration::from_millis(50))
            .dlq("dead", 1)
            .route("work", |_msg: RbmqMessage<String>| async {
                Err::<(), HandlerErr>(HandlerErr)
            })
            .build()
            .await
            .unwrap();

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(800)))
            .await
            .unwrap();

        // After ~800ms with a 100ms vt, the message has been redelivered ~7 times.
        // First failure (rc=1): leaves it. Second failure (rc=2 > max_failures=1): DLQ.
        let work_attrs = rbmq.get_queue_attributes("work").await.unwrap();
        assert_eq!(work_attrs.msgs, 0, "source should be drained");
        let dead = rbmq
            .receive_message::<String>("dead", None)
            .await
            .unwrap()
            .expect("DLQ should have the message");
        assert!(dead.rc >= 2, "rc should be preserved (was {})", dead.rc);

        rbmq.delete_queue("work").await.unwrap();
        rbmq.delete_queue("dead").await.unwrap();
    });
}

#[test]
fn dlq_self_loop_rejected_at_build() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let result = Worker::builder(opts(&ctx.ns))
            .dlq("work", 0)
            .route("work", |_msg: RbmqMessage<String>| async {
                Ok::<(), Infallible>(())
            })
            .build()
            .await;
        assert!(matches!(result, Err(RbmqError::InvalidFormat(_))));
    });
}

#[test]
fn dlq_not_triggered_when_handler_succeeds() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("work", None, None, None).await.unwrap();
        rbmq.create_queue("dead", None, None, None).await.unwrap();
        rbmq.send_message("work", "ok".to_string(), None)
            .await
            .unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .poll_interval(Duration::from_millis(50))
            .dlq("dead", 0)
            .route("work", |_msg: RbmqMessage<String>| async {
                Ok::<(), Infallible>(())
            })
            .build()
            .await
            .unwrap();
        worker
            .run_until(tokio::time::sleep(Duration::from_millis(300)))
            .await
            .unwrap();

        let dead_attrs = rbmq.get_queue_attributes("dead").await.unwrap();
        assert_eq!(dead_attrs.msgs, 0, "successful handler must not DLQ");

        rbmq.delete_queue("work").await.unwrap();
        rbmq.delete_queue("dead").await.unwrap();
    });
}

// ---------------------------------------------------------------------------

#[derive(Debug)]
struct HandlerErr;

impl std::fmt::Display for HandlerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "boom")
    }
}

impl std::error::Error for HandlerErr {}
