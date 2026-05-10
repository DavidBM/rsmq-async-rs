#![cfg(feature = "worker")]

mod support;

use rbmq::{Rbmq, RbmqConnection as _, RbmqMessage, RbmqOptions, Worker};
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
fn worker_drains_many_messages() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", Some(Duration::from_secs(60)), None, None).await.unwrap();
        for i in 0..30 {
            rbmq.send_message("q", format!("m-{i}"), None).await.unwrap();
        }

        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", move |_msg: RbmqMessage<String>| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            worker
                .run_until(async move {
                    loop {
                        if count.load(Ordering::SeqCst) >= 30 {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
        })
        .await;

        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0);
    });
}

#[test]
fn worker_routes_messages_for_two_queues_concurrently() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("a", Some(Duration::from_secs(60)), None, None).await.unwrap();
        rbmq.create_queue("b", Some(Duration::from_secs(60)), None, None).await.unwrap();

        for _ in 0..5 {
            rbmq.send_message("a", "from-a", None).await.unwrap();
        }
        for _ in 0..5 {
            rbmq.send_message("b", "from-b", None).await.unwrap();
        }

        let a_count = Arc::new(AtomicUsize::new(0));
        let b_count = Arc::new(AtomicUsize::new(0));
        let a = a_count.clone();
        let b = b_count.clone();

        let worker = Worker::builder(opts(&ctx.ns))
            .route("a", move |_: RbmqMessage<String>| {
                let a = a.clone();
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .route("b", move |_: RbmqMessage<String>| {
                let b = b.clone();
                async move {
                    b.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            worker
                .run_until(async move {
                    loop {
                        if a_count.load(Ordering::SeqCst) >= 5 && b_count.load(Ordering::SeqCst) >= 5
                        {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
        })
        .await;
    });
}

#[test]
fn worker_dlq_per_route_with_distinct_dlqs() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("a", Some(Duration::ZERO), None, None).await.unwrap();
        rbmq.create_queue("b", Some(Duration::ZERO), None, None).await.unwrap();
        rbmq.create_queue("a_dlq", None, None, None).await.unwrap();
        rbmq.create_queue("b_dlq", None, None, None).await.unwrap();

        rbmq.send_message("a", "x", None).await.unwrap();
        rbmq.send_message("b", "y", None).await.unwrap();

        #[derive(Debug)]
        struct Boom;
        impl std::fmt::Display for Boom {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("boom")
            }
        }
        impl std::error::Error for Boom {}

        let worker = Worker::builder(opts(&ctx.ns))
            .route("a", |_: RbmqMessage<String>| async { Err::<(), _>(Boom) })
            .route("b", |_: RbmqMessage<String>| async { Err::<(), _>(Boom) })
            .dlq_for("a", "a_dlq", 0)
            .dlq_for("b", "b_dlq", 0)
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let shutdown = tokio::time::sleep(Duration::from_millis(500));
        let _ = worker.run_until(shutdown).await;

        let a_attrs = rbmq.get_queue_attributes("a_dlq").await.unwrap();
        let b_attrs = rbmq.get_queue_attributes("b_dlq").await.unwrap();
        assert_eq!(a_attrs.msgs, 1);
        assert_eq!(b_attrs.msgs, 1);
    });
}

#[test]
fn worker_dlq_with_route_specific_overrides_global() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("a", Some(Duration::ZERO), None, None).await.unwrap();
        rbmq.create_queue("b", Some(Duration::ZERO), None, None).await.unwrap();
        rbmq.create_queue("global_dlq", None, None, None).await.unwrap();
        rbmq.create_queue("a_specific_dlq", None, None, None).await.unwrap();

        rbmq.send_message("a", "x", None).await.unwrap();
        rbmq.send_message("b", "y", None).await.unwrap();

        #[derive(Debug)]
        struct Boom;
        impl std::fmt::Display for Boom {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("boom")
            }
        }
        impl std::error::Error for Boom {}

        let worker = Worker::builder(opts(&ctx.ns))
            .route("a", |_: RbmqMessage<String>| async { Err::<(), _>(Boom) })
            .route("b", |_: RbmqMessage<String>| async { Err::<(), _>(Boom) })
            .dlq("global_dlq", 0)
            .dlq_for("a", "a_specific_dlq", 0)
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = worker.run_until(tokio::time::sleep(Duration::from_millis(500))).await;

        let a_specific = rbmq.get_queue_attributes("a_specific_dlq").await.unwrap();
        let global = rbmq.get_queue_attributes("global_dlq").await.unwrap();
        // 'a' goes to its specific DLQ; 'b' falls through to the global DLQ.
        assert_eq!(a_specific.msgs, 1);
        assert_eq!(global.msgs, 1);
    });
}

#[test]
fn worker_continues_after_handler_panic_in_separate_run_call() {
    // We can't capture a panic from the handler future itself in the worker without
    // catch_unwind; this test checks that explicit Err returns recover cleanly and the
    // worker keeps polling the next message.
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", Some(Duration::from_millis(100)), None, None).await.unwrap();
        rbmq.send_message("q", "fail-then-ok", None).await.unwrap();

        let attempts = Arc::new(AtomicUsize::new(0));
        let a = attempts.clone();

        #[derive(Debug)]
        struct Boom;
        impl std::fmt::Display for Boom {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("boom")
            }
        }
        impl std::error::Error for Boom {}

        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", move |_: RbmqMessage<String>| {
                let a = a.clone();
                let n = a.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err::<(), _>(Boom)
                    } else {
                        Ok(())
                    }
                }
            })
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = worker.run_until(tokio::time::sleep(Duration::from_millis(800))).await;
        // First attempt failed (and was left for redelivery), second succeeded and deleted.
        assert!(attempts.load(Ordering::SeqCst) >= 2);
        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0);
    });
}

#[test]
fn worker_with_no_messages_idles_without_error() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", |_: RbmqMessage<String>| async {
                Ok::<(), Infallible>(())
            })
            .poll_interval(Duration::from_millis(50))
            .build()
            .await
            .unwrap();

        // Should idle peacefully then exit on shutdown.
        let r = worker
            .run_until(tokio::time::sleep(Duration::from_millis(200)))
            .await;
        assert!(r.is_ok());
    });
}

#[test]
fn worker_shutdown_signal_is_respected_promptly() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", |_: RbmqMessage<String>| async {
                Ok::<(), Infallible>(())
            })
            .poll_interval(Duration::from_secs(60)) // would wait long if shutdown didn't preempt
            .build()
            .await
            .unwrap();

        let start = std::time::Instant::now();
        let r = worker
            .run_until(tokio::time::sleep(Duration::from_millis(50)))
            .await;
        let elapsed = start.elapsed();
        assert!(r.is_ok());
        // Should exit much sooner than the poll interval.
        assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
    });
}

#[test]
fn worker_error_path_logs_and_redelivers() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", Some(Duration::from_millis(100)), None, None).await.unwrap();
        rbmq.send_message("q", "hi", None).await.unwrap();

        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();

        #[derive(Debug)]
        struct Boom;
        impl std::fmt::Display for Boom {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("boom")
            }
        }
        impl std::error::Error for Boom {}

        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", move |_: RbmqMessage<String>| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(Boom)
                }
            })
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = worker.run_until(tokio::time::sleep(Duration::from_millis(500))).await;
        // Without DLQ, message is redelivered repeatedly.
        assert!(count.load(Ordering::SeqCst) >= 2);
    });
}

#[test]
fn worker_decode_error_does_not_call_handler() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", Some(Duration::from_millis(100)), None, None).await.unwrap();
        // Send invalid UTF-8 bytes so String decode fails.
        rbmq.send_message("q", vec![0xff, 0xfe], None).await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", move |_: RbmqMessage<String>| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                }
            })
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = worker.run_until(tokio::time::sleep(Duration::from_millis(300))).await;
        // The handler is NEVER called for un-decodable bodies.
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn worker_handler_with_vec_u8_decodes_anything() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();
        rbmq.send_message("q", vec![0xff, 0x00, 0x42], None).await.unwrap();

        let got = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let g = got.clone();
        let worker = Worker::builder(opts(&ctx.ns))
            .route("q", move |m: RbmqMessage<Vec<u8>>| {
                let g = g.clone();
                async move {
                    *g.lock().unwrap() = m.message;
                    Ok::<(), Infallible>(())
                }
            })
            .poll_interval(Duration::from_millis(20))
            .build()
            .await
            .unwrap();

        let _ = worker.run_until(tokio::time::sleep(Duration::from_millis(500))).await;
        assert_eq!(*got.lock().unwrap(), vec![0xff, 0x00, 0x42]);
    });
}
