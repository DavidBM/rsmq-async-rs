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
fn send_batch_returns_one_id_per_message_in_order() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let payloads: Vec<String> = (0..5).map(|i| format!("msg-{i}")).collect();
        let ids = rbmq
            .send_message_batch("q", payloads.clone(), None)
            .await
            .unwrap();

        assert_eq!(ids.len(), payloads.len());
        for id in &ids {
            assert!(!id.is_empty());
        }
        // IDs should all be distinct
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());

        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 5);
        assert_eq!(attrs.totalsent, 5);

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn send_batch_empty_input_returns_empty_vec_no_redis_op() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let ids = rbmq
            .send_message_batch::<String>("q", vec![], None)
            .await
            .unwrap();
        assert!(ids.is_empty());

        // Counters untouched.
        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalsent, 0);

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn receive_batch_returns_up_to_max_count() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let payloads: Vec<String> = (0..5).map(|i| format!("msg-{i}")).collect();
        rbmq.send_message_batch("q", payloads.clone(), None)
            .await
            .unwrap();

        let received = rbmq
            .receive_message_batch::<String>("q", None, 3)
            .await
            .unwrap();
        assert_eq!(received.len(), 3);
        for msg in &received {
            assert_eq!(msg.rc, 1);
            assert!(payloads.contains(&msg.message));
        }

        // Queue still holds all 5 (we didn't delete), and we incremented totalrecv 3 times.
        // We deliberately don't assert on hiddenmsgs here — the JS-compat threshold in
        // getQueueAttributes.lua is second-rounded, so messages and the threshold can
        // both fall on the same second and the count flickers.
        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 5);
        assert_eq!(attrs.totalrecv, 3);

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn receive_batch_returns_fewer_when_queue_underfilled() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        rbmq.send_message_batch("q", vec!["a".to_string(), "b".to_string()], None)
            .await
            .unwrap();

        let received = rbmq
            .receive_message_batch::<String>("q", None, 10)
            .await
            .unwrap();
        assert_eq!(received.len(), 2);

        // Empty queue → empty Vec
        rbmq.delete_message("q", &received[0].id).await.unwrap();
        rbmq.delete_message("q", &received[1].id).await.unwrap();
        let drained = rbmq
            .receive_message_batch::<String>("q", None, 10)
            .await
            .unwrap();
        assert!(drained.is_empty());

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn receive_batch_max_count_zero_is_noop() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();
        rbmq.send_message("q", "x".to_string(), None).await.unwrap();

        let out = rbmq
            .receive_message_batch::<String>("q", None, 0)
            .await
            .unwrap();
        assert!(out.is_empty());

        // No counters touched.
        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.totalrecv, 0);

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn batch_respects_maxsize() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, Some(1024))
            .await
            .unwrap();

        // One small message, one too-big — entire batch should fail without inserting any.
        let big = "x".repeat(2000);
        let result = rbmq
            .send_message_batch("q", vec!["small".to_string(), big], None)
            .await;
        assert!(result.is_err());

        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0, "no messages should have landed");
        assert_eq!(attrs.totalsent, 0);

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn receive_batch_then_delete_each() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let n = 4;
        let payloads: Vec<String> = (0..n).map(|i| format!("p{i}")).collect();
        rbmq.send_message_batch("q", payloads, None).await.unwrap();

        let msgs = rbmq
            .receive_message_batch::<String>("q", Some(Duration::from_secs(30)), n as u32)
            .await
            .unwrap();
        assert_eq!(msgs.len(), n as usize);

        for m in &msgs {
            assert!(rbmq.delete_message("q", &m.id).await.unwrap());
        }

        let attrs = rbmq.get_queue_attributes("q").await.unwrap();
        assert_eq!(attrs.msgs, 0);

        rbmq.delete_queue("q").await.unwrap();
    });
}
