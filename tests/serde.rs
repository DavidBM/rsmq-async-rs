#![cfg(feature = "serde")]

mod support;

use rbmq::{Json, Rbmq, RbmqConnection as _, RbmqError, RbmqJsonExt as _};
use serde::{Deserialize, Serialize};
use support::*;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

async fn new_rbmq(ctx: &TestContext) -> Rbmq {
    Rbmq::new_with_connection(ctx.async_connection().await.unwrap(), false, Some(&ctx.ns))
        .await
        .unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Job {
    id: u64,
    name: String,
}

#[test]
fn json_wrapper_round_trip() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let job = Job {
            id: 7,
            name: "hello".into(),
        };
        rbmq.send_message("q", Json(job.clone()), None)
            .await
            .unwrap();

        let msg = rbmq
            .receive_message::<Json<Job>>("q", None)
            .await
            .unwrap()
            .expect("message present");

        assert_eq!(msg.message.0, job);
        rbmq.delete_message("q", &msg.id).await.unwrap();
        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn ext_trait_round_trip() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let job = Job {
            id: 42,
            name: "world".into(),
        };
        rbmq.send_json("q", &job, None).await.unwrap();

        let msg = rbmq
            .receive_json::<Job>("q", None)
            .await
            .unwrap()
            .expect("message present");

        assert_eq!(msg.message, job);
        rbmq.delete_message("q", &msg.id).await.unwrap();
        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn ext_trait_pop_round_trip() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        let job = Job {
            id: 1,
            name: "pop".into(),
        };
        rbmq.send_json("q", &job, None).await.unwrap();

        let msg = rbmq
            .pop_json::<Job>("q")
            .await
            .unwrap()
            .expect("message present");

        assert_eq!(msg.message, job);
        // pop already deletes; queue should now be empty
        assert!(rbmq.pop_json::<Job>("q").await.unwrap().is_none());
        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn ext_trait_decode_error_surfaces_as_json_error() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        // Send something that's not JSON for the target type.
        rbmq.send_message("q", "not-json".to_string(), None)
            .await
            .unwrap();

        let err = rbmq
            .receive_json::<Job>("q", None)
            .await
            .expect_err("should be a JSON decode error");
        assert!(
            matches!(err, RbmqError::JsonError(_)),
            "expected JsonError, got: {err:?}"
        );

        rbmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn json_wrapper_decode_failure_returns_none_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q", None, None, None).await.unwrap();

        // Plain string, not the JSON shape Json<Job> expects.
        rbmq.send_message("q", "junk".to_string(), None)
            .await
            .unwrap();

        // The wrapper's TryFrom returns Err(Vec<u8>) on parse failure, which
        // bubbles up as RbmqError::CannotDecodeMessage.
        let err = rbmq
            .receive_message::<Json<Job>>("q", None)
            .await
            .expect_err("should be a decode error");
        assert!(matches!(err, RbmqError::CannotDecodeMessage(_)));

        rbmq.delete_queue("q").await.unwrap();
    });
}
