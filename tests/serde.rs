#![cfg(feature = "serde")]

mod support;

use rsmq_async::{Json, Rsmq, RsmqConnection as _, RsmqError, RsmqJsonExt as _};
use serde::{Deserialize, Serialize};
use support::*;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

async fn new_rsmq(ctx: &TestContext) -> Rsmq {
    Rsmq::new_with_connection(ctx.async_connection().await.unwrap(), false, Some(&ctx.ns))
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
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();

        let job = Job {
            id: 7,
            name: "hello".into(),
        };
        rsmq.send_message("q", Json(job.clone()), None)
            .await
            .unwrap();

        let msg = rsmq
            .receive_message::<Json<Job>>("q", None)
            .await
            .unwrap()
            .expect("message present");

        assert_eq!(msg.message.0, job);
        rsmq.delete_message("q", &msg.id).await.unwrap();
        rsmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn ext_trait_round_trip() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();

        let job = Job {
            id: 42,
            name: "world".into(),
        };
        rsmq.send_json("q", &job, None).await.unwrap();

        let msg = rsmq
            .receive_json::<Job>("q", None)
            .await
            .unwrap()
            .expect("message present");

        assert_eq!(msg.message, job);
        rsmq.delete_message("q", &msg.id).await.unwrap();
        rsmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn ext_trait_pop_round_trip() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();

        let job = Job {
            id: 1,
            name: "pop".into(),
        };
        rsmq.send_json("q", &job, None).await.unwrap();

        let msg = rsmq
            .pop_json::<Job>("q")
            .await
            .unwrap()
            .expect("message present");

        assert_eq!(msg.message, job);
        // pop already deletes; queue should now be empty
        assert!(rsmq.pop_json::<Job>("q").await.unwrap().is_none());
        rsmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn ext_trait_decode_error_surfaces_as_json_error() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();

        // Send something that's not JSON for the target type.
        rsmq.send_message("q", "not-json".to_string(), None)
            .await
            .unwrap();

        let err = rsmq
            .receive_json::<Job>("q", None)
            .await
            .expect_err("should be a JSON decode error");
        assert!(
            matches!(err, RsmqError::JsonError(_)),
            "expected JsonError, got: {err:?}"
        );

        rsmq.delete_queue("q").await.unwrap();
    });
}

#[test]
fn json_wrapper_decode_failure_returns_none_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rsmq = new_rsmq(&ctx).await;
        rsmq.create_queue("q", None, None, None).await.unwrap();

        // Plain string, not the JSON shape Json<Job> expects.
        rsmq.send_message("q", "junk".to_string(), None)
            .await
            .unwrap();

        // The wrapper's TryFrom returns Err(Vec<u8>) on parse failure, which
        // bubbles up as RsmqError::CannotDecodeMessage.
        let err = rsmq
            .receive_message::<Json<Job>>("q", None)
            .await
            .expect_err("should be a decode error");
        assert!(matches!(err, RsmqError::CannotDecodeMessage(_)));

        rsmq.delete_queue("q").await.unwrap();
    });
}
