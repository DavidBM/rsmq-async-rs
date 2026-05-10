mod support;

use rbmq::{RedisBytes, Rbmq, RbmqConnection as _, RbmqError};
use std::{convert::TryFrom, time::Duration};
use support::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

async fn new_rbmq(ctx: &TestContext) -> Rbmq {
    Rbmq::new_with_connection(ctx.async_connection().await.unwrap(), false, Some(&ctx.ns))
        .await
        .unwrap()
}

#[test]
fn send_receiving_deleting_message() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue1", None, None, None).await.unwrap();

        rbmq.send_message("queue1", "testmessage", None)
            .await
            .unwrap();

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_some());

        let message = message.unwrap();

        rbmq.delete_message("queue1", &message.id).await.unwrap();

        assert_eq!(message.message, "testmessage".to_string());

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();

        assert!(message.is_none());
        rbmq.delete_queue("queue1").await.unwrap();
    })
}

#[test]
fn send_receiving_delayed_message() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue1", None, None, None).await.unwrap();

        rbmq.send_message("queue1", "testmessage", Some(Duration::from_secs(2)))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_none());

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_none());

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_none());

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_some());

        let message = message.unwrap();

        rbmq.delete_message("queue1", &message.id).await.unwrap();

        assert_eq!(message.message, "testmessage".to_string());

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();

        assert!(message.is_none());
        rbmq.delete_queue("queue1").await.unwrap();
    })
}

#[test]
fn send_receiving_deleting_message_vec_u8() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue1", None, None, None).await.unwrap();

        rbmq.send_message("queue1", "testmessage", None)
            .await
            .unwrap();

        let message = rbmq
            .receive_message::<Vec<u8>>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_some());

        let message = message.unwrap();

        rbmq.delete_message("queue1", &message.id).await.unwrap();

        assert_eq!(message.message, b"testmessage");

        let message = rbmq
            .receive_message::<Vec<u8>>("queue1", None)
            .await
            .unwrap();

        assert!(message.is_none());
        rbmq.delete_queue("queue1").await.unwrap();
    })
}

#[test]
fn send_receiving_deleting_message_custom_type() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    #[derive(Debug, PartialEq)]
    struct MyValue(Vec<u8>);

    impl TryFrom<RedisBytes> for MyValue {
        type Error = Vec<u8>;

        fn try_from(t: RedisBytes) -> Result<Self, Self::Error> {
            Ok(MyValue(t.into_bytes()))
        }
    }

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue1", None, None, None).await.unwrap();

        rbmq.send_message("queue1", b"testmessage".to_owned().to_vec(), None)
            .await
            .unwrap();

        let message = rbmq
            .receive_message::<MyValue>("queue1", None)
            .await
            .unwrap();
        assert!(message.is_some());

        let message = message.unwrap();

        rbmq.delete_message("queue1", &message.id).await.unwrap();

        assert_eq!(message.message, MyValue(b"testmessage".to_owned().to_vec()));

        let message = rbmq
            .receive_message::<MyValue>("queue1", None)
            .await
            .unwrap();

        assert!(message.is_none());
        rbmq.delete_queue("queue1").await.unwrap();
    })
}

#[test]
fn pop_message() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue2", None, None, None).await.unwrap();

        rbmq.send_message("queue2", "testmessage", None)
            .await
            .unwrap();

        let message = rbmq.pop_message::<String>("queue2").await.unwrap();

        assert!(message.is_some());

        let message = message.unwrap();

        assert_eq!(message.message, "testmessage");

        let message = rbmq.pop_message::<String>("queue2").await.unwrap();

        assert!(message.is_none());

        rbmq.delete_queue("queue2").await.unwrap();
    })
}

#[test]
fn pop_message_vec_u8() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue2", None, None, None).await.unwrap();

        rbmq.send_message("queue2", "testmessage", None)
            .await
            .unwrap();

        let message = rbmq.pop_message::<Vec<u8>>("queue2").await.unwrap();

        assert!(message.is_some());

        let message = message.unwrap();

        assert_eq!(message.message, "testmessage".as_bytes());

        let message = rbmq.pop_message::<String>("queue2").await.unwrap();

        assert!(message.is_none());

        rbmq.delete_queue("queue2").await.unwrap();
    })
}

#[test]
fn creating_queue() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue3", None, None, None).await.unwrap();

        let queues = rbmq.list_queues().await.unwrap();

        assert_eq!(queues, vec!("queue3"));

        let result = rbmq.create_queue("queue3", None, None, None).await;

        assert!(result.is_err());

        if let Err(RbmqError::QueueExists) = result {
            rbmq.delete_queue("queue3").await.unwrap();
        } else {
            panic!()
        }
    })
}

#[test]
fn updating_queue() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue4", None, None, None).await.unwrap();

        let attributes = rbmq.get_queue_attributes("queue4").await.unwrap();

        assert_eq!(attributes.vt, Duration::from_secs(30));
        assert_eq!(attributes.delay, Duration::ZERO);
        assert_eq!(attributes.maxsize, 65536);
        assert_eq!(attributes.totalrecv, 0);
        assert_eq!(attributes.totalsent, 0);
        assert_eq!(attributes.msgs, 0);
        assert_eq!(attributes.hiddenmsgs, 0);
        assert!(attributes.created > 0);
        assert!(attributes.modified > 0);

        rbmq.set_queue_attributes(
            "queue4",
            Some(Duration::from_secs(45)),
            Some(Duration::from_secs(5)),
            Some(2048),
        )
        .await
        .unwrap();

        let attributes = rbmq.get_queue_attributes("queue4").await.unwrap();

        assert_eq!(attributes.vt, Duration::from_secs(45));
        assert_eq!(attributes.delay, Duration::from_secs(5));
        assert_eq!(attributes.maxsize, 2048);
        assert_eq!(attributes.totalrecv, 0);
        assert_eq!(attributes.totalsent, 0);
        assert_eq!(attributes.msgs, 0);
        assert_eq!(attributes.hiddenmsgs, 0);
        assert!(attributes.created > 0);
        assert!(attributes.modified > 0);

        rbmq.delete_queue("queue4").await.unwrap();
    })
}

#[test]
fn deleting_queue() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue5", None, None, None).await.unwrap();

        let queues = rbmq.list_queues().await.unwrap();

        assert_eq!(queues, vec!("queue5"));

        rbmq.delete_queue("queue5").await.unwrap();

        let queues = rbmq.list_queues().await.unwrap();

        assert_eq!(queues, Vec::<String>::new());

        let result = rbmq.delete_queue("queue5").await;

        assert!(result.is_err());

        if let Err(RbmqError::QueueNotFound) = result {
        } else {
            panic!("{:?}", result)
        }

        let result = rbmq.get_queue_attributes("queue5").await;

        assert!(result.is_err());

        if let Err(RbmqError::QueueNotFound) = result {
        } else {
            panic!("{:?}", result)
        }

        let result = rbmq
            .set_queue_attributes(
                "queue5",
                Some(Duration::from_secs(45)),
                Some(Duration::from_secs(5)),
                Some(2048),
            )
            .await;

        assert!(result.is_err());

        if let Err(RbmqError::QueueNotFound) = result {
        } else {
            panic!("{:?}", result)
        }
    })
}

#[test]
fn change_message_visibility() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue6", None, None, None).await.unwrap();

        rbmq.send_message("queue6", "testmessage", None)
            .await
            .unwrap();

        let message = rbmq
            .receive_message::<String>("queue6", None)
            .await
            .unwrap();
        assert!(message.is_some());

        let message_id = message.unwrap().id;

        let message = rbmq
            .receive_message::<String>("queue6", None)
            .await
            .unwrap();
        assert!(message.is_none());

        rbmq.change_message_visibility("queue6", &message_id, Duration::ZERO)
            .await
            .unwrap();

        let ten_millis = std::time::Duration::from_millis(10);
        std::thread::sleep(ten_millis);

        let message = rbmq
            .receive_message::<String>("queue6", None)
            .await
            .unwrap();
        assert!(message.is_some());

        assert_eq!(message_id, message.unwrap().id);

        rbmq.delete_queue("queue6").await.unwrap();
    })
}

#[test]
fn change_queue_size() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue6", None, None, None).await.unwrap();

        rbmq.set_queue_attributes("queue6", None, None, Some(-1))
            .await
            .unwrap();

        let attributes = rbmq.get_queue_attributes("queue6").await.unwrap();

        assert_eq!(attributes.maxsize, -1);
    })
}

#[cfg(feature = "break-js-comp")]
#[test]
fn sent_messages_must_keep_order() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let ctx = TestContext::new();
        let connection = ctx.async_connection().await.unwrap();
        let mut rbmq = Rbmq::new_with_connection(connection, false, Some(&ctx.ns))
            .await
            .unwrap();

        rbmq.create_queue("queue1", None, None, None).await.unwrap();

        for i in 0..10000 {
            rbmq.send_message("queue1", format!("testmessage{}", i), None)
                .await
                .unwrap();
        }

        for i in 0..10000 {
            let message = rbmq
                .receive_message::<String>("queue1", None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.message, format!("testmessage{}", i));

            rbmq.delete_message("queue1", &message.id).await.unwrap();
        }

        let message = rbmq
            .receive_message::<String>("queue1", None)
            .await
            .unwrap();

        assert!(message.is_none());
        rbmq.delete_queue("queue1").await.unwrap();
    })
}

// ---------------------------------------------------------------------------
// Queue name validation (tests the valid_name_format fix: && → ||, char check)
// ---------------------------------------------------------------------------

#[test]
fn queue_name_empty_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        assert!(matches!(
            rbmq.create_queue("", None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_too_long_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        let long = "a".repeat(161);
        assert!(matches!(
            rbmq.create_queue(&long, None, None, None).await,
            Err(RbmqError::InvalidFormat(_))
        ));
    });
}

#[test]
fn queue_name_invalid_chars_are_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        for bad in &["has space", "has.dot", "has@at", "has/slash", "has:colon"] {
            assert!(
                matches!(
                    rbmq.create_queue(bad, None, None, None).await,
                    Err(RbmqError::InvalidFormat(_))
                ),
                "expected InvalidFormat for {:?}",
                bad
            );
        }
    });
}

#[test]
fn queue_name_valid_boundaries_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("valid-name_123", None, None, None)
            .await
            .unwrap();
        let max_name = "a".repeat(160);
        rbmq.create_queue(&max_name, None, None, None)
            .await
            .unwrap();
    });
}

// ---------------------------------------------------------------------------
// maxsize validation on create_queue
// ---------------------------------------------------------------------------

#[test]
fn create_queue_maxsize_boundaries() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;

        rbmq.create_queue("q-min", None, None, Some(1024))
            .await
            .unwrap();
        rbmq.create_queue("q-max", None, None, Some(65536))
            .await
            .unwrap();
        rbmq.create_queue("q-unlimited", None, None, Some(-1))
            .await
            .unwrap();

        assert!(matches!(
            rbmq.create_queue("q-bad-low", None, None, Some(1023)).await,
            Err(RbmqError::InvalidValue(_, _, _))
        ));
        assert!(matches!(
            rbmq.create_queue("q-bad-high", None, None, Some(65537))
                .await,
            Err(RbmqError::InvalidValue(_, _, _))
        ));
    });
}

// ---------------------------------------------------------------------------
// Message size enforcement
// ---------------------------------------------------------------------------

#[test]
fn message_exactly_at_maxsize_is_accepted() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();
        rbmq.send_message("q1", "x".repeat(65536), None)
            .await
            .unwrap();
    });
}

#[test]
fn message_exceeding_maxsize_is_rejected() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();
        assert!(matches!(
            rbmq.send_message("q1", "x".repeat(65537), None).await,
            Err(RbmqError::MessageTooLong)
        ));
    });
}

#[test]
fn unlimited_maxsize_accepts_large_message() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, Some(-1)).await.unwrap();
        rbmq.send_message("q1", "x".repeat(100_000), None)
            .await
            .unwrap();
    });
}

// ---------------------------------------------------------------------------
// Queue attributes and message counters (tests getQueueAttributes.lua)
// ---------------------------------------------------------------------------

#[test]
fn queue_attributes_track_sent_and_received_counts() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();

        let attrs = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(attrs.msgs, 0);
        assert_eq!(attrs.totalsent, 0);
        assert_eq!(attrs.totalrecv, 0);
        assert!(attrs.created > 0);
        assert!(attrs.modified > 0);

        for i in 0..3u32 {
            rbmq.send_message("q1", format!("msg{i}"), None)
                .await
                .unwrap();
        }
        let attrs = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(attrs.msgs, 3);
        assert_eq!(attrs.totalsent, 3);
        assert_eq!(attrs.totalrecv, 0);

        // Receive 2 — they become hidden (not deleted)
        rbmq.receive_message::<String>("q1", None).await.unwrap();
        rbmq.receive_message::<String>("q1", None).await.unwrap();

        let attrs = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(attrs.msgs, 3);
        assert_eq!(attrs.totalsent, 3);
        assert_eq!(attrs.totalrecv, 2);
    });
}

#[test]
fn get_queue_attributes_on_nonexistent_queue() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        assert!(matches!(
            rbmq.get_queue_attributes("noqueue").await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

// ---------------------------------------------------------------------------
// Partial set_queue_attributes (tests the pipe-pattern fix)
// ---------------------------------------------------------------------------

#[test]
fn set_queue_attributes_only_updates_specified_fields() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();

        // Update only vt — delay and maxsize keep their defaults
        rbmq.set_queue_attributes("q1", Some(Duration::from_secs(60)), None, None)
            .await
            .unwrap();
        let a = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(a.vt, Duration::from_secs(60));
        assert_eq!(a.delay, Duration::ZERO);
        assert_eq!(a.maxsize, 65536);

        // Update only delay — vt must stay at 60 s
        rbmq.set_queue_attributes("q1", None, Some(Duration::from_secs(10)), None)
            .await
            .unwrap();
        let a = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(a.vt, Duration::from_secs(60));
        assert_eq!(a.delay, Duration::from_secs(10));
        assert_eq!(a.maxsize, 65536);

        // Update only maxsize — others unchanged
        rbmq.set_queue_attributes("q1", None, None, Some(2048))
            .await
            .unwrap();
        let a = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(a.vt, Duration::from_secs(60));
        assert_eq!(a.delay, Duration::from_secs(10));
        assert_eq!(a.maxsize, 2048);
    });
}

// ---------------------------------------------------------------------------
// Receive count and first-received timestamp (rc / fr fields)
// ---------------------------------------------------------------------------

#[test]
fn receive_count_increments_and_first_received_is_preserved() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();
        rbmq.send_message("q1", "hello", None).await.unwrap();

        let msg = rbmq
            .receive_message::<String>("q1", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.rc, 1);
        assert!(msg.fr > 0);
        let first_received = msg.fr;
        let id = msg.id.clone();

        // Make the message visible again immediately
        rbmq.change_message_visibility("q1", &id, Duration::ZERO)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let msg2 = rbmq
            .receive_message::<String>("q1", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg2.id, id);
        assert_eq!(msg2.rc, 2);
        assert_eq!(msg2.fr, first_received); // fr must not change on re-delivery
    });
}

// ---------------------------------------------------------------------------
// delete_message return value
// ---------------------------------------------------------------------------

#[test]
fn delete_message_returns_true_then_false() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();
        rbmq.send_message("q1", "hello", None).await.unwrap();

        let msg = rbmq
            .receive_message::<String>("q1", None)
            .await
            .unwrap()
            .unwrap();

        assert!(rbmq.delete_message("q1", &msg.id).await.unwrap());
        assert!(!rbmq.delete_message("q1", &msg.id).await.unwrap());
    });
}

// ---------------------------------------------------------------------------
// message.sent field is populated (tests the safe .get(0..10) slice)
// ---------------------------------------------------------------------------

#[test]
fn message_sent_field_is_nonzero() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        rbmq.create_queue("q1", None, None, None).await.unwrap();
        rbmq.send_message("q1", "hello", None).await.unwrap();

        let msg = rbmq
            .receive_message::<String>("q1", None)
            .await
            .unwrap()
            .unwrap();
        assert!(msg.sent > 0, "sent should be a non-zero timestamp");
    });
}

// ---------------------------------------------------------------------------
// Operations on non-existent queues
// ---------------------------------------------------------------------------

#[test]
fn operations_on_nonexistent_queue_return_queue_not_found() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;

        assert!(matches!(
            rbmq.send_message("ghost", "msg", None).await,
            Err(RbmqError::QueueNotFound)
        ));
        assert!(matches!(
            rbmq.receive_message::<String>("ghost", None).await,
            Err(RbmqError::QueueNotFound)
        ));
        assert!(matches!(
            rbmq.pop_message::<String>("ghost").await,
            Err(RbmqError::QueueNotFound)
        ));
        assert!(matches!(
            rbmq.get_queue_attributes("ghost").await,
            Err(RbmqError::QueueNotFound)
        ));
        assert!(matches!(
            rbmq.set_queue_attributes("ghost", None, None, None).await,
            Err(RbmqError::QueueNotFound)
        ));
        assert!(matches!(
            rbmq.change_message_visibility("ghost", "fakeid", Duration::from_secs(5))
                .await,
            Err(RbmqError::QueueNotFound)
        ));
    });
}

// ---------------------------------------------------------------------------
// Nil body guard in receiveMessage.lua:
// totalrecv must NOT increment when the message body is missing from the hash
// ---------------------------------------------------------------------------

#[test]
fn receive_skips_phantom_message_without_body() {
    rt().block_on(async {
        let ctx = TestContext::new();
        let mut rbmq = new_rbmq(&ctx).await;
        let mut raw = ctx.async_connection().await.unwrap();

        rbmq.create_queue("q1", None, None, None).await.unwrap();

        // Inject a phantom message ID directly into the sorted set (no hash entry).
        // Score 0 is always in the past, so it is immediately visible.
        redis::cmd("ZADD")
            .arg(format!("{}:q1", ctx.ns))
            .arg(0u64)
            .arg("phantom-id")
            .query_async::<()>(&mut raw)
            .await
            .unwrap();

        let msg = rbmq.receive_message::<String>("q1", None).await.unwrap();
        assert!(msg.is_none(), "phantom message should not be returned");

        let attrs = rbmq.get_queue_attributes("q1").await.unwrap();
        assert_eq!(
            attrs.totalrecv, 0,
            "totalrecv must not increment for nil body"
        );
        assert_eq!(attrs.msgs, 1, "phantom ID is still in the sorted set");
    });
}
