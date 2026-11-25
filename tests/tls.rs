#![cfg(any(feature = "tokio-tls", feature = "smol-tls"))]

use rbmq::{Rbmq, RbmqConnection};

#[tokio::test]
#[ignore = "requires the Redis TLS endpoint from tools/test-tls.sh"]
async fn validates_redis_server_identity() {
    let port = std::env::var("RBMQ_TLS_TEST_PORT").unwrap();
    let mut queue = Rbmq::connect(&format!("rediss://localhost:{port}"))
        .await
        .unwrap();
    queue.ensure_queue("tls").await.unwrap();
    queue.send_message("tls", "hello", None).await.unwrap();
    assert_eq!(
        queue
            .pop_message::<String>("tls")
            .await
            .unwrap()
            .unwrap()
            .message,
        "hello"
    );
    assert!(Rbmq::connect(&format!("rediss://127.0.0.1:{port}"))
        .await
        .is_err());
}
