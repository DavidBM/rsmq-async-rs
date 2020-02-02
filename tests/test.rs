mod support;

use rsmq_async::Rsmq;
use support::*;

#[tokio::test]
async fn it_works() {
    let ctx = TestContext::new();
    let connection = ctx.async_connection().await.unwrap();

    let mut rsmq = Rsmq::new_with_connection(Default::default(), connection);

    rsmq.create_queue("myqueue", None, None, None).await.unwrap();

    rsmq.send_message("myqueue", "testmessage", None).await.unwrap();

    let message = rsmq.receive_message("myqueue", None).await.unwrap();
    
    assert!(message.is_some());

    let message = message.unwrap();

    rsmq.delete_message("myqueue", &message.id).await.unwrap();

    assert_eq!(message.message, "testmessage".to_string());

    let message = rsmq.receive_message("myqueue", None).await.unwrap();

    assert!(message.is_none());
}
