use rsmq::{Rsmq, RsmqError};
use async_std::task;

#[test]
fn it_works() -> Result<(), RsmqError> {
    task::block_on(async {
        let mut rsmq = Rsmq::new(Default::default()).await?;
        
        let message = rsmq.receive_message_async("myqueue", None).await?;
        
        rsmq.delete_message_async("myqueue", &message.id).await?;

        Ok(())
    })
}
