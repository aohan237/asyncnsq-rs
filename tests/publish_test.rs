use async_std::io;
use async_std::task;
use asyncnsq::create_writer;
use asyncnsq::data::Address;
use serde::{Deserialize, Serialize};

#[test]
fn test_publish() -> io::Result<()> {
    let addr = Address::ReaderdAddr("127.0.0.1:4150".to_string());
    task::block_on(async {
        dbg!("test_publish");
        let mut writer = create_writer(addr).await?;
        writer.connect().await?;
        #[derive(Serialize, Deserialize)]
        pub struct TestMsg {
            name: String,
        }
        for _ in 0..1000 {
            let tt = TestMsg {
                name: "ss".to_string(),
            };
            writer.publish("test_async_nsq", tt).await?;
        }

        Ok(())
    })
}
