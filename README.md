# Asyncnsq

[![Crate](https://img.shields.io/crates/v/asyncnsq.svg)](https://crates.io/crates/asyncnsq)

A Rust Async nsq client.

almost fulfill all the protocol but compressing.

for now, it is only a beta project. and it has a lot of noise log message.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
asyncnsq = "*"
```

## Examples

### nsqd writer example

```rust
use async_std::io;
use async_std::task;
use asyncnsq::create_writer;
use asyncnsq::data::Address;
use serde::{Deserialize, Serialize};

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
```

### nsq httpd example

```rust
use async_std::io;
use async_std::sync::Arc;
use async_std::{stream, task};
use async_trait::async_trait;
use asyncnsq::data::{Address, Msg};
use asyncnsq::utils::read_toml_config;
use asyncnsq::{create_writer, subscribe};
use asyncnsq::{MsgHandler, NsqHttpdInitConfig, NsqLookupd};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub struct TestMsg {
    name: String,
}

impl TestMsg {
    fn new() -> TestMsg {
        TestMsg {
            name: "test".to_string(),
        }
    }
}

#[async_trait]
impl MsgHandler for TestMsg {
    async fn handler(&self, msg: Msg) -> Option<Msg> {
        dbg!(&self.name);
        let res = msg.finish().await;
        dbg!(res);
        None
    }
}

pub async fn msg_handler(data: i32, msg: Msg) -> Option<Msg> {
    println!("custom msg handler->{:?} ,self.name->{:?}", msg, data);
    if let Ok(res) = msg.req(1000).await {
        None
    } else {
        Some(msg)
    }
}

async fn test_reader() -> io::Result<()> {
    let nsq_init_config: NsqHttpdInitConfig =
        read_toml_config::<NsqHttpdInitConfig>("tests/config.toml".to_string()).await?;
    let pt = serde_json::to_string(&nsq_init_config.identify)?;
    let addr = Address::ReaderdAddr("127.0.0.1:4150".to_string());
    let tt = TestMsg::new();
    let mut reader = subscribe(
        addr,
        Arc::new(Box::new(tt)),
        "test_async_nsq".to_string(),
        "tt".to_string(),
        pt,
    )
    .await?;
    Ok(())
}

async fn test_lookupd_reader() -> io::Result<()> {
    let tt = TestMsg::new();
    let nsq_init_config: NsqHttpdInitConfig =
        read_toml_config::<NsqHttpdInitConfig>("tests/config.toml".to_string()).await?;
    let httpd_address = nsq_init_config.httpd_adress;
    let mut m_lookupd = NsqLookupd::new(
        httpd_address.address,
        httpd_address.topic,
        httpd_address.nsq_channel,
        nsq_init_config.identify,
    );
    let interval = stream::interval(Duration::from_secs(60));
    m_lookupd
        .periodically_lookup(interval, Arc::new(Box::new(tt)))
        .await?;
    Ok(())
}

async fn test_writer() -> io::Result<()> {
    let addr = Address::ReaderdAddr("127.0.0.1:4150".to_string());
    dbg!("test_publish");
    let mut writer = create_writer(addr).await?;
    writer.connect().await?;
    #[derive(Serialize, Deserialize)]
    pub struct TestMsg {
        name: String,
    }
    loop {
        let tt = TestMsg {
            name: "ss".to_string(),
        };
        let mut ttt = Vec::new();
        ttt.push(tt);
        let tt = TestMsg {
            name: "ss".to_string(),
        };
        ttt.push(tt);
        writer.multi_publish("tt", ttt).await?;
        let tt = TestMsg {
            name: "ss".to_string(),
        };
        writer.publish("tt", tt).await?;
        let tt = TestMsg {
            name: "ss".to_string(),
        };
        writer.delay_publish("tt", 1000, tt).await?;
        task::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
fn main() -> io::Result<()> {
    task::block_on(async { test_lookupd_reader().await })
}
```

## nsq init config toml example

```toml
[identify]
client_id="asyncnsq"
heartbeat_interval=10000
output_buffer_timeout=30000
msg_timeout = 7200
user_agent="rust"

[httpd_adress]
address="127.0.0.1:4161"
topic="test_async_nsq"
nsq_channel="tt"
```

## instructions

i'm glad that it can help you with your work