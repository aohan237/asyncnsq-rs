//! # asyncnsq
//!
//! `asyncnsq` is the async nsq lib for rust ,
//!  which utilizes async-std and async_trait
//!  to implement async nsq client
//!
//! # Examples
//!
//! ## nsqd writer example
//!
//! ```
//! use async_std::io;
//! use async_std::task;
//! use asyncnsq::create_writer;
//! use asyncnsq::data::Address;
//! use serde::{Deserialize, Serialize};
//!
//! fn test_publish() -> io::Result<()> {
//!     let addr = Address::ReaderdAddr("127.0.0.1:4150".to_string());
//!     task::block_on(async {
//!         dbg!("test_publish");
//!         let mut writer = create_writer(addr).await?;
//!         writer.connect().await?;
//!         #[derive(Serialize, Deserialize)]
//!         pub struct TestMsg {
//!             name: String,
//!         }
//!         for _ in 0..1000 {
//!             let tt = TestMsg {
//!                 name: "ss".to_string(),
//!             };
//!             writer.publish("test_async_nsq", tt).await?;
//!         }
//!     
//!         Ok(())
//!     })
//! }
//! ```
//! ## nsq httpd example
//! ```
//! use async_std::io;
//! use async_std::sync::Arc;
//! use async_std::{stream, task};
//! use async_trait::async_trait;
//! use asyncnsq::data::{Address, Msg};
//! use asyncnsq::utils::read_toml_config;
//! use asyncnsq::{create_writer, subscribe};
//! use asyncnsq::{MsgHandler, NsqHttpdInitConfig, NsqLookupd};
//! use serde::{Deserialize, Serialize};
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize)]
//! pub struct TestMsg {
//!     name: String,
//! }
//!
//! impl TestMsg {
//!     fn new() -> TestMsg {
//!         TestMsg {
//!             name: "test".to_string(),
//!         }
//!     }
//! }
//!
//! #[async_trait]
//! impl MsgHandler for TestMsg {
//!     async fn handler(&self, msg: Msg) -> Option<Msg> {
//!         dbg!(&self.name);
//!         let res = msg.finish().await;
//!         dbg!(res);
//!         None
//!     }
//! }
//!
//! pub async fn msg_handler(data: i32, msg: Msg) -> Option<Msg> {
//!     println!("custom msg handler->{:?} ,self.name->{:?}", msg, data);
//!     if let Ok(res) = msg.req(1000).await {
//!         None
//!     } else {
//!         Some(msg)
//!     }
//! }
//!
//! async fn test_reader() -> io::Result<()> {
//!     let nsq_init_config: NsqHttpdInitConfig =
//!         read_toml_config::<NsqHttpdInitConfig>("tests/config.toml".to_string()).await?;
//!     let pt = serde_json::to_string(&nsq_init_config.identify)?;
//!     let addr = Address::ReaderdAddr("127.0.0.1:4150".to_string());
//!     let tt = TestMsg::new();
//!     let mut reader = subscribe(
//!         addr,
//!         Arc::new(Box::new(tt)),
//!         "test_async_nsq".to_string(),
//!         "tt".to_string(),
//!         pt,
//!     )
//!     .await?;
//!     Ok(())
//! }
//!
//! async fn test_lookupd_reader() -> io::Result<()> {
//!     let tt = TestMsg::new();
//!     let nsq_init_config: NsqHttpdInitConfig =
//!         read_toml_config::<NsqHttpdInitConfig>("tests/config.toml".to_string()).await?;
//!     let httpd_address = nsq_init_config.httpd_adress;
//!     let mut m_lookupd = NsqLookupd::new(
//!         httpd_address.address,
//!         httpd_address.topic,
//!         httpd_address.nsq_channel,
//!         nsq_init_config.identify,
//!     );
//!     let interval = stream::interval(Duration::from_secs(60));
//!     m_lookupd
//!         .periodically_lookup(interval, Arc::new(Box::new(tt)))
//!         .await?;
//!     Ok(())
//! }
//!
//! async fn test_writer() -> io::Result<()> {
//!     let addr = Address::ReaderdAddr("127.0.0.1:4150".to_string());
//!     dbg!("test_publish");
//!     let mut writer = create_writer(addr).await?;
//!     writer.connect().await?;
//!     #[derive(Serialize, Deserialize)]
//!     pub struct TestMsg {
//!         name: String,
//!     }
//!     loop {
//!         let tt = TestMsg {
//!             name: "ss".to_string(),
//!         };
//!         let mut ttt = Vec::new();
//!         ttt.push(tt);
//!         let tt = TestMsg {
//!             name: "ss".to_string(),
//!         };
//!         ttt.push(tt);
//!         writer.multi_publish("tt", ttt).await?;
//!         let tt = TestMsg {
//!             name: "ss".to_string(),
//!         };
//!         writer.publish("tt", tt).await?;
//!         let tt = TestMsg {
//!             name: "ss".to_string(),
//!         };
//!         writer.delay_publish("tt", 1000, tt).await?;
//!         task::sleep(Duration::from_secs(1)).await;
//!     }
//!     Ok(())
//! }
//! fn main() -> io::Result<()> {
//!     task::block_on(async { test_lookupd_reader().await })
//! }
//! ```
//! ## nsq init config toml example
//! ``` toml
//! [identify]
//! client_id="asyncnsq"
//! heartbeat_interval=10000
//! output_buffer_timeout=30000
//! msg_timeout = 7200
//! user_agent="rust"
//!
//! [httpd_adress]
//! address="127.0.0.1:4161"
//! topic="test_async_nsq"
//! nsq_channel="tt"
//! ```

use async_std::future;
use async_std::io;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::{channel, Arc, Mutex};
use async_std::{stream, task};
use std::collections::HashSet;
pub mod data;
pub mod reader;
pub mod utils;
pub mod writer;
pub use async_trait::async_trait;
pub use data::{
    Address, IdentifyConfig, Msg, NsqHttpdInitConfig, NsqLookupdConfig, NsqdClientConfig,
    NsqdConfig, NsqdInitConfig,
};
pub use reader::Reader;
pub use utils::read_toml_config;
pub use writer::Writer;

/// async msg handler trait
#[async_trait]
pub trait MsgHandler {
    async fn handler(&self, msg: Msg) -> Option<Msg> {
        Some(msg)
    }
}

pub struct NsqLookupd {
    pub address: String,
    pub cached_address: HashSet<NsqdConfig>,
    pub topic: String,
    pub nsq_channel: String,
    pub identify_config: IdentifyConfig,
}

impl NsqLookupd {
    pub fn new(
        address: String,
        topic: String,
        nsq_channel: String,
        identify_config: IdentifyConfig,
    ) -> NsqLookupd {
        NsqLookupd {
            address,
            topic,
            cached_address: HashSet::new(),
            nsq_channel,
            identify_config,
        }
    }

    /// periodically query the lookupd address for discover nsqd server
    pub async fn periodically_lookup(
        &mut self,
        mut interval: stream::Interval,
        handler: Arc<Box<dyn MsgHandler + Send + Sync>>,
    ) -> io::Result<()> {
        loop {
            match interval.next().await {
                Some(m_interval) => {
                    self.nsqlookup(handler.clone()).await;
                    println!("prints every four seconds");
                }
                None => break,
            }
        }
        Ok(())
    }

    /// the fn of querying the lookupd server
    async fn nsqlookup(
        &mut self,
        handler: Arc<Box<dyn MsgHandler + Send + Sync>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let url = format!(
            "http://{address}/lookup?topic={topic}",
            address = self.address,
            topic = self.topic
        );
        dbg!(&url);
        let mut res = surf::get(url).await?;
        let body = res.body_string().await?;
        dbg!(&body);
        let nsq_lookupd_config: NsqLookupdConfig = serde_json::from_str(body.as_str())?;
        for nsqd_config in nsq_lookupd_config.producers {
            if !self.cached_address.contains(&nsqd_config) {
                dbg!("find new address,{:?}", &nsqd_config);
                let addr = Address::ReaderdAddr(format!(
                    "{}:{}",
                    nsqd_config.broadcast_address, nsqd_config.tcp_port,
                ));
                task::spawn(subscribe(
                    addr,
                    handler.clone(),
                    self.topic.to_owned(),
                    self.nsq_channel.to_owned(),
                    serde_json::to_string(&self.identify_config)?,
                ));
                self.cached_address.insert(nsqd_config);
            } else {
                dbg!("address exists,{:?}", &nsqd_config);
            }
        }
        Ok(())
    }
}

/// a simple entry of reader to subscribe a specific nsq topic and channel
pub async fn subscribe(
    address: Address,
    handler: Arc<Box<dyn MsgHandler + Send + Sync>>,
    nsq_topic: String,
    nsq_channel: String,
    identify_config: String,
) -> io::Result<Reader> {
    let mut reader = create_reader(address, identify_config).await?;
    reader.connect().await?;
    loop {
        reader.sub(nsq_topic.as_str(), nsq_channel.as_str()).await?;
        'a: loop {
            dbg!("msg_receiver");
            let msg_received = reader.msg_receiver.recv().await;
            if let Some(Some(msg)) = msg_received {
                if let Some(n_msg) = handler.handler(msg).await {
                    reader.msg_sender.send(Some(n_msg)).await;
                    break 'a;
                }
            } else if let Some(None) = msg_received {
                break 'a;
            }
        }
        dbg!("reconnect tcpstream");
        reader.reconnect().await?;
    }
    Ok(reader)
}

/// a simple entry for creating reader
pub async fn create_reader(address: Address, identify_config: String) -> io::Result<Reader> {
    if let Address::ReaderdAddr(m_addr) = &address {
        let stream = TcpStream::connect(m_addr).await?;
        let stream = Arc::new(stream);
        let max_inflight = 50;
        let (msg_sender, msg_receiver) = channel(100);
        let msg_sender = Arc::new(msg_sender);
        let data_bufer = Arc::new(Mutex::new(Vec::new()));
        let reader = Reader {
            stream,
            max_inflight,
            connected: Arc::new(Mutex::new(false)),
            msg_sender,
            msg_receiver,
            data_bufer,
            address,
            identify_config,
        };
        return Ok(reader);
    }
    Err(io::Error::new(io::ErrorKind::Other, "address error"))
}

/// a simple entry for creating writer
pub async fn create_writer(address: Address) -> io::Result<Writer> {
    if let Address::ReaderdAddr(m_addr) = &address {
        let stream = TcpStream::connect(m_addr).await?;
        let stream = Arc::new(stream);
        let writer = Writer { stream, address };
        return Ok(writer);
    }
    Err(io::Error::new(io::ErrorKind::Other, "address error"))?
}
