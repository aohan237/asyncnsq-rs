use async_std::io;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::Arc;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::cmp::{Eq, PartialEq};
use std::hash::{Hash, Hasher};

/// nsq client config from server response
#[derive(Debug, Deserialize, Serialize)]
pub struct NsqdClientConfig {
    pub max_rdy_count: u64,
    pub version: String,
    pub max_msg_timeout: u64,
    pub msg_timeout: u64,
    pub tls_v1: bool,
    pub deflate: bool,
    pub deflate_level: u8,
    pub max_deflate_level: u8,
    pub snappy: bool,
    pub sample_rate: u64,
    pub auth_required: bool,
    pub output_buffer_size: u64,
    pub output_buffer_timeout: u64,
}

/// identify config for client
#[derive(Debug, Deserialize, Serialize)]
pub struct IdentifyConfig {
    pub client_id: String,
    pub heartbeat_interval: u64,
    pub output_buffer_timeout: u64,
    pub msg_timeout: u64,
    pub user_agent: String,
}

/// client nsqd address config
#[derive(Debug, Deserialize, Serialize)]
pub struct ClientNsqdAddressConfig {
    pub hostname: String,
    pub tcp_port: u32,
}

/// client httpd address config
#[derive(Debug, Deserialize, Serialize)]
pub struct ClientHttpdAddressConfig {
    pub address: String,
    pub topic: String,
    pub nsq_channel: String,
}

impl IdentifyConfig {
    pub fn new(
        client_id: String,
        heartbeat_interval: u64,
        output_buffer_timeout: u64,
        msg_timeout: u64,
        user_agent: String,
    ) -> IdentifyConfig {
        IdentifyConfig {
            client_id,
            heartbeat_interval,
            output_buffer_timeout,
            msg_timeout,
            user_agent,
        }
    }
}

/// toml  config for client to initial connect to lookupd server
#[derive(Debug, Deserialize, Serialize)]
pub struct NsqHttpdInitConfig {
    pub identify: IdentifyConfig,
    pub httpd_adress: ClientHttpdAddressConfig,
}

/// toml config for client to initial connect nsqd server
#[derive(Debug, Deserialize, Serialize)]
pub struct NsqdInitConfig {
    pub identify: IdentifyConfig,
    pub nsqd_adress: ClientNsqdAddressConfig,
}

/// the msg struct from server
#[derive(Debug)]
pub struct Msg {
    pub ts: i64,
    pub attempts: u16,
    pub id: Vec<u8>,
    pub body: Vec<u8>,
    pub stream: Arc<TcpStream>,
}
impl Msg {
    /// finish the msg
    pub async fn finish(&self) -> io::Result<()> {
        let mut msg = Vec::new();
        msg.put(&b"FIN "[..]);
        msg.put(&self.id);
        msg.put(&b"\n"[..]);
        let writer = &mut &*self.stream;
        writer.write_all(&msg).await?;
        writer.write_all("RDY 2\n".as_bytes()).await
    }
    /// requeue the msg
    pub async fn req(&self, timeout: i32) -> io::Result<()> {
        let mut msg = Vec::new();
        let timeout = timeout.to_string();
        msg.put(&b"REQ "[..]);
        msg.put(&self.id);
        msg.put(&b" "[..]);
        msg.put(timeout.as_bytes());
        msg.put(&b"\n"[..]);
        let writer = &mut &*self.stream;
        writer.write_all(&msg).await?;
        writer.write_all("RDY 2\n".as_bytes()).await
    }
    /// touch the msg
    pub async fn touch(&self) -> io::Result<()> {
        let mut msg = Vec::new();
        msg.put(&b"TOUCH "[..]);
        msg.put(&self.id);
        msg.put(&b"\n"[..]);
        let writer = &mut &*self.stream;
        writer.write_all(&msg).await?;
        writer.write_all("RDY 2\n".as_bytes()).await
    }
}

/// the address enum
pub enum Address {
    ReaderdAddresses(Vec<String>),
    ReaderdAddr(String),
    HttpdAddress(String),
}

/// the nsqd config
#[derive(Serialize, Deserialize, Debug)]
pub struct NsqdConfig {
    pub broadcast_address: String,
    pub hostname: String,
    pub remote_address: String,
    pub tcp_port: u32,
    pub http_port: u32,
    pub version: String,
}

/// impl PartialEq for nsqdconfig to decide the duplicate
impl PartialEq for NsqdConfig {
    fn eq(&self, other: &Self) -> bool {
        if (self.broadcast_address == other.broadcast_address)
            && (self.tcp_port == other.tcp_port)
            && (self.http_port == other.http_port)
            && (self.version == other.version)
        {
            return true;
        }
        false
    }
}

impl Eq for NsqdConfig {}

impl Hash for NsqdConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.broadcast_address.hash(state);
        self.tcp_port.hash(state);
        self.http_port.hash(state);
        self.version.hash(state);
    }
}

/// nsq lookupd config
#[derive(Serialize, Deserialize)]
pub struct NsqLookupdConfig {
    pub channels: Vec<String>,
    pub producers: Vec<NsqdConfig>,
}
