use crate::data::{Address, IdentifyConfig, Msg};
use async_std::future;
use async_std::io;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::{channel, Arc, Mutex, Receiver, Sender};
use async_std::task;

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// the reader struct
pub struct Reader {
    pub stream: Arc<TcpStream>,
    pub max_inflight: i32,
    pub connected: Arc<Mutex<bool>>,
    pub msg_sender: Arc<Sender<Option<Msg>>>,
    pub msg_receiver: Receiver<Option<Msg>>,
    pub data_bufer: Arc<Mutex<Vec<u8>>>,
    pub address: Address,
    pub identify_config: String,
}

impl Reader {
    /// reconnect of the stream. close the old one and create a new one
    /// deal with the msgs which use the old closed stream
    /// use new stream to deal with the msg
    pub async fn reconnect(&mut self) -> io::Result<()> {
        let mut failed_msg = Vec::new();
        while !self.msg_receiver.is_empty() {
            if let Some(Some(msg)) = self.msg_receiver.recv().await {
                failed_msg.push(msg);
            }
        }
        if let Address::ReaderdAddr(m_addr) = &self.address {
            let stream = TcpStream::connect(m_addr).await?;
            let stream = Arc::new(stream);
            self.stream = stream;
            self.connect().await?;
            for mut msg in failed_msg {
                msg.stream = self.stream.clone();
                msg.req(1000).await?
            }
        }
        Ok(())
    }

    /// connect a stream
    pub async fn connect(&mut self) -> io::Result<()> {
        let writer = &mut &*self.stream.clone();
        let tmp = writer.write_all("  V2".as_bytes()).await?;
        let mut msg = Vec::new();
        // let pt = json!({"client_id": "ssss","hostname": "bbbb","heartbeat_interval": 1000,"feature_negotiation": true});
        let mm = self.identify_config.to_owned();
        let tmp = mm.as_bytes();
        let tmp_length: u32 = tmp.len() as u32;
        msg.put(&b"IDENTIFY\n"[..]);
        msg.put_u32_be(tmp_length);
        msg.put(tmp);
        let _res = writer.write_all(&msg).await?;
        *self.connected.lock().await = true;
        let (raw_sender, raw_receiver) = channel(1000);
        task::spawn(Reader::parse_data(
            raw_receiver,
            self.msg_sender.clone(),
            self.stream.clone(),
            self.connected.clone(),
            self.data_bufer.clone(),
        ));
        task::spawn(Reader::read_data(
            self.stream.clone(),
            self.connected.clone(),
            raw_sender,
        ));
        Ok(())
    }
    /// send sub topic command
    pub async fn sub(&self, nsq_topic: &str, nsq_channel: &str) -> io::Result<()> {
        let writer = &mut &*self.stream.clone();
        let mut cmd = "SUB ".to_string();
        cmd.push_str(nsq_topic);
        cmd.push_str(" ");
        cmd.push_str(nsq_channel);
        cmd.push_str("\n");
        writer.write_all(cmd.as_bytes()).await?;
        writer.write_all("RDY 5\n".as_bytes()).await?;
        Ok(())
    }
    /// function that read data from stream async.
    async fn read_data(
        stream: Arc<TcpStream>,
        connected: Arc<Mutex<bool>>,
        raw_sender: Sender<Vec<u8>>,
    ) -> io::Result<()> {
        dbg!("loop stream");
        dbg!("start to read");
        let reader = &mut &*stream;
        let mut zero = 0;
        loop {
            let mut tmpbuf = vec![0u8; 1024];
            dbg!("wait for read");
            let n = reader.read(&mut tmpbuf).await?;
            dbg!("read complete", n);
            if n > 0 {
                tmpbuf.split_off(n);
                raw_sender.send(tmpbuf).await;
                zero = 0;
            } else {
                zero += 1;
                if zero > 10 {
                    *connected.lock().await = false;
                    dbg!("stream disconnected");
                    return Err(io::Error::new(io::ErrorKind::Other, "stream disconnected"))?;
                }
                dbg!(zero);
            }
        }
        dbg!("read_data finish");
        Ok(())
    }
    /// parse data from the stream
    async fn parse_data(
        raw_receiver: Receiver<Vec<u8>>,
        msg_sender: Arc<Sender<Option<Msg>>>,
        stream: Arc<TcpStream>,
        connected: Arc<Mutex<bool>>,
        data_bufer: Arc<Mutex<Vec<u8>>>,
    ) -> io::Result<()> {
        let mut all_data = data_bufer.lock().await;
        loop {
            if let Some(mut raw_data) = raw_receiver.recv().await {
                all_data.append(&mut raw_data);
            }
            'a: loop {
                if all_data.len() > 8 {
                    let msg_type = BigEndian::read_u32(&all_data[4..8]) as usize;
                    let msg_length = BigEndian::read_u32(&all_data[..4]) as usize;
                    dbg!(msg_length, msg_type);
                    if all_data.len() < 4 + msg_length {
                        dbg!(String::from_utf8_lossy(&all_data[..]));
                        dbg!(all_data.len());
                        break 'a;
                    }
                    let msg: Vec<u8> = all_data.drain(..4 + msg_length).collect();
                    match msg_type {
                        2 => {
                            let ts = BigEndian::read_i64(&msg[8..16]);
                            let attempts = BigEndian::read_u16(&msg[16..18]);
                            let id = msg[18..34].to_vec();
                            let body = msg[34..].to_vec();
                            let msg = Msg {
                                ts,
                                attempts,
                                id,
                                body,
                                stream: stream.clone(),
                            };
                            msg_sender.send(Some(msg)).await;
                        }
                        0 => {
                            println!("0-msg-len-{:?}, buf-len-{:?}", msg.len(), all_data.len());
                            println!(
                                "0- response - msg ->{} {}\n",
                                msg_length,
                                String::from_utf8_lossy(&msg[..])
                            );
                            let writer = &mut &*stream.clone();
                            writer.write_all("NOP\n".as_bytes()).await?;
                            println!("0--all ->{}\n", String::from_utf8_lossy(&all_data[..]));
                        }
                        1 => {
                            println!(
                                "1-msg ->{} {}\n",
                                msg_length,
                                String::from_utf8_lossy(&msg[..])
                            );
                            println!("1--all ->{}\n", String::from_utf8_lossy(&all_data[..]))
                        }
                        _ => {
                            println!("None--all ->{}\n", String::from_utf8_lossy(&all_data[..]));
                        }
                    }
                } else {
                    if !*connected.lock().await {
                        dbg!("stream parser error");
                        msg_sender.send(None).await;
                        return Err(io::Error::new(io::ErrorKind::Other, "stream parser error"))?;
                    }
                    println!("->{} \n", String::from_utf8_lossy(&all_data[..]));
                    break 'a;
                }
            }
        }
        Ok(())
    }
}
