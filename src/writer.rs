use async_std::future;
use async_std::io;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::{channel, Arc, Mutex, Receiver, Sender};
use async_std::task;

use crate::data::Address;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub struct Writer {
    pub stream: Arc<TcpStream>,
    pub address: Address,
}

impl Writer {
    pub async fn reconnect(&mut self) -> io::Result<()> {
        if let Address::ReaderdAddr(m_addr) = &self.address {
            let stream = TcpStream::connect(m_addr).await?;
            let stream = Arc::new(stream);
            self.stream = stream;
            self.connect().await?;
        }
        Ok(())
    }

    pub async fn connect(&mut self) -> io::Result<()> {
        let writer = &mut &*self.stream.clone();
        let tmp = writer.write_all("  V2".as_bytes()).await?;
        let mut msg = Vec::new();
        let pt = json!({"client_id": "ssss","hostname": "bbbb","heartbeat_interval": 1000,"feature_negotiation": true});
        let mm = pt.to_string();
        let tmp = mm.as_bytes();
        let tmp_length: u32 = tmp.len() as u32;
        msg.put(&b"IDENTIFY\n"[..]);
        msg.put_u32_be(tmp_length);
        msg.put(tmp);
        let _res = writer.write_all(&msg).await?;
        task::spawn(Writer::read_data(self.stream.clone()));
        Ok(())
    }

    async fn parse_data(all_data: &mut Vec<u8>, stream: Arc<TcpStream>) -> io::Result<()> {
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
                println!("->{} \n", String::from_utf8_lossy(&all_data[..]));
                break 'a;
            }
        }
        Ok(())
    }

    async fn read_data(stream: Arc<TcpStream>) -> io::Result<()> {
        dbg!("loop stream");
        dbg!("start to read");
        let reader = &mut &*stream;
        let mut all_data: Vec<u8> = Vec::new();
        loop {
            let mut tmpbuf = vec![0u8; 1024];
            dbg!("wait for read");
            let n = reader.read(&mut tmpbuf).await?;
            dbg!("read complete", n);
            if n > 0 {
                tmpbuf.split_off(n);
                all_data.append(&mut tmpbuf);
                Writer::parse_data(&mut all_data, stream.clone()).await?;
            } else {
                return Ok(());
            }
        }
        dbg!("read_data finish");
        Ok(())
    }
    /// publish data
    pub async fn publish(&self, topic: &str, data: impl Serialize) -> io::Result<()> {
        let writer = &mut &*self.stream.clone();
        let mut msg = Vec::new();
        let mm = serde_json::to_string(&data).unwrap();
        let tmp = mm.as_bytes();
        let tmp_length: u32 = tmp.len() as u32;
        msg.put(&b"PUB "[..]);
        msg.put(topic.as_bytes());
        msg.put(&b"\n"[..]);
        msg.put_u32_be(tmp_length);
        msg.put(tmp);
        writer.write_all(&msg).await
    }
    /// delay publish data
    pub async fn delay_publish(
        &self,
        topic: &str,
        delay: i32,
        data: impl Serialize,
    ) -> io::Result<()> {
        let writer = &mut &*self.stream.clone();
        let mut msg = Vec::new();
        let mm = serde_json::to_string(&data).unwrap();
        let tmp = mm.as_bytes();
        let tmp_length: u32 = tmp.len() as u32;
        let delay_time = delay.to_string();
        msg.put(&b"PUB "[..]);
        msg.put(topic.as_bytes());
        msg.put(&b" "[..]);
        msg.put(delay_time.as_bytes());
        msg.put(&b"\n"[..]);
        msg.put_u32_be(tmp_length);
        msg.put(tmp);
        writer.write_all(&msg).await
    }
    /// publish multi data
    pub async fn multi_publish(&self, topic: &str, data: Vec<impl Serialize>) -> io::Result<()> {
        let writer = &mut &*self.stream.clone();
        let num_msg = data.len() as u32;

        let mut msg = Vec::new();
        let mut t_msg = Vec::new();
        for t_data in data {
            let mm = serde_json::to_string(&t_data).unwrap();
            let tmp = mm.as_bytes();
            let tmp_length: u32 = tmp.len() as u32;
            t_msg.put_u32_be(tmp_length);
            t_msg.put(tmp);
        }
        let body_size = t_msg.len() as u32;
        msg.put(&b"MPUB "[..]);
        msg.put(topic.as_bytes());
        msg.put(&b"\n"[..]);
        msg.put_u32_be(body_size);
        msg.put_u32_be(num_msg);
        msg.append(&mut t_msg);
        writer.write_all(&msg).await
    }
}
