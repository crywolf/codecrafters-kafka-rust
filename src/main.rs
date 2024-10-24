use std::{
    io::{Read, Write},
    net::TcpListener,
};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, BytesMut};

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                eprintln!("accepted new connection");

                let mut buf = [0u8; 4];
                stream.read_exact(&mut buf).context("read message size")?;

                let msg_size = i32::from_be_bytes(buf) as usize;
                let mut msg = BytesMut::with_capacity(msg_size);
                msg.resize(msg_size, 0);
                stream.read_exact(&mut msg).context("read message data")?;

                let request_api_key = msg.get_i16();
                let request_api_version = msg.get_i16();
                let correlation_id = msg.get_i32();
                eprintln!("msg_size: {msg_size}, request_api_key: {request_api_key}, request_api_version: {request_api_version}, correlation_id: {correlation_id}");

                let msg_size = 8;
                let mut resp = BytesMut::new();
                resp.put_i32(msg_size);
                resp.put_i32(correlation_id);
                stream.write_all(&resp)?
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }

    Ok(())
}
