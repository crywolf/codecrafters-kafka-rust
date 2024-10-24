use std::{io::Write, net::TcpListener};

use anyhow::Result;
use bytes::{BufMut, BytesMut};

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                eprintln!("accepted new connection");

                let correlation_id = 7;
                let msg_length = 8;
                let mut buf = BytesMut::new();
                buf.put_i32(msg_length);
                buf.put_i32(correlation_id);
                stream.write_all(&buf)?
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }

    Ok(())
}
