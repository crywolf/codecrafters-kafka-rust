use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    loop {
        let (stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            eprintln!("accepted new connection");

            handle_connection(stream).await.unwrap_or_else(|e| {
                eprintln!("Error: {:?}", e);
            })
        });
    }
}

pub async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // peek into stream and try read msg size to check if connection is still open
    while stream.peek(&mut [0; 4]).await.is_ok() {
        // connection is not closed

        let mut buf = [0u8; 4];
        stream
            .read_exact(&mut buf)
            .await
            .context("read message size")?;

        let msg_size = i32::from_be_bytes(buf) as usize;
        let mut msg = BytesMut::with_capacity(msg_size);
        msg.resize(msg_size, 0);
        stream
            .read_exact(&mut msg)
            .await
            .context("read message data")?;

        let mut error_code: i16 = 0;

        // https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
        let request_api_key = msg.get_i16(); // https://kafka.apache.org/protocol.html#protocol_api_keys
        let request_api_version = msg.get_i16();
        let correlation_id = msg.get_i32();
        /*
        + client_id: A string identifying the client that sent the request.
        + tagged_fields: Optional tagged fields
            This can be ignored for now, they're optional tagged fields used to introduce additional features over time
                (https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields).
            The value for this will always be a null byte in this challenge (i.e. no tagged fields are present)
        */

        if request_api_key == 18 {
            match request_api_version {
                0..=4 => {}
                _ => error_code = 35, // UNSUPPORTED_VERSION}
            }
        }

        /////////////////
        // The APIVersions response uses the "v0" header format, while all other responses use the "v1" header format. (????)
        // The response header format (v0) is 4 bytes long, and contains exactly one field: correlation_id
        // The response header format (v1) contains an additional tag_buffer field.

        let mut api_keys = BytesMut::new();
        let num_api_keys = 1 + 1; // COMPACT_ARRAY: N+1, because null array is represented as 0, empty array (actual length of 0) is represented as 1
        api_keys.put_i16(18); // api_key
        api_keys.put_i16(0); // min_version
        api_keys.put_i16(4); // max_version

        let mut resp = BytesMut::new();
        let msg_size = 0; // will be counted later
        resp.put_i32(msg_size);

        resp.put_i32(correlation_id); // HEADER 4 bytes
                                      // BODY - ApiVersions Response (Version: 3)
        resp.put_i16(error_code); // 2 bytes

        if request_api_key == 18 {
            resp.put_u8(num_api_keys);
            resp.put(api_keys);
        }
        resp.put_u8(0); // _tagged_fields
        resp.put_i32(0); // throttle_time_ms (4 bytes)
        resp.put_u8(0); // _tagged_fields

        let resp_size = resp.len() as i32;

        let msg_size = resp
            .first_chunk_mut::<4>()
            .expect("message size element is present in response header");
        *msg_size = (resp_size - 4).to_be_bytes();

        stream.write_all(&resp).await.context("write response")?
    }

    Ok(())
}
