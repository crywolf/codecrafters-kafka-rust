mod protocol;

use protocol::response::ApiVersionsResponseV3;
use protocol::{ApiKey, ResponseMessage};

use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use tokio::net::TcpListener;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
    // peek into the stream and try to read msg size to check if connection is still open
    while stream.peek(&mut [0; 4]).await.is_ok() {
        // connection is not closed

        let mut msg_size_buf = [0u8; 4];
        stream
            .read_exact(&mut msg_size_buf)
            .await
            .context("read message size")?;

        let msg_size = i32::from_be_bytes(msg_size_buf) as usize;
        let mut msg = BytesMut::with_capacity(msg_size);
        msg.resize(msg_size, 0);
        stream
            .read_exact(&mut msg)
            .await
            .context("read message data")?;

        // https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
        // Request Header v2
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

        if request_api_key != ApiKey::ApiVersions.into() {
            eprintln!("Unsupported api key `{}`", request_api_key);
            // terminate the connection because I don't know what respose Kafka is supposed to return
            break;
        }

        let mut resp = ApiVersionsResponseV3::new(correlation_id, request_api_version);
        let resp_message = ResponseMessage::from_bytes(resp.as_bytes());

        stream
            .write_all(resp_message.as_bytes())
            .await
            .context("write response")?
    }

    Ok(())
}
