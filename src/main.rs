mod logic;
mod protocol;

use logic::UnsupportedApiKeyError;
use protocol::{request, ResponseMessage};

use anyhow::{Context, Result};
use bytes::BytesMut;
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

        let mut msg = msg.freeze();

        let header = request::HeaderV2::from_bytes(&mut msg.clone());
        let request_api_key = header.request_api_key;

        let resp = match logic::process(request_api_key, &mut msg).context("process request") {
            Ok(resp) => resp,
            Err(err) => match err.downcast_ref::<UnsupportedApiKeyError>() {
                Some(e) => {
                    // I could create a specific error response here but I just print the error
                    // and terminate the connection because I don't know what respose Kafka is supposed to return
                    {
                        eprintln!("Error: {e}");
                        Err(err)
                    }
                }
                None => Err(err),
            }?,
        };

        let resp_message = ResponseMessage::from_bytes(resp.as_bytes());

        stream
            .write_all(resp_message.as_bytes())
            .await
            .context("write response")?
    }

    Ok(())
}
