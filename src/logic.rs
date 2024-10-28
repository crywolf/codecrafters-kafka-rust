use anyhow::{bail, Context, Result};
use bytes::Bytes;
use thiserror::Error;

use crate::protocol::{
    request::{
        api_versions::ApiVersionsRequest,
        describe_topic_partitions::DescribeTopicPartitionsRequestV0,
    },
    ApiKey, Response,
};

pub fn process(request_api_key: i16, msg: &mut Bytes) -> Result<Box<dyn Response + Send>> {
    // https://kafka.apache.org/protocol.html#protocol_api_keys
    let request_api_key = match ApiKey::try_from(request_api_key) {
        Ok(key) => key,
        Err(_) => {
            bail!(UnsupportedApiKeyError(request_api_key));
        }
    };

    let response: Box<dyn Response + Send> = match request_api_key {
        ApiKey::ApiVersions => {
            let req =
                ApiVersionsRequest::from_bytes(msg).context("deserialize ApiVersionsRequest")?;
            let resp = req.process();
            Box::new(resp)
        }
        ApiKey::DescribeTopicPartitions => {
            let req = DescribeTopicPartitionsRequestV0::from_bytes(msg);
            let resp = req.process().context("process request")?;
            Box::new(resp)
        }
    };

    Ok(response)
}
#[derive(Debug, Error)]
#[error("Unsupported api key `{0}`")]
pub struct UnsupportedApiKeyError(i16);
