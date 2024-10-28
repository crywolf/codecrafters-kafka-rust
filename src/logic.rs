use anyhow::{Context, Result};
use bytes::Bytes;

use crate::protocol::{
    request::{
        api_versions::ApiVersionsRequest,
        describe_topic_partitions::DescribeTopicPartitionsRequestV0,
    },
    ApiKey, Response,
};

pub fn process(request_api_key: ApiKey, msg: &mut Bytes) -> Result<Box<dyn Response + Send>> {
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
