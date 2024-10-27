use anyhow::Result;
use bytes::Bytes;

use crate::protocol::response::api_versions::ApiVersionsResponseV3;

use super::HeaderV2;

#[derive(Debug)]
pub struct ApiVersionsRequest {
    header: HeaderV2,
}

impl ApiVersionsRequest {
    // https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    pub fn from_bytes(src: &mut Bytes) -> Result<Self> {
        let header = HeaderV2::from_bytes(src);
        Ok(Self { header })
    }

    pub fn process(self) -> ApiVersionsResponseV3 {
        ApiVersionsResponseV3::new(self.header.correlation_id, self.header.request_api_version)
    }
}
