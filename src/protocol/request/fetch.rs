use bytes::{Buf, Bytes};

use crate::protocol::response::fetch::FetchResponseV16;

use super::HeaderV2;

#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchRequestV16 {
    pub header: HeaderV2,
    max_wait_ms: u32,
    min_bytes: u32,
    max_bytes: u32,
    isolation_level: u8,
    session_id: u32,
    session_epoch: u32,
}

impl FetchRequestV16 {
    // https://kafka.apache.org/protocol.html#The_Messages_Fetch
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let header = HeaderV2::from_bytes(src);

        let max_wait_ms = src.get_u32();
        let min_bytes = src.get_u32();
        let max_bytes = src.get_u32();
        let isolation_level = src.get_u8();
        let session_id = src.get_u32();
        let session_epoch = src.get_u32();
        // TODO
        _ = src.get_u8(); // tag buffer

        Self {
            header,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
        }
    }

    pub fn process(self) -> FetchResponseV16 {
        FetchResponseV16::new(self.header.correlation_id, self.session_id)
    }
}
