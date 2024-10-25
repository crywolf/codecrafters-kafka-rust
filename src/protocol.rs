pub mod response;

use num_enum::IntoPrimitive;

// https://kafka.apache.org/protocol.html#protocol_api_keys
#[derive(Clone, Copy, IntoPrimitive)]
#[repr(i16)]
pub enum ApiKey {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

// https://kafka.apache.org/protocol.html#protocol_error_codes
#[derive(Clone, Copy, IntoPrimitive)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
}
