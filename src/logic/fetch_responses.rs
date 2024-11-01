use anyhow::{Context, Result};

use crate::protocol::{
    record_batch::RecordBatches,
    request::fetch::FetchRequestV16,
    response::fetch::{FetchResponseV16, TopicPartition, TopicResponse},
    ErrorCode,
};

/// https://kafka.apache.org/documentation/#log
const CLUSTER_METADATA_LOG_FILE: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn process(req: FetchRequestV16) -> Result<FetchResponseV16> {
    if req.topics.is_empty() {
        let responses = vec![];
        return Ok(FetchResponseV16::new(
            req.header.correlation_id,
            req.session_id,
            responses,
        ));
    };

    let responses = if let Some(topic_request) = req.topics.first() {
        // default - topic does not exist
        let mut error_code = ErrorCode::UnknownTopicId;

        let record_batches = RecordBatches::from_file(CLUSTER_METADATA_LOG_FILE)
            .context("read record batches from file")?;

        let topic_id = topic_request.topic_id.clone();

        let records = if let Some(record_batch) = record_batches.batch_for_topic(&topic_id) {
            error_code = ErrorCode::None;
            record_batch.records.to_vec()
        } else {
            Vec::new()
        };

        let partition = TopicPartition {
            partition_index: 0,
            error_code,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: Vec::new(),
            preferred_read_replica: 0,
            records,
        };
        let partitions = vec![partition];
        let topic_response = TopicResponse::new(topic_id, partitions);
        vec![topic_response]
    } else {
        vec![]
    };

    Ok(FetchResponseV16::new(
        req.header.correlation_id,
        req.session_id,
        responses,
    ))
}
