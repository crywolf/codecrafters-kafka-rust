use anyhow::{Context, Result};

use crate::protocol::{
    record_batch::RecordBatches,
    request::fetch::FetchRequestV16,
    response::fetch::{BatchBytes, FetchResponseV16, TopicPartition, TopicResponse},
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
        // TODO iterate through more topic requests?

        // default - topic does not exist
        let mut error_code = ErrorCode::UnknownTopicId;

        let topic_id = topic_request.topic_id.clone();

        let partition_id = topic_request
            .partitions
            .first()
            .expect("partition_id shoud be set in request")
            .partition;

        let record_batches = RecordBatches::from_file(CLUSTER_METADATA_LOG_FILE)
            .context("read record batches from file")?;

        let mut partition_record_batches = Vec::new();
        if let Some(raw_batch) = record_batches.raw_batch_for_topic(&topic_id, partition_id)? {
            error_code = ErrorCode::None;
            let batch_bytes = BatchBytes { bytes: raw_batch };
            partition_record_batches.push(batch_bytes);
        }

        let partition = TopicPartition {
            partition_index: 0,
            error_code,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: Vec::new(),
            preferred_read_replica: 0,
            record_batches: partition_record_batches,
        };
        let partitions = vec![partition];
        let topic_response = TopicResponse::new(topic_id, partitions);
        vec![topic_response]
    } else {
        // no topic requested
        vec![]
    };

    Ok(FetchResponseV16::new(
        req.header.correlation_id,
        req.session_id,
        responses,
    ))
}
