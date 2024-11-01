use std::path::Path;

use anyhow::{Context, Result};
use bytes::{Buf, Bytes, BytesMut};

use crate::protocol::types::{CompactArray, CompactString, Uuid, VarInt};

use super::types::{self, CompactNullableBytes, NullableBytes};

pub struct RecordBatches {
    batches: Vec<RecordBatch>,
}

impl RecordBatches {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let file_bytes = std::fs::read(path).context("read file")?;
        let mut data = BytesMut::with_capacity(file_bytes.len());
        data.extend_from_slice(&file_bytes);
        let mut data = data.freeze();

        let mut batches = Vec::new();
        while data.remaining() > 0 {
            let record_batch = RecordBatch::from_bytes(&mut data);
            batches.push(record_batch);
        }
        Ok(Self { batches })
    }

    #[allow(dead_code)]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn batch_for_topic(&self, topic_id: &str) -> Option<&RecordBatch> {
        self.batches.iter().find(|&b| {
            let topic_found = b.records.iter().any(
                |r| matches!(&r.value, RecordValue::Topic(topic) if topic.topic_id == topic_id),
            );
            topic_found
        })
    }
}

/// A record batch is the format that Kafka uses to store multiple records.
#[derive(Debug)]
#[allow(dead_code)]
pub struct RecordBatch {
    /// Base Offset is a 8-byte big-endian integer indicating the offset of the first record in this batch.
    base_offset: i64,
    /// Batch Length is a 4-byte big-endian integer indicating the length of the entire record batch in bytes.
    /// This value excludes the Base Offset (8 bytes) and the Batch Length (4 bytes) itself, but includes all other bytes in the record batch.
    batch_length: i32,
    /// Partition Leader Epoch is a 4-byte big-endian integer indicating the epoch of the leader for this partition.
    /// It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes.
    /// This value is used to detect out of order writes.
    partition_leader_epoch: i32,
    /// Magic Byte is a 1-byte big-endian integer indicating the version of the record batch format.
    /// This value is used to evolve the record batch format in a backward-compatible way.
    magic: i8, // (current magic value is 2)
    /// CRC is a 4-byte big-endian integer indicating the CRC32-C checksum of the record batch.
    /// The CRC is computed over the data following the CRC field to the end of the record batch. The CRC32-C (Castagnoli) polynomial is used for the computation.
    crc: u32,
    /// Attributes is a bitmask of the following flags:
    /// bit 0~2:
    /// 0: no compression
    /// 1: gzip
    /// 2: snappy
    /// 3: lz4
    /// 4: zstd
    /// bit 3: timestampType
    /// bit 4: isTransactional (0 means not transactional)
    /// bit 5: isControlBatch (0 means not a control batch)
    /// bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
    /// bit 7~15: unused
    attributes: i16,
    /// Last Offset Delta is a 4-byte big-endian integer indicating the difference between the last offset of this record batch and the base offset.
    last_offset_delta: i32,
    /// Base Timestamp is a 8-byte big-endian integer indicating the timestamp of the first record in this batch.
    base_timestamp: i64,
    /// Max Timestamp is a 8-byte big-endian integer indicating the maximum timestamp of the records in this batch.
    max_timestamp: i64,
    /// Producer ID is a 8-byte big-endian integer indicating the ID of the producer that produced the records in this batch.
    producer_id: i64, // In this case, the value is 0xffffffffffffffff, which is -1 in decimal. This is a special value that indicates that the producer ID is not set.
    /// Producer Epoch is a 2-byte big-endian integer indicating the epoch of the producer that produced the records in this batch.
    producer_epoch: i16, // In this case, the value is 0xffff, which is -1 in decimal. This is a special value that indicates that the producer epoch is not applicable.
    /// Base Sequence is a 4-byte big-endian integer indicating the sequence number of the first record in a batch.
    /// It is used to ensure the correct ordering and deduplication of messages produced by a Kafka producer.
    base_sequence: i32,

    pub records: Vec<Record>, // NULLABLE_BYTES
}

impl RecordBatch {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let base_offset = src.get_i64();
        let batch_length = src.get_i32();
        let partition_leader_epoch = src.get_i32();
        let magic = src.get_i8();
        let crc = src.get_u32();
        let attributes = src.get_i16();
        let last_offset_delta = src.get_i32();
        let base_timestamp = src.get_i64();
        let max_timestamp = src.get_i64();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let base_sequence = src.get_i32();
        let records = NullableBytes::deserialize::<Record, RecordBatch>(src);

        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }
}

impl types::Deserialize<Record> for RecordBatch {
    fn deserialize(src: &mut Bytes) -> Record {
        Record::from_bytes(src)
    }
}

/// A record is the format that Kafka uses to store a single record.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Record {
    /// Length is a signed variable size integer indicating the length of the record, the length is calculated from the attributes field to the end of the record.
    length: i64,
    /// Attributes is a 1-byte big-endian integer indicating the attributes of the record. Currently, this field is unused in the protocol.
    attributes: i8,
    /// Timestamp Delta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.
    timestamp_delta: i64,
    /// Offset Delta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.
    offset_delta: i64,
    /// Key is a byte array indicating the key of the record.
    key: Vec<u8>,
    /// Value Length is a signed variable size integer indicating the length of the value of the record.
    value_length: i64,
    /// Value is a byte array indicating the value of the record.
    pub value: RecordValue,
    headers: Vec<Header>,
}

impl Record {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let length = VarInt::deserialize(src);
        let attributes = src.get_i8();
        let timestamp_delta = VarInt::deserialize(src);
        let offset_delta = VarInt::deserialize(src);
        let key = CompactNullableBytes::deserialize(src);
        let value_length = VarInt::deserialize(src);
        let value = RecordValue::from_bytes(src);
        let headers = CompactArray::deserialize::<Header, Record>(src);

        Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_length,
            value,
            headers,
        }
    }
}

impl types::Deserialize<Header> for Record {
    fn deserialize(_src: &mut Bytes) -> Header {
        // we assume that headers array is empty, so this would not be called
        Header
    }
}

impl types::Serialize for Record {
    fn serialize(&mut self) -> Bytes {
        // TODO
        Bytes::new()
    }
}

#[derive(Debug, Clone, Copy)]
struct Header;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

#[derive(Debug, Clone)]
pub struct TopicValue {
    pub topic_name: String,
    pub topic_id: String, // UUID
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PartitionValue {
    pub partition_id: u32,
    pub topic_id: String,
    pub replicas: Vec<u32>,
    /// The in-sync replicas of this partition
    pub in_sync_replicas: Vec<u32>,
    /// The replicas that we are in the process of removing
    pub removing_replicas: Vec<u32>,
    pub adding_replicas: Vec<u32>,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub partition_epoch: u32,
    pub directories: Vec<String>,
}

impl types::Deserialize<u32> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> u32 {
        src.get_u32()
    }
}

impl types::Deserialize<String> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> String {
        Uuid::deserialize(src)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FeatureLevelValue {
    name: String,
    level: u16,
}

impl RecordValue {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        // Frame Version is indicating the version of the format of the record.
        let frame_version = src.get_u8();
        assert_eq!(frame_version, 1);

        let record_type = src.get_u8();
        match record_type {
            2 => {
                // Topic Record Value
                let version = src.get_u8();
                assert_eq!(version, 0);
                let topic_name = CompactString::deserialize(src);
                let topic_id = Uuid::deserialize(src);

                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);
                RecordValue::Topic(TopicValue {
                    topic_name,
                    topic_id,
                })
            }
            3 => {
                // Partition Record Value
                let version = src.get_u8();
                assert_eq!(version, 1);
                let partition_id = src.get_u32();
                let topic_id = Uuid::deserialize(src);

                let replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let in_sync_replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let removing_replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let adding_replicas = CompactArray::deserialize::<_, PartitionValue>(src);

                let leader_id = src.get_u32();
                let leader_epoch = src.get_u32();
                let partition_epoch = src.get_u32();

                let directories = CompactArray::deserialize::<String, PartitionValue>(src);

                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);

                RecordValue::Partition(PartitionValue {
                    partition_id,
                    topic_id,
                    replicas,
                    in_sync_replicas,
                    removing_replicas,
                    adding_replicas,
                    leader_id,
                    leader_epoch,
                    partition_epoch,
                    directories,
                })
            }

            12 => {
                // Feature Level Record Value
                let version = src.get_u8();
                assert_eq!(version, 0);
                let name = CompactString::deserialize(src);
                let level = src.get_u16();
                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);
                RecordValue::FeatureLevel(FeatureLevelValue { name, level })
            }

            _ => unimplemented!(),
        }
    }
}
