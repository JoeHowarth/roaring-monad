use bytes::Bytes;

use crate::domain::types::{BlockLogHeader, Log, LogDirectoryBucket, Topic32};
use crate::error::{Error, Result};

pub fn validate_log(log: &Log) -> bool {
    log.topics.len() <= 4
}

pub fn encode_log(log: &Log) -> Bytes {
    let topic_count = log.topics.len() as u8;
    let mut out = Vec::with_capacity(80 + topic_count as usize * 32 + log.data.len());
    out.extend_from_slice(&log.address);
    out.push(topic_count);
    for topic in &log.topics {
        out.extend_from_slice(topic);
    }
    out.extend_from_slice(&(log.data.len() as u32).to_be_bytes());
    out.extend_from_slice(&log.data);
    out.extend_from_slice(&log.block_num.to_be_bytes());
    out.extend_from_slice(&log.tx_idx.to_be_bytes());
    out.extend_from_slice(&log.log_idx.to_be_bytes());
    out.extend_from_slice(&log.block_hash);
    Bytes::from(out)
}

pub fn decode_log(bytes: &[u8]) -> Result<Log> {
    if bytes.len() < 20 + 1 + 4 + 8 + 4 + 4 + 32 {
        return Err(Error::Decode("log too short"));
    }

    let mut pos = 0usize;
    let mut address = [0u8; 20];
    address.copy_from_slice(&bytes[pos..pos + 20]);
    pos += 20;

    let topic_count = bytes[pos] as usize;
    pos += 1;
    if topic_count > 4 {
        return Err(Error::Decode("topic count exceeds 4"));
    }

    let topic_bytes = topic_count * 32;
    if bytes.len() < pos + topic_bytes + 4 + 8 + 4 + 4 + 32 {
        return Err(Error::Decode("log missing topic bytes"));
    }

    let mut topics = Vec::<Topic32>::with_capacity(topic_count);
    for _ in 0..topic_count {
        let mut topic = [0u8; 32];
        topic.copy_from_slice(&bytes[pos..pos + 32]);
        pos += 32;
        topics.push(topic);
    }

    let mut data_len = [0u8; 4];
    data_len.copy_from_slice(&bytes[pos..pos + 4]);
    pos += 4;
    let data_len = u32::from_be_bytes(data_len) as usize;

    if bytes.len() < pos + data_len + 8 + 4 + 4 + 32 {
        return Err(Error::Decode("log missing data/body bytes"));
    }

    let data = bytes[pos..pos + data_len].to_vec();
    pos += data_len;

    let mut block_num = [0u8; 8];
    block_num.copy_from_slice(&bytes[pos..pos + 8]);
    pos += 8;

    let mut tx_idx = [0u8; 4];
    tx_idx.copy_from_slice(&bytes[pos..pos + 4]);
    pos += 4;

    let mut log_idx = [0u8; 4];
    log_idx.copy_from_slice(&bytes[pos..pos + 4]);
    pos += 4;

    let mut block_hash = [0u8; 32];
    block_hash.copy_from_slice(&bytes[pos..pos + 32]);

    Ok(Log {
        address,
        topics,
        data,
        block_num: u64::from_be_bytes(block_num),
        tx_idx: u32::from_be_bytes(tx_idx),
        log_idx: u32::from_be_bytes(log_idx),
        block_hash,
    })
}

pub fn encode_log_directory_bucket(bucket: &LogDirectoryBucket) -> Bytes {
    assert!(u32::try_from(bucket.first_log_ids.len()).is_ok());
    let mut out = Vec::with_capacity(1 + 8 + 4 + bucket.first_log_ids.len() * 8);
    out.push(1);
    out.extend_from_slice(&bucket.start_block.to_be_bytes());
    out.extend_from_slice(&(bucket.first_log_ids.len() as u32).to_be_bytes());
    for first_log_id in &bucket.first_log_ids {
        out.extend_from_slice(&first_log_id.to_be_bytes());
    }
    Bytes::from(out)
}

pub fn decode_log_directory_bucket(bytes: &[u8]) -> Result<LogDirectoryBucket> {
    if bytes.len() < 1 + 8 + 4 + 8 {
        return Err(Error::Decode("log directory bucket too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported log directory bucket version"));
    }
    let start_block = u64::from_be_bytes(
        bytes[1..9]
            .try_into()
            .map_err(|_| Error::Decode("log directory bucket start_block"))?,
    );
    let count = u32::from_be_bytes(
        bytes[9..13]
            .try_into()
            .map_err(|_| Error::Decode("log directory bucket count"))?,
    ) as usize;
    if count < 2 {
        return Err(Error::Decode("log directory bucket missing sentinel"));
    }
    let expected_len = 1 + 8 + 4 + count * 8;
    if bytes.len() != expected_len {
        return Err(Error::Decode("invalid log directory bucket length"));
    }
    let mut first_log_ids = Vec::with_capacity(count);
    let mut pos = 13usize;
    for _ in 0..count {
        first_log_ids.push(u64::from_be_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .map_err(|_| Error::Decode("log directory bucket first_log_id"))?,
        ));
        pos += 8;
    }
    Ok(LogDirectoryBucket {
        start_block,
        first_log_ids,
    })
}

pub fn encode_block_log_header(header: &BlockLogHeader) -> Bytes {
    assert!(u32::try_from(header.offsets.len()).is_ok());
    let mut out = Vec::with_capacity(1 + 4 + header.offsets.len() * 4);
    out.push(1);
    out.extend_from_slice(&(header.offsets.len() as u32).to_be_bytes());
    for offset in &header.offsets {
        out.extend_from_slice(&offset.to_be_bytes());
    }
    Bytes::from(out)
}

pub fn decode_block_log_header(bytes: &[u8]) -> Result<BlockLogHeader> {
    if bytes.len() < 1 + 4 + 4 {
        return Err(Error::Decode("block log header too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported block log header version"));
    }
    let count = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .map_err(|_| Error::Decode("block log header count"))?,
    ) as usize;
    if count < 2 {
        return Err(Error::Decode("block log header missing sentinel"));
    }
    let expected_len = 1 + 4 + count * 4;
    if bytes.len() != expected_len {
        return Err(Error::Decode("invalid block log header length"));
    }
    let mut offsets = Vec::with_capacity(count);
    let mut pos = 5usize;
    for _ in 0..count {
        offsets.push(u32::from_be_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Decode("block log header offset"))?,
        ));
        pos += 4;
    }
    Ok(BlockLogHeader { offsets })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_log() {
        let log = Log {
            address: [7u8; 20],
            topics: vec![[1u8; 32], [2u8; 32]],
            data: vec![9, 8, 7],
            block_num: 12,
            tx_idx: 3,
            log_idx: 2,
            block_hash: [5u8; 32],
        };
        let enc = encode_log(&log);
        let dec = decode_log(&enc).expect("decode");
        assert_eq!(dec.address, log.address);
        assert_eq!(dec.topics, log.topics);
        assert_eq!(dec.data, log.data);
        assert_eq!(dec.block_num, log.block_num);
        assert_eq!(dec.tx_idx, log.tx_idx);
        assert_eq!(dec.log_idx, log.log_idx);
        assert_eq!(dec.block_hash, log.block_hash);
    }

    #[test]
    fn roundtrip_log_directory_bucket() {
        let bucket = LogDirectoryBucket {
            start_block: 5001,
            first_log_ids: vec![120_000_000, 120_000_003, 120_000_003, 120_000_008],
        };
        let enc = encode_log_directory_bucket(&bucket);
        let dec = decode_log_directory_bucket(&enc).expect("decode bucket");
        assert_eq!(dec, bucket);
    }

    #[test]
    fn roundtrip_block_log_header() {
        let header = BlockLogHeader {
            offsets: vec![0, 41, 97, 124],
        };
        let enc = encode_block_log_header(&header);
        let dec = decode_block_log_header(&enc).expect("decode block log header");
        assert_eq!(dec, header);
    }

    #[test]
    fn roundtrip_large_log_directory_bucket() {
        let count = (u16::MAX as usize) + 2;
        let mut first_log_ids = Vec::with_capacity(count);
        for i in 0..count {
            first_log_ids.push(i as u64);
        }
        let bucket = LogDirectoryBucket {
            start_block: 123,
            first_log_ids,
        };

        let enc = encode_log_directory_bucket(&bucket);
        let dec = decode_log_directory_bucket(&enc).expect("decode large bucket");
        assert_eq!(dec, bucket);
    }

    #[test]
    fn roundtrip_large_block_log_header() {
        let count = (u16::MAX as usize) + 2;
        let mut offsets = Vec::with_capacity(count);
        for i in 0..count {
            offsets.push(i as u32);
        }
        let header = BlockLogHeader { offsets };

        let enc = encode_block_log_header(&header);
        let dec = decode_block_log_header(&enc).expect("decode large block log header");
        assert_eq!(dec, header);
    }
}
