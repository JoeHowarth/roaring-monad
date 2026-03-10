use std::collections::HashMap;

use bytes::Bytes;

use crate::domain::types::{Log, LogLocator, Topic32};
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

pub fn encode_log_locator(locator: &LogLocator) -> Bytes {
    assert!(locator.blob_key.len() <= u16::MAX as usize);
    let key_len = locator.blob_key.len() as u16;
    let mut out = Vec::with_capacity(1 + 2 + key_len as usize + 4 + 4);
    out.push(1);
    out.extend_from_slice(&key_len.to_be_bytes());
    out.extend_from_slice(&locator.blob_key[..key_len as usize]);
    out.extend_from_slice(&locator.byte_offset.to_be_bytes());
    out.extend_from_slice(&locator.byte_len.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_log_locator(bytes: &[u8]) -> Result<LogLocator> {
    if bytes.len() < 1 + 2 + 4 + 4 {
        return Err(Error::Decode("log locator too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported log locator version"));
    }
    let key_len = u16::from_be_bytes(
        bytes[1..3]
            .try_into()
            .map_err(|_| Error::Decode("log locator key_len"))?,
    ) as usize;
    let min_len = 1 + 2 + key_len + 4 + 4;
    if bytes.len() != min_len {
        return Err(Error::Decode("invalid log locator length"));
    }
    let blob_key = bytes[3..3 + key_len].to_vec();
    let off_idx = 3 + key_len;
    let byte_offset = u32::from_be_bytes(
        bytes[off_idx..off_idx + 4]
            .try_into()
            .map_err(|_| Error::Decode("log locator offset"))?,
    );
    let byte_len = u32::from_be_bytes(
        bytes[off_idx + 4..off_idx + 8]
            .try_into()
            .map_err(|_| Error::Decode("log locator len"))?,
    );
    Ok(LogLocator {
        blob_key,
        byte_offset,
        byte_len,
    })
}

pub fn encode_log_locator_page(
    page_start_log_id: u64,
    entries: &HashMap<u16, LogLocator>,
) -> Bytes {
    let mut ordered: Vec<(u16, &LogLocator)> = entries
        .iter()
        .map(|(slot, locator)| (*slot, locator))
        .collect();
    ordered.sort_by_key(|(slot, _)| *slot);
    assert!(ordered.len() <= u16::MAX as usize);

    let mut out_len = 1 + 8 + 2;
    for (_, locator) in &ordered {
        assert!(locator.blob_key.len() <= u16::MAX as usize);
        out_len += 2 + 2 + locator.blob_key.len() + 4 + 4;
    }

    let mut out = Vec::with_capacity(out_len);
    out.push(1);
    out.extend_from_slice(&page_start_log_id.to_be_bytes());
    out.extend_from_slice(&(ordered.len() as u16).to_be_bytes());
    for (slot, locator) in ordered {
        out.extend_from_slice(&slot.to_be_bytes());
        out.extend_from_slice(&(locator.blob_key.len() as u16).to_be_bytes());
        out.extend_from_slice(&locator.blob_key);
        out.extend_from_slice(&locator.byte_offset.to_be_bytes());
        out.extend_from_slice(&locator.byte_len.to_be_bytes());
    }
    Bytes::from(out)
}

pub fn decode_log_locator_page(bytes: &[u8]) -> Result<(u64, HashMap<u16, LogLocator>)> {
    if bytes.len() < 1 + 8 + 2 {
        return Err(Error::Decode("log locator page too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported log locator page version"));
    }

    let page_start_log_id = u64::from_be_bytes(
        bytes[1..9]
            .try_into()
            .map_err(|_| Error::Decode("log locator page start"))?,
    );
    let entry_count = u16::from_be_bytes(
        bytes[9..11]
            .try_into()
            .map_err(|_| Error::Decode("log locator page count"))?,
    ) as usize;

    let mut pos = 11usize;
    let mut entries = HashMap::with_capacity(entry_count);
    for _ in 0..entry_count {
        if bytes.len() < pos + 2 + 2 + 4 + 4 {
            return Err(Error::Decode("log locator page truncated"));
        }
        let slot = u16::from_be_bytes(
            bytes[pos..pos + 2]
                .try_into()
                .map_err(|_| Error::Decode("log locator page slot"))?,
        );
        pos += 2;

        let key_len = u16::from_be_bytes(
            bytes[pos..pos + 2]
                .try_into()
                .map_err(|_| Error::Decode("log locator page key_len"))?,
        ) as usize;
        pos += 2;

        if bytes.len() < pos + key_len + 4 + 4 {
            return Err(Error::Decode("log locator page key/value truncated"));
        }
        let blob_key = bytes[pos..pos + key_len].to_vec();
        pos += key_len;

        let byte_offset = u32::from_be_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Decode("log locator page offset"))?,
        );
        pos += 4;
        let byte_len = u32::from_be_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Decode("log locator page len"))?,
        );
        pos += 4;

        if entries
            .insert(
                slot,
                LogLocator {
                    blob_key,
                    byte_offset,
                    byte_len,
                },
            )
            .is_some()
        {
            return Err(Error::Decode("log locator page duplicate slot"));
        }
    }

    if pos != bytes.len() {
        return Err(Error::Decode("invalid log locator page length"));
    }

    Ok((page_start_log_id, entries))
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
    fn roundtrip_log_locator() {
        let locator = LogLocator {
            blob_key: b"log_packs/abc".to_vec(),
            byte_offset: 12,
            byte_len: 48,
        };
        let enc = encode_log_locator(&locator);
        let dec = decode_log_locator(&enc).expect("decode locator");
        assert_eq!(dec, locator);
    }

    #[test]
    fn roundtrip_log_locator_page() {
        let mut entries = HashMap::new();
        entries.insert(
            0,
            LogLocator {
                blob_key: b"log_packs/a".to_vec(),
                byte_offset: 12,
                byte_len: 48,
            },
        );
        entries.insert(
            77,
            LogLocator {
                blob_key: b"log_packs/b".to_vec(),
                byte_offset: 900,
                byte_len: 16,
            },
        );

        let enc = encode_log_locator_page(1024, &entries);
        let (page_start, dec) = decode_log_locator_page(&enc).expect("decode locator page");

        assert_eq!(page_start, 1024);
        assert_eq!(dec.len(), entries.len());
        assert_eq!(dec.get(&0), entries.get(&0));
        assert_eq!(dec.get(&77), entries.get(&77));
    }
}
