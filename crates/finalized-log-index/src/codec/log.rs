use bytes::Bytes;

use crate::domain::types::{
    BlockMeta, Log, LogLocator, MetaState, Topic0Mode, Topic0Stats, Topic32,
};
use crate::error::{Error, Result};

pub fn validate_log(log: &Log) -> bool {
    log.topics.len() <= 4
}

pub fn encode_meta_state(state: &MetaState) -> Bytes {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&state.indexed_finalized_head.to_be_bytes());
    out.extend_from_slice(&state.next_log_id.to_be_bytes());
    out.extend_from_slice(&state.writer_epoch.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_meta_state(bytes: &[u8]) -> Result<MetaState> {
    if bytes.len() != 24 {
        return Err(Error::Decode("invalid meta/state length"));
    }
    let mut a = [0u8; 8];
    let mut b = [0u8; 8];
    let mut c = [0u8; 8];
    a.copy_from_slice(&bytes[0..8]);
    b.copy_from_slice(&bytes[8..16]);
    c.copy_from_slice(&bytes[16..24]);
    Ok(MetaState {
        indexed_finalized_head: u64::from_be_bytes(a),
        next_log_id: u64::from_be_bytes(b),
        writer_epoch: u64::from_be_bytes(c),
    })
}

pub fn encode_block_meta(meta: &BlockMeta) -> Bytes {
    let mut out = Vec::with_capacity(76);
    out.extend_from_slice(&meta.block_hash);
    out.extend_from_slice(&meta.parent_hash);
    out.extend_from_slice(&meta.first_log_id.to_be_bytes());
    out.extend_from_slice(&meta.count.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_block_meta(bytes: &[u8]) -> Result<BlockMeta> {
    if bytes.len() != 76 {
        return Err(Error::Decode("invalid block_meta length"));
    }
    let mut block_hash = [0u8; 32];
    let mut parent_hash = [0u8; 32];
    block_hash.copy_from_slice(&bytes[0..32]);
    parent_hash.copy_from_slice(&bytes[32..64]);
    let mut first_log_id = [0u8; 8];
    first_log_id.copy_from_slice(&bytes[64..72]);
    let mut count = [0u8; 4];
    count.copy_from_slice(&bytes[72..76]);

    Ok(BlockMeta {
        block_hash,
        parent_hash,
        first_log_id: u64::from_be_bytes(first_log_id),
        count: u32::from_be_bytes(count),
    })
}

pub fn encode_u64(v: u64) -> Bytes {
    Bytes::copy_from_slice(&v.to_be_bytes())
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(Error::Decode("invalid u64 length"));
    }
    let mut v = [0u8; 8];
    v.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(v))
}

pub fn encode_topic0_mode(mode: &Topic0Mode) -> Bytes {
    let mut out = Vec::with_capacity(10);
    out.push(1);
    out.push(if mode.log_enabled { 1 } else { 0 });
    out.extend_from_slice(&mode.enabled_from_block.to_be_bytes());
    Bytes::from(out)
}

pub fn decode_topic0_mode(bytes: &[u8]) -> Result<Topic0Mode> {
    if bytes.len() != 10 || bytes[0] != 1 {
        return Err(Error::Decode("invalid topic0_mode bytes"));
    }
    let log_enabled = bytes[1] == 1;
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[2..10]);
    Ok(Topic0Mode {
        log_enabled,
        enabled_from_block: u64::from_be_bytes(b),
    })
}

pub fn encode_topic0_stats(stats: &Topic0Stats) -> Bytes {
    let mut out = Vec::with_capacity(1 + 4 + 4 + 4 + 8 + 4 + stats.ring_bits.len());
    out.push(2);
    out.extend_from_slice(&stats.window_len.to_be_bytes());
    out.extend_from_slice(&stats.blocks_seen_in_window.to_be_bytes());
    out.extend_from_slice(&stats.ring_cursor.to_be_bytes());
    out.extend_from_slice(&stats.last_updated_block.to_be_bytes());
    out.extend_from_slice(&(stats.ring_bits.len() as u32).to_be_bytes());
    out.extend_from_slice(&stats.ring_bits);
    Bytes::from(out)
}

pub fn decode_topic0_stats(bytes: &[u8]) -> Result<Topic0Stats> {
    if bytes.is_empty() {
        return Err(Error::Decode("invalid topic0_stats bytes"));
    }
    match bytes[0] {
        1 => {
            if bytes.len() < 17 {
                return Err(Error::Decode("invalid topic0_stats bytes"));
            }
            let window_len = u32::from_be_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| Error::Decode("window_len"))?,
            );
            let blocks_seen_in_window = u32::from_be_bytes(
                bytes[5..9]
                    .try_into()
                    .map_err(|_| Error::Decode("blocks_seen"))?,
            );
            let ring_cursor = u32::from_be_bytes(
                bytes[9..13]
                    .try_into()
                    .map_err(|_| Error::Decode("ring_cursor"))?,
            );
            let ring_len = u32::from_be_bytes(
                bytes[13..17]
                    .try_into()
                    .map_err(|_| Error::Decode("ring_len"))?,
            ) as usize;
            if bytes.len() != 17 + ring_len {
                return Err(Error::Decode("topic0_stats ring length mismatch"));
            }
            let ring_bits = bytes[17..].to_vec();
            Ok(Topic0Stats {
                window_len,
                blocks_seen_in_window,
                ring_cursor,
                last_updated_block: 0,
                ring_bits,
            })
        }
        2 => {
            if bytes.len() < 25 {
                return Err(Error::Decode("invalid topic0_stats v2 bytes"));
            }
            let window_len = u32::from_be_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| Error::Decode("window_len"))?,
            );
            let blocks_seen_in_window = u32::from_be_bytes(
                bytes[5..9]
                    .try_into()
                    .map_err(|_| Error::Decode("blocks_seen"))?,
            );
            let ring_cursor = u32::from_be_bytes(
                bytes[9..13]
                    .try_into()
                    .map_err(|_| Error::Decode("ring_cursor"))?,
            );
            let last_updated_block = u64::from_be_bytes(
                bytes[13..21]
                    .try_into()
                    .map_err(|_| Error::Decode("last_updated"))?,
            );
            let ring_len = u32::from_be_bytes(
                bytes[21..25]
                    .try_into()
                    .map_err(|_| Error::Decode("ring_len"))?,
            ) as usize;
            if bytes.len() != 25 + ring_len {
                return Err(Error::Decode("topic0_stats v2 ring length mismatch"));
            }
            let ring_bits = bytes[25..].to_vec();
            Ok(Topic0Stats {
                window_len,
                blocks_seen_in_window,
                ring_cursor,
                last_updated_block,
                ring_bits,
            })
        }
        _ => Err(Error::Decode("unsupported topic0_stats version")),
    }
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
    fn roundtrip_state_and_block_meta() {
        let state = MetaState {
            indexed_finalized_head: 10,
            next_log_id: 55,
            writer_epoch: 9,
        };
        let dec_state = decode_meta_state(&encode_meta_state(&state)).expect("decode state");
        assert_eq!(dec_state, state);

        let meta = BlockMeta {
            block_hash: [1u8; 32],
            parent_hash: [2u8; 32],
            first_log_id: 77,
            count: 99,
        };
        let dec_meta = decode_block_meta(&encode_block_meta(&meta)).expect("decode meta");
        assert_eq!(dec_meta.first_log_id, meta.first_log_id);
        assert_eq!(dec_meta.count, meta.count);
        assert_eq!(dec_meta.block_hash, meta.block_hash);
        assert_eq!(dec_meta.parent_hash, meta.parent_hash);

        let mode = Topic0Mode {
            log_enabled: true,
            enabled_from_block: 42,
        };
        let dec_mode = decode_topic0_mode(&encode_topic0_mode(&mode)).expect("decode mode");
        assert_eq!(dec_mode, mode);

        let stats = Topic0Stats {
            window_len: 50_000,
            blocks_seen_in_window: 123,
            ring_cursor: 7,
            last_updated_block: 500,
            ring_bits: vec![1, 2, 3],
        };
        let dec_stats = decode_topic0_stats(&encode_topic0_stats(&stats)).expect("decode stats");
        assert_eq!(dec_stats, stats);
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
}
