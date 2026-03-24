use bytes::Bytes;

use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;
use crate::logs::types::{BlockLogHeader, Log, Topic32};

pub fn validate_log(log: &Log) -> bool {
    log.topics.len() <= 4
}

impl StorageCodec for Log {
    fn encode(&self) -> Bytes {
        let topic_count = self.topics.len() as u8;
        let mut out = Vec::with_capacity(80 + topic_count as usize * 32 + self.data.len());
        out.extend_from_slice(&self.address);
        out.push(topic_count);
        for topic in &self.topics {
            out.extend_from_slice(topic);
        }
        out.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.data);
        out.extend_from_slice(&self.block_num.to_be_bytes());
        out.extend_from_slice(&self.tx_idx.to_be_bytes());
        out.extend_from_slice(&self.log_idx.to_be_bytes());
        out.extend_from_slice(&self.block_hash);
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
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

        Ok(Self {
            address,
            topics,
            data,
            block_num: u64::from_be_bytes(block_num),
            tx_idx: u32::from_be_bytes(tx_idx),
            log_idx: u32::from_be_bytes(log_idx),
            block_hash,
        })
    }
}

impl StorageCodec for BlockLogHeader {
    fn encode(&self) -> Bytes {
        let offsets = self.offsets.encode();
        let mut out = Vec::with_capacity(1 + offsets.len());
        out.push(1);
        out.extend_from_slice(&offsets);
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 1 + 4 {
            return Err(Error::Decode("block log header too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported block log header version"));
        }
        let offsets = crate::core::offsets::BucketedOffsets::decode(&bytes[1..])?;
        if offsets.is_empty() {
            return Err(Error::Decode("block log header missing sentinel"));
        }
        Ok(Self { offsets })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs::types::DirBucket;

    #[test]
    fn roundtrip_large_log_dir_bucket() {
        let count = (u16::MAX as usize) + 2;
        let mut first_primary_ids = Vec::with_capacity(count);
        for i in 0..count {
            first_primary_ids.push(i as u64);
        }
        let bucket = DirBucket {
            start_block: 123,
            first_primary_ids,
        };

        let enc = bucket.encode();
        let dec = DirBucket::decode(&enc).expect("decode large bucket");
        assert_eq!(dec, bucket);
    }

    #[test]
    fn roundtrip_large_block_log_header() {
        let count = (u16::MAX as usize) + 2;
        let mut offsets = crate::core::offsets::BucketedOffsets::new();
        for i in 0..count {
            offsets.push(i as u64).expect("push offset");
        }
        let header = BlockLogHeader { offsets };

        let enc = header.encode();
        let dec = BlockLogHeader::decode(&enc).expect("decode large block log header");
        assert_eq!(dec, header);
    }
}
