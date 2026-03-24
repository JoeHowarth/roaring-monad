use bytes::Bytes;

use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct PrimaryDirBucket {
    pub start_block: u64,
    pub first_primary_ids: Vec<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PrimaryDirFragment {
    pub block_num: u64,
    pub first_primary_id: u64,
    pub end_primary_id_exclusive: u64,
}

impl StorageCodec for PrimaryDirBucket {
    fn encode(&self) -> Bytes {
        assert!(u32::try_from(self.first_primary_ids.len()).is_ok());
        let mut out = Vec::with_capacity(1 + 8 + 4 + self.first_primary_ids.len() * 8);
        out.push(1);
        out.extend_from_slice(&self.start_block.to_be_bytes());
        out.extend_from_slice(&(self.first_primary_ids.len() as u32).to_be_bytes());
        for first_primary_id in &self.first_primary_ids {
            out.extend_from_slice(&first_primary_id.to_be_bytes());
        }
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 8 + 4 + 8 {
            return Err(Error::Decode("primary directory bucket too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode(
                "unsupported primary directory bucket version",
            ));
        }
        let start_block = u64::from_be_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| Error::Decode("primary directory bucket start_block"))?,
        );
        let count = u32::from_be_bytes(
            bytes[9..13]
                .try_into()
                .map_err(|_| Error::Decode("primary directory bucket count"))?,
        ) as usize;
        if count < 2 {
            return Err(Error::Decode("primary directory bucket missing sentinel"));
        }
        let expected_len = 1 + 8 + 4 + count * 8;
        if bytes.len() != expected_len {
            return Err(Error::Decode("invalid primary directory bucket length"));
        }
        let mut first_primary_ids = Vec::with_capacity(count);
        let mut pos = 13usize;
        for _ in 0..count {
            first_primary_ids.push(u64::from_be_bytes(
                bytes[pos..pos + 8]
                    .try_into()
                    .map_err(|_| Error::Decode("primary directory bucket first_primary_id"))?,
            ));
            pos += 8;
        }
        Ok(Self {
            start_block,
            first_primary_ids,
        })
    }
}

impl StorageCodec for PrimaryDirFragment {
    fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(1 + 8 + 8 + 8);
        out.push(1);
        out.extend_from_slice(&self.block_num.to_be_bytes());
        out.extend_from_slice(&self.first_primary_id.to_be_bytes());
        out.extend_from_slice(&self.end_primary_id_exclusive.to_be_bytes());
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 1 + 8 + 8 + 8 {
            return Err(Error::Decode("invalid primary_dir fragment length"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported primary_dir fragment version"));
        }
        Ok(Self {
            block_num: u64::from_be_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|_| Error::Decode("primary_dir fragment block_num"))?,
            ),
            first_primary_id: u64::from_be_bytes(
                bytes[9..17]
                    .try_into()
                    .map_err(|_| Error::Decode("primary_dir fragment first_primary_id"))?,
            ),
            end_primary_id_exclusive: u64::from_be_bytes(
                bytes[17..25]
                    .try_into()
                    .map_err(|_| Error::Decode("primary_dir fragment end_primary_id_exclusive"))?,
            ),
        })
    }
}
