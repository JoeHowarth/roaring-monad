use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serializer};

use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;
use crate::kernel::table_specs::{PointTableSpec, u64_key};
use crate::store::traits::{BlobStore, MetaStore, TableId};
use crate::tables::Tables;

pub struct BlockHeaderSpec;

impl PointTableSpec for BlockHeaderSpec {
    const TABLE: TableId = TableId::new("block_header");
}

impl BlockHeaderSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct EvmBlockHeader {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub ommers_hash: [u8; 32],
    pub beneficiary: [u8; 20],
    pub state_root: [u8; 32],
    pub transactions_root: [u8; 32],
    pub receipts_root: [u8; 32],
    #[serde(
        serialize_with = "serialize_logs_bloom",
        deserialize_with = "deserialize_logs_bloom"
    )]
    pub logs_bloom: [[u8; 64]; 4],
    pub difficulty: [u8; 32],
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Vec<u8>,
    pub mix_hash: [u8; 32],
    pub nonce: [u8; 8],
    pub base_fee_per_gas: Option<[u8; 32]>,
    pub withdrawals_root: Option<[u8; 32]>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_block_root: Option<[u8; 32]>,
    pub requests_hash: Option<[u8; 32]>,
}

impl EvmBlockHeader {
    pub fn minimal(number: u64, hash: [u8; 32], parent_hash: [u8; 32]) -> Self {
        Self {
            number,
            hash,
            parent_hash,
            ommers_hash: [0; 32],
            beneficiary: [0; 20],
            state_root: [0; 32],
            transactions_root: [0; 32],
            receipts_root: [0; 32],
            logs_bloom: [[0; 64]; 4],
            difficulty: [0; 32],
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: Vec::new(),
            mix_hash: [0; 32],
            nonce: [0; 8],
            base_fee_per_gas: None,
            withdrawals_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_hash: None,
        }
    }
}

fn serialize_logs_bloom<S>(
    logs_bloom: &[[u8; 64]; 4],
    serializer: S,
) -> core::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut bytes = [0u8; 256];
    for (index, chunk) in logs_bloom.iter().enumerate() {
        let start = index * 64;
        let end = start + 64;
        bytes[start..end].copy_from_slice(chunk);
    }
    serializer.serialize_bytes(&bytes)
}

fn deserialize_logs_bloom<'de, D>(deserializer: D) -> core::result::Result<[[u8; 64]; 4], D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = <Vec<u8>>::deserialize(deserializer)?;
    if bytes.len() != 256 {
        return Err(serde::de::Error::invalid_length(bytes.len(), &"256 bytes"));
    }

    let mut logs_bloom = [[0u8; 64]; 4];
    for (index, chunk) in logs_bloom.iter_mut().enumerate() {
        let start = index * 64;
        let end = start + 64;
        chunk.copy_from_slice(&bytes[start..end]);
    }
    Ok(logs_bloom)
}

impl StorageCodec for EvmBlockHeader {
    fn encode(&self) -> Bytes {
        let mut flags = 0u8;
        if self.base_fee_per_gas.is_some() {
            flags |= 1;
        }
        if self.withdrawals_root.is_some() {
            flags |= 1 << 1;
        }
        if self.blob_gas_used.is_some() {
            flags |= 1 << 2;
        }
        if self.excess_blob_gas.is_some() {
            flags |= 1 << 3;
        }
        if self.parent_beacon_block_root.is_some() {
            flags |= 1 << 4;
        }
        if self.requests_hash.is_some() {
            flags |= 1 << 5;
        }

        let extra_data_len =
            u32::try_from(self.extra_data.len()).expect("extra_data must fit into u32");
        let mut out = Vec::with_capacity(
            1 + 1 + 8 + (32 * 8) + 20 + 8 + 8 + 8 + 4 + (64 * 4) + 4 + self.extra_data.len() + 8,
        );
        out.push(1);
        out.push(flags);
        out.extend_from_slice(&self.number.to_be_bytes());
        out.extend_from_slice(&self.hash);
        out.extend_from_slice(&self.parent_hash);
        out.extend_from_slice(&self.ommers_hash);
        out.extend_from_slice(&self.beneficiary);
        out.extend_from_slice(&self.state_root);
        out.extend_from_slice(&self.transactions_root);
        out.extend_from_slice(&self.receipts_root);
        out.extend_from_slice(&256u32.to_be_bytes());
        for chunk in self.logs_bloom {
            out.extend_from_slice(&chunk);
        }
        out.extend_from_slice(&self.difficulty);
        out.extend_from_slice(&self.gas_limit.to_be_bytes());
        out.extend_from_slice(&self.gas_used.to_be_bytes());
        out.extend_from_slice(&self.timestamp.to_be_bytes());
        out.extend_from_slice(&extra_data_len.to_be_bytes());
        out.extend_from_slice(&self.extra_data);
        out.extend_from_slice(&self.mix_hash);
        out.extend_from_slice(&self.nonce);
        if let Some(base_fee_per_gas) = self.base_fee_per_gas {
            out.extend_from_slice(&base_fee_per_gas);
        }
        if let Some(withdrawals_root) = self.withdrawals_root {
            out.extend_from_slice(&withdrawals_root);
        }
        if let Some(blob_gas_used) = self.blob_gas_used {
            out.extend_from_slice(&blob_gas_used.to_be_bytes());
        }
        if let Some(excess_blob_gas) = self.excess_blob_gas {
            out.extend_from_slice(&excess_blob_gas.to_be_bytes());
        }
        if let Some(parent_beacon_block_root) = self.parent_beacon_block_root {
            out.extend_from_slice(&parent_beacon_block_root);
        }
        if let Some(requests_hash) = self.requests_hash {
            out.extend_from_slice(&requests_hash);
        }
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 1 + 8 + (32 * 7) + 20 + 4 + 32 + 8 + 8 + 8 + 4 + 32 + 8 {
            return Err(Error::Decode("block header too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported block header version"));
        }
        let flags = bytes[1];
        let mut pos = 2usize;
        let read_u64 = |bytes: &[u8], pos: &mut usize, message: &'static str| -> Result<u64> {
            let end = pos.saturating_add(8);
            let raw = bytes
                .get(*pos..end)
                .ok_or(Error::Decode(message))?
                .try_into()
                .map_err(|_| Error::Decode(message))?;
            *pos = end;
            Ok(u64::from_be_bytes(raw))
        };
        let read_u32 = |bytes: &[u8], pos: &mut usize, message: &'static str| -> Result<u32> {
            let end = pos.saturating_add(4);
            let raw = bytes
                .get(*pos..end)
                .ok_or(Error::Decode(message))?
                .try_into()
                .map_err(|_| Error::Decode(message))?;
            *pos = end;
            Ok(u32::from_be_bytes(raw))
        };
        let read_fixed =
            |bytes: &[u8], pos: &mut usize, len: usize, message: &'static str| -> Result<Vec<u8>> {
                let end = pos.saturating_add(len);
                let raw = bytes.get(*pos..end).ok_or(Error::Decode(message))?;
                *pos = end;
                Ok(raw.to_vec())
            };

        let number = read_u64(bytes, &mut pos, "block header number")?;
        let hash =
            <[u8; 32]>::try_from(read_fixed(bytes, &mut pos, 32, "block header hash")?.as_slice())
                .map_err(|_| Error::Decode("block header hash"))?;
        let parent_hash = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header parent_hash")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header parent_hash"))?;
        let ommers_hash = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header ommers_hash")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header ommers_hash"))?;
        let beneficiary = <[u8; 20]>::try_from(
            read_fixed(bytes, &mut pos, 20, "block header beneficiary")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header beneficiary"))?;
        let state_root = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header state_root")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header state_root"))?;
        let transactions_root = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header transactions_root")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header transactions_root"))?;
        let receipts_root = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header receipts_root")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header receipts_root"))?;
        let logs_bloom_len =
            usize::try_from(read_u32(bytes, &mut pos, "block header logs_bloom length")?)
                .map_err(|_| Error::Decode("block header logs_bloom length"))?;
        if logs_bloom_len != 256 {
            return Err(Error::Decode("block header logs_bloom length"));
        }
        let logs_bloom_bytes =
            read_fixed(bytes, &mut pos, logs_bloom_len, "block header logs_bloom")?;
        let mut logs_bloom = [[0u8; 64]; 4];
        for (index, chunk) in logs_bloom.iter_mut().enumerate() {
            let start = index * 64;
            let end = start + 64;
            chunk.copy_from_slice(&logs_bloom_bytes[start..end]);
        }
        let difficulty = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header difficulty")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header difficulty"))?;
        let gas_limit = read_u64(bytes, &mut pos, "block header gas_limit")?;
        let gas_used = read_u64(bytes, &mut pos, "block header gas_used")?;
        let timestamp = read_u64(bytes, &mut pos, "block header timestamp")?;
        let extra_data_len =
            usize::try_from(read_u32(bytes, &mut pos, "block header extra_data length")?)
                .map_err(|_| Error::Decode("block header extra_data length"))?;
        let extra_data = read_fixed(bytes, &mut pos, extra_data_len, "block header extra_data")?;
        let mix_hash = <[u8; 32]>::try_from(
            read_fixed(bytes, &mut pos, 32, "block header mix_hash")?.as_slice(),
        )
        .map_err(|_| Error::Decode("block header mix_hash"))?;
        let nonce =
            <[u8; 8]>::try_from(read_fixed(bytes, &mut pos, 8, "block header nonce")?.as_slice())
                .map_err(|_| Error::Decode("block header nonce"))?;

        let base_fee_per_gas = if (flags & 1) != 0 {
            Some(
                <[u8; 32]>::try_from(
                    read_fixed(bytes, &mut pos, 32, "block header base_fee_per_gas")?.as_slice(),
                )
                .map_err(|_| Error::Decode("block header base_fee_per_gas"))?,
            )
        } else {
            None
        };
        let withdrawals_root = if (flags & (1 << 1)) != 0 {
            Some(
                <[u8; 32]>::try_from(
                    read_fixed(bytes, &mut pos, 32, "block header withdrawals_root")?.as_slice(),
                )
                .map_err(|_| Error::Decode("block header withdrawals_root"))?,
            )
        } else {
            None
        };
        let blob_gas_used = if (flags & (1 << 2)) != 0 {
            Some(read_u64(bytes, &mut pos, "block header blob_gas_used")?)
        } else {
            None
        };
        let excess_blob_gas = if (flags & (1 << 3)) != 0 {
            Some(read_u64(bytes, &mut pos, "block header excess_blob_gas")?)
        } else {
            None
        };
        let parent_beacon_block_root = if (flags & (1 << 4)) != 0 {
            Some(
                <[u8; 32]>::try_from(
                    read_fixed(bytes, &mut pos, 32, "block header parent_beacon_block_root")?
                        .as_slice(),
                )
                .map_err(|_| Error::Decode("block header parent_beacon_block_root"))?,
            )
        } else {
            None
        };
        let requests_hash = if (flags & (1 << 5)) != 0 {
            Some(
                <[u8; 32]>::try_from(
                    read_fixed(bytes, &mut pos, 32, "block header requests_hash")?.as_slice(),
                )
                .map_err(|_| Error::Decode("block header requests_hash"))?,
            )
        } else {
            None
        };

        if pos != bytes.len() {
            return Err(Error::Decode("invalid block header length"));
        }

        Ok(Self {
            number,
            hash,
            parent_hash,
            ommers_hash,
            beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            logs_bloom,
            difficulty,
            gas_limit,
            gas_used,
            timestamp,
            extra_data,
            mix_hash,
            nonce,
            base_fee_per_gas,
            withdrawals_root,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_block_root,
            requests_hash,
        })
    }
}

pub async fn load_block_header<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<EvmBlockHeader>> {
    tables.block_headers.get(block_num).await
}

#[cfg(test)]
mod tests {
    use super::EvmBlockHeader;
    use crate::kernel::codec::StorageCodec;

    #[test]
    fn roundtrip_block_header_codec() {
        let mut header = EvmBlockHeader::minimal(7, [7; 32], [6; 32]);
        header.ommers_hash = [1; 32];
        header.beneficiary = [2; 20];
        header.state_root = [3; 32];
        header.transactions_root = [4; 32];
        header.receipts_root = [5; 32];
        header.logs_bloom = [[6; 64]; 4];
        header.difficulty = [7; 32];
        header.gas_limit = 10;
        header.gas_used = 9;
        header.timestamp = 1234;
        header.extra_data = vec![1, 2, 3];
        header.mix_hash = [8; 32];
        header.nonce = [9; 8];
        header.base_fee_per_gas = Some([10; 32]);
        header.withdrawals_root = Some([11; 32]);
        header.blob_gas_used = Some(12);
        header.excess_blob_gas = Some(13);
        header.parent_beacon_block_root = Some([14; 32]);
        header.requests_hash = Some([15; 32]);

        let encoded = header.encode();
        let decoded = EvmBlockHeader::decode(&encoded).expect("decode");
        assert_eq!(decoded, header);
    }
}
