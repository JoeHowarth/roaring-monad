use bytes::Bytes;

use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;
use crate::kernel::table_specs::{PointTableSpec, u64_key};
use crate::store::traits::TableId;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");

pub struct BlockRecordSpec;

impl PointTableSpec for BlockRecordSpec {
    const TABLE: TableId = BLOCK_RECORD_TABLE;
}

impl BlockRecordSpec {
    pub fn key(block_num: u64) -> Vec<u8> {
        u64_key(block_num)
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PrimaryWindowRecord {
    pub first_primary_id: u64,
    pub count: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockRecord {
    pub block_hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub logs: Option<PrimaryWindowRecord>,
    pub traces: Option<PrimaryWindowRecord>,
}

impl StorageCodec for BlockRecord {
    fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(1 + 32 + 32 + 1 + 2 * (8 + 4));
        out.push(1);
        out.extend_from_slice(&self.block_hash);
        out.extend_from_slice(&self.parent_hash);
        let mut flags = 0u8;
        if self.logs.is_some() {
            flags |= 1;
        }
        if self.traces.is_some() {
            flags |= 1 << 1;
        }
        out.push(flags);
        if let Some(window) = self.logs {
            out.extend_from_slice(&window.first_primary_id.to_be_bytes());
            out.extend_from_slice(&window.count.to_be_bytes());
        }
        if let Some(window) = self.traces {
            out.extend_from_slice(&window.first_primary_id.to_be_bytes());
            out.extend_from_slice(&window.count.to_be_bytes());
        }
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 32 + 32 + 1 {
            return Err(Error::Decode("shared block_record too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported shared block_record version"));
        }
        let mut block_hash = [0u8; 32];
        block_hash.copy_from_slice(&bytes[1..33]);
        let mut parent_hash = [0u8; 32];
        parent_hash.copy_from_slice(&bytes[33..65]);
        let flags = bytes[65];
        let mut pos = 66usize;
        let mut decode_window = |present: bool| -> Result<Option<PrimaryWindowRecord>> {
            if !present {
                return Ok(None);
            }
            if bytes.len() < pos + 12 {
                return Err(Error::Decode("shared block_record window too short"));
            }
            let first_primary_id = u64::from_be_bytes(
                bytes[pos..pos + 8]
                    .try_into()
                    .map_err(|_| Error::Decode("shared block_record first_primary_id"))?,
            );
            pos += 8;
            let count = u32::from_be_bytes(
                bytes[pos..pos + 4]
                    .try_into()
                    .map_err(|_| Error::Decode("shared block_record count"))?,
            );
            pos += 4;
            Ok(Some(PrimaryWindowRecord {
                first_primary_id,
                count,
            }))
        };
        let logs = decode_window((flags & 1) != 0)?;
        let traces = decode_window((flags & (1 << 1)) != 0)?;
        if pos != bytes.len() {
            return Err(Error::Decode("invalid shared block_record length"));
        }
        Ok(Self {
            block_hash,
            parent_hash,
            logs,
            traces,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockIdentity {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

impl BlockIdentity {
    pub fn into_block_ref(self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash,
            parent_hash: self.parent_hash,
        }
    }
}

impl From<(u64, &BlockRecord)> for BlockIdentity {
    fn from((number, meta): (u64, &BlockRecord)) -> Self {
        Self {
            number,
            hash: meta.block_hash,
            parent_hash: meta.parent_hash,
        }
    }
}

pub async fn load_block_identity<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<BlockIdentity>> {
    tables
        .block_records()
        .get(block_num)
        .await
        .map(|opt| opt.map(|block_record| BlockIdentity::from((block_num, &block_record))))
}

pub async fn derive_next_log_id<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    indexed_finalized_head: u64,
) -> Result<u64> {
    if indexed_finalized_head == 0 {
        return Ok(0);
    }

    let Some(block_record) = tables.block_records().get(indexed_finalized_head).await? else {
        return Err(Error::NotFound);
    };
    let Some(window) = block_record.logs else {
        return Err(Error::NotFound);
    };
    Ok(window
        .first_primary_id
        .saturating_add(u64::from(window.count)))
}

pub async fn load_block_num_by_hash<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_hash: &[u8; 32],
) -> Result<Option<u64>> {
    tables.block_hash_index().get(block_hash).await
}
