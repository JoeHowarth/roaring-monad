use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub(crate) async fn resolve_request_block_bounds<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    from_block_hash: Option<[u8; 32]>,
    to_block_hash: Option<[u8; 32]>,
) -> Result<(u64, u64)> {
    let from_block = match (from_block, from_block_hash) {
        (Some(number), None) => number,
        (None, Some(hash)) => tables
            .block_hash_index
            .get(&hash)
            .await?
            .ok_or(Error::InvalidParams("unknown from_block_hash"))?,
        _ => {
            return Err(Error::InvalidParams(
                "exactly one of from_block or from_block_hash is required",
            ));
        }
    };
    let to_block = match (to_block, to_block_hash) {
        (Some(number), None) => number,
        (None, Some(hash)) => tables
            .block_hash_index
            .get(&hash)
            .await?
            .ok_or(Error::InvalidParams("unknown to_block_hash"))?,
        _ => {
            return Err(Error::InvalidParams(
                "exactly one of to_block or to_block_hash is required",
            ));
        }
    };
    Ok((from_block, to_block))
}
