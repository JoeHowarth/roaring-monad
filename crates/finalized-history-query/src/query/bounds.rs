use crate::api::{IndexedQueryRequest, QueryBlocksRequest};
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub(crate) trait BlockBoundsRequest {
    fn requested_from_block(&self) -> Option<u64>;
    fn requested_to_block(&self) -> Option<u64>;
    fn requested_from_block_hash(&self) -> Option<[u8; 32]>;
    fn requested_to_block_hash(&self) -> Option<[u8; 32]>;
}

impl<F> BlockBoundsRequest for IndexedQueryRequest<F> {
    fn requested_from_block(&self) -> Option<u64> {
        self.from_block
    }

    fn requested_to_block(&self) -> Option<u64> {
        self.to_block
    }

    fn requested_from_block_hash(&self) -> Option<[u8; 32]> {
        self.from_block_hash
    }

    fn requested_to_block_hash(&self) -> Option<[u8; 32]> {
        self.to_block_hash
    }
}

impl BlockBoundsRequest for QueryBlocksRequest {
    fn requested_from_block(&self) -> Option<u64> {
        self.from_block
    }

    fn requested_to_block(&self) -> Option<u64> {
        self.to_block
    }

    fn requested_from_block_hash(&self) -> Option<[u8; 32]> {
        self.from_block_hash
    }

    fn requested_to_block_hash(&self) -> Option<[u8; 32]> {
        self.to_block_hash
    }
}

pub(crate) async fn resolve_request_block_bounds<
    M: MetaStore,
    B: BlobStore,
    R: BlockBoundsRequest,
>(
    tables: &Tables<M, B>,
    request: &R,
) -> Result<(u64, u64)> {
    let from_block = match (
        request.requested_from_block(),
        request.requested_from_block_hash(),
    ) {
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
    let to_block = match (
        request.requested_to_block(),
        request.requested_to_block_hash(),
    ) {
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
