use crate::core::ids::{LogId, PrimaryIdRange};
use crate::core::range::ResolvedBlockRange;
use crate::error::Result;
use crate::logs::types::BlockRecord;
use crate::logs::types::LogBlockWindow;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub async fn load_log_block_record<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<BlockRecord>> {
    tables.block_records().get(block_num).await
}

pub async fn load_log_block_window<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
) -> Result<Option<LogBlockWindow>> {
    Ok(load_log_block_record(tables, block_num)
        .await?
        .as_ref()
        .map(LogBlockWindow::from))
}

pub async fn resolve_log_window<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_range: &ResolvedBlockRange,
) -> Result<Option<PrimaryIdRange>> {
    if block_range.is_empty() {
        return Ok(None);
    }

    let Some(from_block_window) = load_log_block_window(tables, block_range.from_block).await?
    else {
        return Ok(None);
    };
    let Some(to_block_window) = load_log_block_window(tables, block_range.to_block).await? else {
        return Ok(None);
    };

    let start = from_block_window.first_log_id;
    let end_exclusive = LogId::new(
        to_block_window
            .first_log_id
            .get()
            .saturating_add(to_block_window.count as u64),
    );
    if start >= end_exclusive {
        return Ok(None);
    }

    Ok(PrimaryIdRange::new(
        start,
        LogId::new(end_exclusive.get().saturating_sub(1)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::StorageCodec;
    use crate::core::refs::BlockRef;
    use crate::logs::keys::BLOCK_RECORD_TABLE;
    use crate::logs::table_specs::BlockRecordSpec;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{MetaStore, PutCond};
    use crate::tables::Tables;
    use futures::executor::block_on;

    async fn seed_block(meta: &InMemoryMetaStore, block_num: u64, first_log_id: u64, count: u32) {
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(block_num),
            BlockRecord {
                block_hash: [block_num as u8; 32],
                parent_hash: [0; 32],
                first_log_id,
                count,
            }
            .encode(),
            PutCond::Any,
        )
        .await
        .expect("seed block record");
    }

    fn non_empty_range(from: u64, to: u64) -> ResolvedBlockRange {
        ResolvedBlockRange {
            from_block: from,
            to_block: to,
            resolved_from_ref: BlockRef::zero(from),
            resolved_to_ref: BlockRef::zero(to),
            examined_endpoint_ref: BlockRef::zero(to),
        }
    }

    #[test]
    fn resolve_log_window_returns_none_for_empty_range() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = ResolvedBlockRange::empty(BlockRef::zero(0));
            assert!(resolve_log_window(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_none_when_from_block_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 10);
            assert!(resolve_log_window(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_none_when_to_block_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_block(&meta, 5, 100, 3).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 10);
            assert!(resolve_log_window(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_none_for_zero_count_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_block(&meta, 5, 100, 0).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 5);
            assert!(resolve_log_window(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_valid_range_for_single_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_block(&meta, 7, 50, 5).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(7, 7);
            let window = resolve_log_window(&tables, &range)
                .await
                .unwrap()
                .expect("should return window");
            assert_eq!(window.start, LogId::new(50));
            assert_eq!(window.end_inclusive, LogId::new(54));
        });
    }

    #[test]
    fn resolve_log_window_spans_multiple_blocks() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_block(&meta, 5, 50, 3).await;
            seed_block(&meta, 7, 53, 3).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 7);
            let window = resolve_log_window(&tables, &range)
                .await
                .unwrap()
                .expect("should return window");
            assert_eq!(window.start, LogId::new(50));
            assert_eq!(window.end_inclusive, LogId::new(55));
        });
    }
}
