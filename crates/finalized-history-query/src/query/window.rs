use crate::core::ids::{FamilyIdRange, FamilyIdValue};
use crate::core::range::ResolvedBlockRange;
use crate::core::state::{BlockRecord, PrimaryWindowRecord};
use crate::error::Result;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

/// Maps a resolved finalized block range onto the inclusive primary-ID window
/// that contains the family's first and last non-empty blocks in that range.
pub(crate) async fn resolve_primary_window<M, B, I, F>(
    tables: &Tables<M, B>,
    block_range: &ResolvedBlockRange,
    select_window: F,
) -> Result<Option<FamilyIdRange<I>>>
where
    M: MetaStore,
    B: BlobStore,
    I: FamilyIdValue + Copy + Ord,
    F: Fn(&BlockRecord) -> Option<PrimaryWindowRecord>,
{
    if block_range.is_empty() {
        return Ok(None);
    }

    let Some(start) = first_primary_id_in_range(
        tables,
        &select_window,
        block_range.from_block,
        block_range.to_block,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(end_exclusive): Option<I> = end_primary_id_exclusive_in_range(
        tables,
        &select_window,
        block_range.from_block,
        block_range.to_block,
    )
    .await?
    else {
        return Ok(None);
    };

    Ok(FamilyIdRange::new(
        start,
        I::new(end_exclusive.get().saturating_sub(1)),
    ))
}

async fn first_primary_id_in_range<M, B, I, F>(
    tables: &Tables<M, B>,
    select_window: &F,
    from_block: u64,
    to_block: u64,
) -> Result<Option<I>>
where
    M: MetaStore,
    B: BlobStore,
    I: FamilyIdValue + Copy + Ord,
    F: Fn(&BlockRecord) -> Option<PrimaryWindowRecord>,
{
    let mut block_num = from_block;
    while block_num <= to_block {
        let Some(record) = tables.block_records.get(block_num).await? else {
            return Ok(None);
        };
        let Some(window) = select_window(&record) else {
            return Ok(None);
        };
        if window.count > 0 {
            return Ok(Some(I::new(window.first_primary_id)));
        }
        block_num = block_num.saturating_add(1);
    }
    Ok(None)
}

async fn end_primary_id_exclusive_in_range<M, B, I, F>(
    tables: &Tables<M, B>,
    select_window: &F,
    from_block: u64,
    to_block: u64,
) -> Result<Option<I>>
where
    M: MetaStore,
    B: BlobStore,
    I: FamilyIdValue + Copy + Ord,
    F: Fn(&BlockRecord) -> Option<PrimaryWindowRecord>,
{
    let mut block_num = to_block;
    loop {
        let Some(record) = tables.block_records.get(block_num).await? else {
            return Ok(None);
        };
        let Some(window) = select_window(&record) else {
            return Ok(None);
        };
        if window.count > 0 {
            return Ok(Some(I::new(
                window
                    .first_primary_id
                    .saturating_add(u64::from(window.count)),
            )));
        }
        if block_num == from_block {
            break;
        }
        block_num = block_num.saturating_sub(1);
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ids::LogId;
    use crate::core::refs::BlockRef;
    use crate::core::state::{BLOCK_RECORD_TABLE, BlockRecordSpec};
    use crate::kernel::codec::StorageCodec;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{MetaStore, PutCond};
    use futures::executor::block_on;

    async fn seed_log_block(
        meta: &InMemoryMetaStore,
        block_num: u64,
        first_log_id: u64,
        count: u32,
    ) {
        meta.put(
            BLOCK_RECORD_TABLE,
            &BlockRecordSpec::key(block_num),
            BlockRecord {
                block_hash: [block_num as u8; 32],
                parent_hash: [0; 32],
                logs: Some(PrimaryWindowRecord {
                    first_primary_id: first_log_id,
                    count,
                }),
                traces: None,
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

    async fn resolve_logs<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
        block_range: &ResolvedBlockRange,
    ) -> Result<Option<FamilyIdRange<LogId>>> {
        resolve_primary_window::<_, _, LogId, _>(tables, block_range, |record| record.logs).await
    }

    #[test]
    fn resolve_log_window_returns_none_for_empty_range() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = ResolvedBlockRange::empty(BlockRef::zero(0));
            assert!(resolve_logs(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_none_when_from_block_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 10);
            assert!(resolve_logs(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_none_when_to_block_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_log_block(&meta, 5, 100, 3).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 10);
            assert!(resolve_logs(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_none_for_zero_count_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_log_block(&meta, 5, 100, 0).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 5);
            assert!(resolve_logs(&tables, &range).await.unwrap().is_none());
        });
    }

    #[test]
    fn resolve_log_window_returns_valid_range_for_single_block() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_log_block(&meta, 7, 50, 5).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(7, 7);
            let window = resolve_logs(&tables, &range)
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
            seed_log_block(&meta, 5, 50, 3).await;
            seed_log_block(&meta, 7, 53, 3).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 7);
            let window = resolve_logs(&tables, &range)
                .await
                .unwrap()
                .expect("should return window");
            assert_eq!(window.start, LogId::new(50));
            assert_eq!(window.end_inclusive, LogId::new(55));
        });
    }

    #[test]
    fn resolve_log_window_skips_zero_count_boundary_blocks() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            seed_log_block(&meta, 5, 50, 0).await;
            seed_log_block(&meta, 6, 50, 2).await;
            seed_log_block(&meta, 7, 52, 0).await;
            let tables = Tables::without_cache(meta, InMemoryBlobStore::default());
            let range = non_empty_range(5, 7);
            let window = resolve_logs(&tables, &range)
                .await
                .unwrap()
                .expect("should return window");
            assert_eq!(window.start, LogId::new(50));
            assert_eq!(window.end_inclusive, LogId::new(51));
        });
    }
}
