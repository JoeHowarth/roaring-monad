use crate::domain::types::BlockRecord;
use crate::error::Result;
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

#[cfg(test)]
mod tests {
    use super::{load_log_block_record, load_log_block_window};
    use crate::core::ids::LogId;
    use crate::domain::keys::{BLOCK_RECORD_TABLE, block_record_suffix};
    use crate::domain::types::BlockRecord;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{MetaStore, PutCond};
    use crate::tables::Tables;
    use futures::executor::block_on;
    use std::sync::Arc;

    #[test]
    fn load_log_block_window_projects_log_specific_fields() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(
                Arc::new(meta.clone()),
                Arc::new(InMemoryBlobStore::default()),
            );
            meta.put(
                BLOCK_RECORD_TABLE,
                &block_record_suffix(14),
                BlockRecord {
                    block_hash: [1; 32],
                    parent_hash: [2; 32],
                    first_log_id: 42,
                    count: 3,
                }
                .encode(),
                PutCond::Any,
            )
            .await
            .expect("write block meta");

            let block_record = load_log_block_record(&tables, 14)
                .await
                .expect("load block meta")
                .expect("block meta present");
            let block_window = load_log_block_window(&tables, 14)
                .await
                .expect("load block window")
                .expect("block window present");

            assert_eq!(block_record.first_log_id, 42);
            assert_eq!(block_record.count, 3);
            assert_eq!(block_window.first_log_id, LogId::new(42));
            assert_eq!(block_window.count, 3);
        });
    }
}
