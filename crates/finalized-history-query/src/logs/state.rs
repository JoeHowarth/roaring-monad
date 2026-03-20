use crate::domain::keys::block_record_key;
use crate::domain::types::BlockRecord;
use crate::error::Result;
use crate::logs::types::LogBlockWindow;
use crate::store::traits::MetaStore;

pub async fn load_log_block_record<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
) -> Result<Option<BlockRecord>> {
    let Some(record) = meta_store.get(&block_record_key(block_num)).await? else {
        return Ok(None);
    };
    Ok(Some(BlockRecord::decode(&record.value)?))
}

pub async fn load_log_block_window<M: MetaStore>(
    meta_store: &M,
    block_num: u64,
) -> Result<Option<LogBlockWindow>> {
    Ok(load_log_block_record(meta_store, block_num)
        .await?
        .as_ref()
        .map(LogBlockWindow::from))
}

#[cfg(test)]
mod tests {
    use super::{load_log_block_record, load_log_block_window};
    use crate::core::ids::LogId;
    use crate::domain::keys::block_record_key;
    use crate::domain::types::BlockRecord;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{MetaStore, PutCond};
    use futures::executor::block_on;

    #[test]
    fn load_log_block_window_projects_log_specific_fields() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            meta.put(
                &block_record_key(14),
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

            let block_record = load_log_block_record(&meta, 14)
                .await
                .expect("load block meta")
                .expect("block meta present");
            let block_window = load_log_block_window(&meta, 14)
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
