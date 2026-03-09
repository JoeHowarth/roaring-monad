use crate::core::execution::{MatchedPrimary, PrimaryMaterializer};
use crate::core::ids::PrimaryIdRange;
use crate::core::range::ResolvedBlockRange;
use crate::error::Result;
use crate::logs::filter::LogFilter;
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone, Copy, Default)]
pub struct LogBlockScanner;

impl LogBlockScanner {
    pub async fn execute<M: MetaStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        block_range: &ResolvedBlockRange,
        log_window: PrimaryIdRange,
        filter: &LogFilter,
        take: usize,
    ) -> Result<Vec<MatchedPrimary<Log>>> {
        let mut out = Vec::new();
        let mut materializer = LogMaterializer::new(meta_store, blob_store);

        for block_num in block_range.from_block..=block_range.to_block {
            let Some(block_ref) = crate::core::range::RangeResolver
                .load_block_ref(meta_store, block_num)
                .await?
            else {
                continue;
            };
            let Some(record) = meta_store
                .get(&crate::domain::keys::block_meta_key(block_num))
                .await?
            else {
                continue;
            };
            let block_meta = crate::codec::log::decode_block_meta(&record.value)?;
            let start = block_meta.first_log_id.max(log_window.start);
            let end_inclusive = (block_meta.first_log_id + block_meta.count as u64)
                .saturating_sub(1)
                .min(log_window.end_inclusive);

            if start > end_inclusive {
                continue;
            }

            for id in start..=end_inclusive {
                let Some(log) = materializer.load_by_id(id).await? else {
                    continue;
                };
                if !crate::logs::filter::exact_match(&log, filter) {
                    continue;
                }

                out.push(MatchedPrimary {
                    id,
                    item: log,
                    block_ref,
                });
                if out.len() >= take {
                    return Ok(out);
                }
            }
        }

        Ok(out)
    }
}
