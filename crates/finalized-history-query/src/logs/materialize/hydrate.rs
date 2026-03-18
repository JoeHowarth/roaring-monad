use crate::cache::{BytesCache, TableId};
use crate::codec::log_ref::{BlockLogHeaderRef, LogRef};
use crate::core::execution::PrimaryMaterializer;
use crate::core::ids::LogId;
use crate::core::refs::BlockRef;
use crate::domain::keys::{block_log_header_key, block_logs_blob_key, point_log_payload_cache_key};
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::state::load_log_block_meta;
use crate::store::traits::{BlobStore, MetaStore};

use super::LogMaterializer;

impl<'a, M: MetaStore, B: BlobStore, C: BytesCache> LogMaterializer<'a, M, B, C> {
    pub(crate) async fn load_block_header(
        &mut self,
        block_num: u64,
    ) -> Result<Option<BlockLogHeaderRef>> {
        let key = block_log_header_key(block_num);
        if let Some(bytes) = self.cache.get(TableId::BlockLogHeaders, &key) {
            return Ok(Some(BlockLogHeaderRef::new(bytes)?));
        }
        let Some(record) = self.meta_store.get(&key).await? else {
            return Ok(None);
        };
        self.cache.put(
            TableId::BlockLogHeaders,
            &key,
            record.value.clone(),
            record.value.len(),
        );
        Ok(Some(BlockLogHeaderRef::new(record.value)?))
    }

    fn point_log_payload_key(&self, block_num: u64, local_ordinal: usize) -> Result<Vec<u8>> {
        let local_ordinal =
            u64::try_from(local_ordinal).map_err(|_| Error::Decode("local ordinal overflow"))?;
        Ok(point_log_payload_cache_key(block_num, local_ordinal))
    }

    pub(crate) async fn load_contiguous_run(
        &mut self,
        block_num: u64,
        start_local_ordinal: usize,
        end_local_ordinal_inclusive: usize,
    ) -> Result<Vec<LogRef>> {
        if end_local_ordinal_inclusive < start_local_ordinal {
            return Ok(Vec::new());
        }

        let mut cached = Vec::with_capacity(
            end_local_ordinal_inclusive
                .saturating_sub(start_local_ordinal)
                .saturating_add(1),
        );
        let mut all_cached = true;
        for local_ordinal in start_local_ordinal..=end_local_ordinal_inclusive {
            let payload_key = self.point_log_payload_key(block_num, local_ordinal)?;
            let maybe_cached = self.cache.get(TableId::PointLogPayloads, &payload_key);
            cached.push((payload_key, maybe_cached));
            if cached
                .last()
                .and_then(|(_, value)| value.as_ref())
                .is_none()
            {
                all_cached = false;
            }
        }
        if all_cached {
            return cached
                .into_iter()
                .map(|(_, bytes)| bytes.map(LogRef::new).transpose()?.ok_or(Error::NotFound))
                .collect();
        }

        let Some(header) = self.load_block_header(block_num).await? else {
            return Ok(Vec::new());
        };
        if end_local_ordinal_inclusive + 1 >= header.count() {
            return Ok(Vec::new());
        }

        let start = header.offset(start_local_ordinal);
        let end = header.offset(end_local_ordinal_inclusive + 1);
        let Some(run_bytes) = self
            .blob_store
            .read_range(
                &block_logs_blob_key(block_num),
                u64::from(start),
                u64::from(end),
            )
            .await?
        else {
            return Ok(Vec::new());
        };

        let mut out = Vec::with_capacity(cached.len());
        for (index, (payload_key, maybe_cached)) in cached.into_iter().enumerate() {
            if let Some(bytes) = maybe_cached {
                out.push(LogRef::new(bytes)?);
                continue;
            }

            let local_ordinal = start_local_ordinal + index;
            let relative_start = header
                .offset(local_ordinal)
                .checked_sub(start)
                .ok_or(Error::Decode("invalid block log range"))?;
            let relative_end = header
                .offset(local_ordinal + 1)
                .checked_sub(start)
                .ok_or(Error::Decode("invalid block log range"))?;
            let payload = slice_relative(&run_bytes, relative_start, relative_end)?;
            self.cache.put(
                TableId::PointLogPayloads,
                &payload_key,
                payload.clone(),
                payload.len(),
            );
            out.push(LogRef::new(payload)?);
        }

        Ok(out)
    }
}

fn slice_relative(bytes: &bytes::Bytes, start: u32, end: u32) -> Result<bytes::Bytes> {
    let start = usize::try_from(start).map_err(|_| Error::Decode("block log range overflow"))?;
    let end = usize::try_from(end).map_err(|_| Error::Decode("block log range overflow"))?;
    if start > end || end > bytes.len() {
        return Err(Error::Decode("invalid block log range"));
    }
    Ok(bytes.slice(start..end))
}

impl<M: MetaStore, B: BlobStore, C: BytesCache> PrimaryMaterializer
    for LogMaterializer<'_, M, B, C>
{
    type Primary = LogRef;
    type Filter = LogFilter;

    async fn load_by_id(&mut self, id: LogId) -> Result<Option<Self::Primary>> {
        let Some(location) = self.resolve_log_id(id).await? else {
            return Ok(None);
        };
        Ok(self
            .load_contiguous_run(
                location.block_num,
                location.local_ordinal,
                location.local_ordinal,
            )
            .await?
            .into_iter()
            .next())
    }

    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef> {
        let block_num = item.block_num();
        if let Some(block_ref) = self.block_ref_cache.get(&block_num).copied() {
            return Ok(block_ref);
        }

        let block_ref = if let Some(block_ref) = self
            .range_resolver
            .load_block_ref(self.meta_store, block_num)
            .await?
        {
            block_ref
        } else {
            let Some(block_meta) = load_log_block_meta(self.meta_store, block_num).await? else {
                return Err(Error::NotFound);
            };
            BlockRef {
                number: block_num,
                hash: *item.block_hash(),
                parent_hash: block_meta.parent_hash,
            }
        };
        self.block_ref_cache.insert(block_num, block_ref);
        Ok(block_ref)
    }

    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }
}
