use std::collections::HashMap;

use async_trait::async_trait;

use crate::codec::log::{decode_log, decode_log_locator, decode_log_locator_page};
use crate::core::execution::PrimaryMaterializer;
use crate::core::range::RangeResolver;
use crate::core::refs::BlockRef;
use crate::domain::keys::{log_locator_key, log_locator_page_key, log_locator_page_start};
use crate::error::{Error, Result};
use crate::logs::filter::{LogFilter, exact_match};
use crate::logs::state::load_log_block_meta;
use crate::logs::types::{Log, LogLocator};
use crate::store::traits::{BlobStore, MetaStore};

pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore> {
    meta_store: &'a M,
    blob_store: &'a B,
    range_resolver: RangeResolver,
    locator_page_cache: HashMap<u64, HashMap<u16, LogLocator>>,
    log_pack_cache: HashMap<Vec<u8>, bytes::Bytes>,
    block_ref_cache: HashMap<u64, BlockRef>,
}

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub fn new(meta_store: &'a M, blob_store: &'a B) -> Self {
        Self {
            meta_store,
            blob_store,
            range_resolver: RangeResolver,
            locator_page_cache: HashMap::new(),
            log_pack_cache: HashMap::new(),
            block_ref_cache: HashMap::new(),
        }
    }
}

#[async_trait]
impl<M: MetaStore, B: BlobStore> PrimaryMaterializer for LogMaterializer<'_, M, B> {
    type Primary = Log;
    type Filter = LogFilter;

    async fn load_by_id(&mut self, id: u64) -> Result<Option<Self::Primary>> {
        let page_start = log_locator_page_start(id);
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.locator_page_cache.entry(page_start)
        {
            let page = match self
                .meta_store
                .get(&log_locator_page_key(page_start))
                .await?
            {
                Some(record) => {
                    let (stored_page_start, entries) = decode_log_locator_page(&record.value)?;
                    if stored_page_start != page_start {
                        return Err(Error::Decode("log locator page start mismatch"));
                    }
                    entries
                }
                None => HashMap::new(),
            };
            entry.insert(page);
        }

        let mut locator = self.locator_page_cache.get(&page_start).and_then(|page| {
            let slot = u16::try_from(id - page_start).ok()?;
            page.get(&slot).cloned()
        });
        if locator.is_none() {
            locator = match self.meta_store.get(&log_locator_key(id)).await? {
                Some(record) => Some(decode_log_locator(&record.value)?),
                None => None,
            };
        }
        let Some(locator) = locator else {
            return Ok(None);
        };

        if !self.log_pack_cache.contains_key(&locator.blob_key) {
            let Some(blob) = self.blob_store.get_blob(&locator.blob_key).await? else {
                return Ok(None);
            };
            self.log_pack_cache.insert(locator.blob_key.clone(), blob);
        }

        let Some(blob) = self.log_pack_cache.get(&locator.blob_key) else {
            return Ok(None);
        };
        let start = locator.byte_offset as usize;
        let end = start.saturating_add(locator.byte_len as usize);
        if end > blob.len() || start > end {
            return Err(Error::Decode("invalid log locator span"));
        }

        Ok(Some(decode_log(&blob[start..end])?))
    }

    async fn block_ref_for(&mut self, item: &Self::Primary) -> Result<BlockRef> {
        if let Some(block_ref) = self.block_ref_cache.get(&item.block_num).copied() {
            return Ok(block_ref);
        }

        let block_ref = if let Some(block_ref) = self
            .range_resolver
            .load_block_ref(self.meta_store, item.block_num)
            .await?
        {
            block_ref
        } else {
            let Some(block_meta) = load_log_block_meta(self.meta_store, item.block_num).await?
            else {
                return Err(Error::NotFound);
            };
            BlockRef {
                number: item.block_num,
                hash: item.block_hash,
                parent_hash: block_meta.parent_hash,
            }
        };
        self.block_ref_cache.insert(item.block_num, block_ref);
        Ok(block_ref)
    }

    fn exact_match(&self, item: &Self::Primary, filter: &Self::Filter) -> bool {
        exact_match(item, filter)
    }
}
