use std::collections::BTreeMap;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use roaring::RoaringBitmap;

use crate::api::query_logs::{ExecutionBudget, QueryLogsRequest};
use crate::cache::{BytesCache, NoopBytesCache, TableId};
use crate::codec::log::decode_stream_bitmap_meta;
use crate::codec::log_ref::LogRef;
use crate::config::Config;
use crate::core::execution::{MatchedPrimary, PrimaryMaterializer, ShardBitmapSet};
use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{RangeResolver, ResolvedBlockRange};
use crate::domain::keys::{
    STREAM_PAGE_LOCAL_ID_SPAN, local_range_for_shard, log_shard, stream_fragment_blob_key,
    stream_fragment_meta_prefix, stream_id, stream_page_blob_key, stream_page_meta_key,
    stream_page_start_local,
};
use crate::error::{Error, Result};
use crate::logs::filter::LogFilter;
use crate::logs::index_spec::{
    ClauseKind, clause_values_20, clause_values_32, is_full_shard_range, is_too_broad,
};
use crate::logs::materialize::LogMaterializer;
use crate::logs::types::Log;
use crate::logs::window::LogWindowResolver;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::chunk::decode_chunk;

const STREAM_LOAD_CONCURRENCY: usize = 32;

#[derive(Debug, Clone)]
struct IndexedClauseSpec {
    kind: ClauseKind,
    stream_kind: &'static str,
    values: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct PreparedShardClause {
    kind: ClauseKind,
    stream_ids: Vec<String>,
    estimated_count: u64,
}

#[derive(Debug, Clone)]
pub struct LogsQueryEngine {
    max_or_terms: usize,
    range_resolver: RangeResolver,
    window_resolver: LogWindowResolver,
}

impl LogsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
            range_resolver: RangeResolver,
            window_resolver: LogWindowResolver,
        }
    }

    pub async fn query_logs<M: MetaStore + PublicationStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>> {
        self.query_logs_with_cache(meta_store, blob_store, &NoopBytesCache, request, budget)
            .await
    }

    pub async fn query_logs_with_cache<
        M: MetaStore + PublicationStore,
        B: BlobStore,
        C: BytesCache,
    >(
        &self,
        meta_store: &M,
        blob_store: &B,
        cache: &C,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>> {
        if request.limit == 0 {
            return Err(Error::InvalidParams("limit must be at least 1"));
        }
        if !request.filter.has_indexed_clause() {
            return Err(Error::InvalidParams(
                "query must include at least one indexed address or topic clause",
            ));
        }

        let effective_limit = match budget.max_results {
            Some(0) => {
                return Err(Error::InvalidParams(
                    "budget.max_results must be at least 1 when set",
                ));
            }
            Some(max_results) => request.limit.min(max_results),
            None => request.limit,
        };

        let block_range = self
            .range_resolver
            .resolve(
                meta_store,
                request.from_block,
                request.to_block,
                request.order,
            )
            .await?;
        if block_range.is_empty() {
            return Ok(self.empty_page(&block_range));
        }

        let Some(mut log_window) = self
            .window_resolver
            .resolve(meta_store, &block_range)
            .await?
        else {
            return Ok(self.empty_page(&block_range));
        };

        if let Some(resume_log_id) = request.resume_log_id.map(LogId::new) {
            if !log_window.contains(resume_log_id) {
                return Err(Error::InvalidParams(
                    "resume_log_id outside resolved block window",
                ));
            }

            let Some(resumed_window) = log_window.resume_strictly_after(resume_log_id) else {
                return Ok(self.empty_page(&block_range));
            };
            log_window = resumed_window;
        }

        if is_too_broad(&request.filter, self.max_or_terms) {
            let actual = request.filter.max_or_terms();
            return Err(Error::QueryTooBroad {
                actual,
                max: self.max_or_terms,
            });
        }

        let take = effective_limit.saturating_add(1);
        let matched = self
            .execute_indexed_query(
                meta_store,
                blob_store,
                cache,
                &request.filter,
                (log_window.start, log_window.end_inclusive),
                take,
            )
            .await?;

        Ok(self.build_page(block_range, effective_limit, matched))
    }

    fn empty_page(&self, block_range: &ResolvedBlockRange) -> QueryPage<Log> {
        QueryPage {
            items: Vec::new(),
            meta: QueryPageMeta {
                resolved_from_block: block_range.resolved_from_ref,
                resolved_to_block: block_range.resolved_to_ref,
                cursor_block: block_range.examined_endpoint_ref,
                has_more: false,
                next_resume_log_id: None,
            },
        }
    }

    fn build_page(
        &self,
        block_range: ResolvedBlockRange,
        effective_limit: usize,
        mut matched: Vec<MatchedPrimary<LogRef>>,
    ) -> QueryPage<Log> {
        let has_more = matched.len() > effective_limit;
        if has_more {
            matched.truncate(effective_limit);
        }

        let next_resume_log_id = if has_more {
            matched.last().map(|matched_log| matched_log.id.get())
        } else {
            None
        };
        let cursor_block = matched
            .last()
            .map(|matched_log| matched_log.block_ref)
            .unwrap_or(block_range.examined_endpoint_ref);
        // Convert LogRef → Log at the API boundary
        let items = matched
            .into_iter()
            .map(|matched_log| matched_log.item.to_owned_log())
            .collect();

        QueryPage {
            items,
            meta: QueryPageMeta {
                resolved_from_block: block_range.resolved_from_ref,
                resolved_to_block: block_range.resolved_to_ref,
                cursor_block,
                has_more,
                next_resume_log_id,
            },
        }
    }

    async fn execute_indexed_query<M: MetaStore, B: BlobStore, C: BytesCache>(
        &self,
        meta_store: &M,
        blob_store: &B,
        cache: &C,
        filter: &LogFilter,
        id_window: (LogId, LogId),
        take: usize,
    ) -> Result<Vec<MatchedPrimary<LogRef>>> {
        let (from_log_id, to_log_id_inclusive) = id_window;
        let clause_specs = build_clause_specs(filter);
        let mut matched = Vec::new();
        let mut materializer = LogMaterializer::new(meta_store, blob_store, cache);

        for shard_raw in from_log_id.shard().get()..=to_log_id_inclusive.shard().get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            let shard_clauses = prepare_shard_clauses(
                meta_store,
                cache,
                &clause_specs,
                shard,
                local_from,
                local_to,
            )
            .await?;

            if shard_clauses.is_empty() {
                continue;
            }

            let mut shard_accumulator: Option<RoaringBitmap> = None;
            for prepared_clause in shard_clauses {
                let clause_bitmap = load_prepared_clause_bitmap(
                    meta_store,
                    blob_store,
                    cache,
                    &prepared_clause,
                    local_from.get(),
                    local_to.get(),
                )
                .await?;
                if clause_bitmap.is_empty() {
                    shard_accumulator = Some(RoaringBitmap::new());
                    break;
                }

                match shard_accumulator.as_mut() {
                    Some(accumulator) => {
                        *accumulator &= &clause_bitmap;
                        if accumulator.is_empty() {
                            break;
                        }
                    }
                    None => shard_accumulator = Some(clause_bitmap),
                }
            }

            let Some(shard_accumulator) = shard_accumulator else {
                continue;
            };
            if shard_accumulator.is_empty() {
                continue;
            }

            for local_raw in shard_accumulator {
                let local = LogLocalId::new(local_raw).expect("bitmap values must fit local-id");
                let id = compose_log_id(shard, local);
                let Some(item) = materializer.load_by_id(id).await? else {
                    continue;
                };
                if !materializer.exact_match(&item, filter) {
                    continue;
                }

                let block_ref = materializer.block_ref_for(&item).await?;
                matched.push(MatchedPrimary {
                    id,
                    item,
                    block_ref,
                });
                if matched.len() >= take {
                    return Ok(matched);
                }
            }
        }

        Ok(matched)
    }
}

async fn load_clause_sets<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    clause_specs: &[IndexedClauseSpec],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    load_clause_sets_with_cache(
        meta_store,
        blob_store,
        &NoopBytesCache,
        clause_specs,
        from_log_id,
        to_log_id_inclusive,
    )
    .await
}

async fn load_clause_sets_with_cache<M: MetaStore, B: BlobStore, C: BytesCache>(
    meta_store: &M,
    blob_store: &B,
    cache: &C,
    clause_specs: &[IndexedClauseSpec],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    let mut clause_sets = Vec::new();

    for clause_spec in clause_specs {
        let set = fetch_union_log_level_with_cache(
            meta_store,
            blob_store,
            cache,
            clause_spec.stream_kind,
            &clause_spec.values,
            from_log_id,
            to_log_id_inclusive,
        )
        .await?;
        clause_sets.push(set);
    }

    Ok(clause_sets)
}

#[doc(hidden)]
pub async fn load_clause_sets_for_benchmark<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    filter: &LogFilter,
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<Vec<ShardBitmapSet>> {
    load_clause_sets(
        meta_store,
        blob_store,
        &build_clause_specs(filter),
        from_log_id,
        to_log_id_inclusive,
    )
    .await
}

fn build_clause_specs(filter: &LogFilter) -> Vec<IndexedClauseSpec> {
    let mut clauses = Vec::new();

    if let Some(clause) = &filter.address {
        let values = clause_values_20(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Address,
                stream_kind: "addr",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic1 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic1,
                stream_kind: "topic1",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic2 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic2,
                stream_kind: "topic2",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic3 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic3,
                stream_kind: "topic3",
                values,
            });
        }
    }

    if let Some(clause) = &filter.topic0 {
        let values = clause_values_32(clause);
        if !values.is_empty() {
            clauses.push(IndexedClauseSpec {
                kind: ClauseKind::Topic0,
                stream_kind: "topic0",
                values,
            });
        }
    }

    clauses
}

async fn prepare_shard_clauses<M: MetaStore>(
    meta_store: &M,
    cache: &impl BytesCache,
    clause_specs: &[IndexedClauseSpec],
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<Vec<PreparedShardClause>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());

    for clause_spec in clause_specs {
        let clause =
            prepare_shard_clause(meta_store, cache, clause_spec, shard, local_from, local_to)
                .await?;
        prepared.push(clause);
    }

    prepared.sort_by_key(|clause| (clause.estimated_count, clause_kind_rank(clause.kind)));
    Ok(prepared)
}

async fn prepare_shard_clause<M: MetaStore>(
    meta_store: &M,
    cache: &impl BytesCache,
    clause_spec: &IndexedClauseSpec,
    shard: LogShard,
    local_from: LogLocalId,
    local_to: LogLocalId,
) -> Result<PreparedShardClause> {
    let mut stream_ids = Vec::with_capacity(clause_spec.values.len());
    let mut estimated_count = 0u64;

    for value in &clause_spec.values {
        let stream = stream_id(clause_spec.stream_kind, value, shard);
        estimated_count = estimated_count.saturating_add(
            estimate_stream_overlap(meta_store, cache, &stream, local_from.get(), local_to.get())
                .await?,
        );
        stream_ids.push(stream);
    }

    Ok(PreparedShardClause {
        kind: clause_spec.kind,
        stream_ids,
        estimated_count,
    })
}

async fn estimate_stream_overlap<M: MetaStore>(
    meta_store: &M,
    cache: &impl BytesCache,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<u64> {
    let mut estimated = 0u64;
    let mut page_start = stream_page_start_local(local_from);
    let last_page_start = stream_page_start_local(local_to);

    loop {
        if let Some(meta) = load_stream_page_meta(meta_store, cache, stream_id, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(meta.count));
            }
        } else {
            let page = meta_store
                .list_prefix(
                    &stream_fragment_meta_prefix(stream_id, page_start),
                    None,
                    usize::MAX,
                )
                .await?;
            for key in page.keys {
                let Some(record) = meta_store.get(&key).await? else {
                    continue;
                };
                let meta = decode_stream_bitmap_meta(&record.value)?;
                if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                    estimated = estimated.saturating_add(u64::from(meta.count));
                }
            }
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(estimated)
}

async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    cache: &impl BytesCache,
    prepared_clause: &PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut in_flight = FuturesUnordered::new();

    for stream_id in &prepared_clause.stream_ids {
        in_flight.push(async move {
            load_stream_entries(
                meta_store, blob_store, cache, stream_id, local_from, local_to,
            )
            .await
        });
        if in_flight.len() >= STREAM_LOAD_CONCURRENCY
            && let Some(result) = in_flight.next().await
        {
            out |= &result?;
        }
    }

    while let Some(result) = in_flight.next().await {
        out |= &result?;
    }

    Ok(out)
}

fn clause_kind_rank(kind: ClauseKind) -> u8 {
    match kind {
        ClauseKind::Address => 0,
        ClauseKind::Topic1 => 1,
        ClauseKind::Topic2 => 2,
        ClauseKind::Topic3 => 3,
        ClauseKind::Topic0 => 4,
    }
}

async fn fetch_union_log_level_with_cache<M: MetaStore, B: BlobStore, C: BytesCache>(
    meta_store: &M,
    blob_store: &B,
    cache: &C,
    kind: &str,
    values: &[Vec<u8>],
    from_log_id: LogId,
    to_log_id_inclusive: LogId,
) -> Result<ShardBitmapSet> {
    let mut out = BTreeMap::new();
    let from_shard = log_shard(from_log_id);
    let to_shard = log_shard(to_log_id_inclusive);
    let mut in_flight = FuturesUnordered::new();

    for value in values {
        for shard_raw in from_shard.get()..=to_shard.get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let stream = stream_id(kind, value, shard);
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            in_flight.push(async move {
                let entries = load_stream_entries(
                    meta_store,
                    blob_store,
                    cache,
                    &stream,
                    local_from.get(),
                    local_to.get(),
                )
                .await?;
                Ok::<(LogShard, RoaringBitmap), Error>((shard, entries))
            });
            if in_flight.len() >= STREAM_LOAD_CONCURRENCY
                && let Some(result) = in_flight.next().await
            {
                merge_union_entries(&mut out, result?);
            }
        }
    }

    while let Some(result) = in_flight.next().await {
        merge_union_entries(&mut out, result?);
    }

    Ok(out)
}

fn merge_union_entries(out: &mut ShardBitmapSet, (shard, entries): (LogShard, RoaringBitmap)) {
    if entries.is_empty() {
        return;
    }
    out.entry(shard)
        .and_modify(|existing| *existing |= &entries)
        .or_insert(entries);
}

async fn load_stream_entries<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    cache: &impl BytesCache,
    stream: &str,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    let mut page_start = stream_page_start_local(local_from);
    let last_page_start = stream_page_start_local(local_to);

    loop {
        if let Some(meta) = load_stream_page_meta(meta_store, cache, stream, page_start).await? {
            if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                let loaded_page_blob = maybe_merge_cached_bitmap_blob(
                    blob_store,
                    cache,
                    &stream_page_blob_key(stream, page_start),
                    &mut out,
                    local_from,
                    local_to,
                )
                .await?;
                if !loaded_page_blob {
                    load_stream_fragment_entries_for_page(
                        meta_store, blob_store, stream, page_start, local_from, local_to, &mut out,
                    )
                    .await?;
                }
            }
        } else {
            load_stream_fragment_entries_for_page(
                meta_store, blob_store, stream, page_start, local_from, local_to, &mut out,
            )
            .await?;
        }

        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }

    Ok(out)
}

async fn load_stream_fragment_entries_for_page<M: MetaStore, B: BlobStore>(
    meta_store: &M,
    blob_store: &B,
    stream: &str,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    out: &mut RoaringBitmap,
) -> Result<()> {
    let page = meta_store
        .list_prefix(
            &stream_fragment_meta_prefix(stream, page_start),
            None,
            usize::MAX,
        )
        .await?;
    for key in page.keys {
        let Some(record) = meta_store.get(&key).await? else {
            continue;
        };
        let meta = decode_stream_bitmap_meta(&record.value)?;
        if !overlaps(meta.min_local, meta.max_local, local_from, local_to) {
            continue;
        }
        let block_num = read_u64_suffix(&key)?;
        let _ = maybe_merge_bitmap_blob(
            blob_store,
            &stream_fragment_blob_key(stream, page_start, block_num),
            out,
            local_from,
            local_to,
        )
        .await?;
    }
    Ok(())
}

async fn maybe_merge_bitmap_blob<B: BlobStore>(
    blob_store: &B,
    key: &[u8],
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = blob_store.get_blob(key).await? else {
        return Ok(false);
    };
    let chunk = decode_chunk(&bytes)?;
    if is_full_shard_range(local_from, local_to)
        || (chunk.min_local >= local_from && chunk.max_local <= local_to)
    {
        *out |= &chunk.bitmap;
        return Ok(true);
    }

    for value in chunk.bitmap {
        if value >= local_from && value <= local_to {
            out.insert(value);
        }
    }
    Ok(true)
}

async fn load_stream_page_meta<M: MetaStore, C: BytesCache>(
    meta_store: &M,
    cache: &C,
    stream: &str,
    page_start: u32,
) -> Result<Option<crate::domain::types::StreamBitmapMeta>> {
    let key = stream_page_meta_key(stream, page_start);
    if let Some(bytes) = cache.get(TableId::StreamPageMeta, &key) {
        return Ok(Some(decode_stream_bitmap_meta(&bytes)?));
    }

    let Some(record) = meta_store.get(&key).await? else {
        return Ok(None);
    };
    cache.put(
        TableId::StreamPageMeta,
        &key,
        record.value.clone(),
        record.value.len(),
    );
    Ok(Some(decode_stream_bitmap_meta(&record.value)?))
}

async fn load_stream_page_blob<B: BlobStore, C: BytesCache>(
    blob_store: &B,
    cache: &C,
    key: &[u8],
) -> Result<Option<Bytes>> {
    if let Some(bytes) = cache.get(TableId::StreamPageBlobs, key) {
        return Ok(Some(bytes));
    }

    let Some(bytes) = blob_store.get_blob(key).await? else {
        return Ok(None);
    };
    cache.put(TableId::StreamPageBlobs, key, bytes.clone(), bytes.len());
    Ok(Some(bytes))
}

async fn maybe_merge_cached_bitmap_blob<B: BlobStore, C: BytesCache>(
    blob_store: &B,
    cache: &C,
    key: &[u8],
    out: &mut RoaringBitmap,
    local_from: u32,
    local_to: u32,
) -> Result<bool> {
    let Some(bytes) = load_stream_page_blob(blob_store, cache, key).await? else {
        return Ok(false);
    };
    let chunk = decode_chunk(&bytes)?;
    if is_full_shard_range(local_from, local_to)
        || (chunk.min_local >= local_from && chunk.max_local <= local_to)
    {
        *out |= &chunk.bitmap;
        return Ok(true);
    }

    for value in chunk.bitmap {
        if value >= local_from && value <= local_to {
            out.insert(value);
        }
    }
    Ok(true)
}

fn overlaps(min_local: u32, max_local: u32, local_from: u32, local_to: u32) -> bool {
    min_local <= local_to && max_local >= local_from
}

fn read_u64_suffix(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::Decode("short key suffix"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::load_stream_entries;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use crate::cache::{BytesCacheConfig, HashMapBytesCache, NoopBytesCache, TableCacheConfig};
    use crate::codec::log::encode_stream_bitmap_meta;
    use crate::domain::keys::{
        stream_fragment_blob_key, stream_fragment_meta_key, stream_page_blob_key,
        stream_page_meta_key,
    };
    use crate::domain::types::StreamBitmapMeta;
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::{
        BlobStore, FenceToken, MetaStore, Page, PutCond, PutResult, Record,
    };
    use crate::streams::chunk::{ChunkBlob, encode_chunk};
    use futures::executor::block_on;
    use roaring::RoaringBitmap;

    struct CountingMetaStore {
        inner: InMemoryMetaStore,
        target_key: Vec<u8>,
        get_count: Arc<AtomicU64>,
    }

    impl MetaStore for CountingMetaStore {
        async fn get(&self, key: &[u8]) -> crate::Result<Option<Record>> {
            if key == self.target_key.as_slice() {
                self.get_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get(key).await
        }

        async fn put(
            &self,
            key: &[u8],
            value: Bytes,
            cond: PutCond,
            fence: FenceToken,
        ) -> crate::Result<PutResult> {
            self.inner.put(key, value, cond, fence).await
        }

        async fn delete(
            &self,
            key: &[u8],
            cond: crate::store::traits::DelCond,
            fence: FenceToken,
        ) -> crate::Result<()> {
            self.inner.delete(key, cond, fence).await
        }

        async fn list_prefix(
            &self,
            prefix: &[u8],
            cursor: Option<Vec<u8>>,
            limit: usize,
        ) -> crate::Result<Page> {
            self.inner.list_prefix(prefix, cursor, limit).await
        }
    }

    struct CountingBlobStore {
        inner: InMemoryBlobStore,
        target_key: Vec<u8>,
        get_blob_count: Arc<AtomicU64>,
    }

    impl BlobStore for CountingBlobStore {
        async fn put_blob(&self, key: &[u8], value: Bytes) -> crate::Result<()> {
            self.inner.put_blob(key, value).await
        }

        async fn put_blob_if_absent(
            &self,
            key: &[u8],
            value: Bytes,
        ) -> crate::Result<crate::store::traits::CreateOutcome> {
            self.inner.put_blob_if_absent(key, value).await
        }

        async fn get_blob(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
            if key == self.target_key.as_slice() {
                self.get_blob_count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_blob(key).await
        }

        async fn delete_blob(&self, key: &[u8]) -> crate::Result<()> {
            self.inner.delete_blob(key).await
        }

        async fn list_prefix(
            &self,
            prefix: &[u8],
            cursor: Option<Vec<u8>>,
            limit: usize,
        ) -> crate::Result<Page> {
            self.inner.list_prefix(prefix, cursor, limit).await
        }
    }

    #[test]
    fn load_stream_entries_falls_back_to_fragments_when_page_blob_is_missing() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let stream = "addr/test/00000000";
            let page_start = 0u32;
            let block_num = 7u64;

            let mut fragment_bitmap = RoaringBitmap::new();
            fragment_bitmap.insert(11);
            let fragment_chunk = ChunkBlob {
                min_local: 11,
                max_local: 11,
                count: 1,
                crc32: 0,
                bitmap: fragment_bitmap,
            };

            meta.put(
                &stream_fragment_meta_key(stream, page_start, block_num),
                encode_stream_bitmap_meta(&StreamBitmapMeta {
                    block_num,
                    count: 1,
                    min_local: 11,
                    max_local: 11,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write stream fragment meta");
            blob.put_blob(
                &stream_fragment_blob_key(stream, page_start, block_num),
                encode_chunk(&fragment_chunk).expect("encode fragment chunk"),
            )
            .await
            .expect("write stream fragment blob");

            meta.put(
                &stream_page_meta_key(stream, page_start),
                encode_stream_bitmap_meta(&StreamBitmapMeta {
                    block_num: 0,
                    count: 1,
                    min_local: 11,
                    max_local: 11,
                }),
                PutCond::Any,
                FenceToken(1),
            )
            .await
            .expect("write stream page meta");

            let entries = load_stream_entries(&meta, &blob, &NoopBytesCache, stream, 0, 20)
                .await
                .expect("load stream entries");

            assert!(entries.contains(11));
            assert_eq!(entries.len(), 1);
        });
    }

    #[test]
    fn load_stream_entries_reuses_cached_stream_page_meta_and_blob() {
        block_on(async {
            let stream = "addr/test/00000000";
            let page_start = 0u32;
            let meta_key = stream_page_meta_key(stream, page_start);
            let blob_key = stream_page_blob_key(stream, page_start);

            let inner_meta = InMemoryMetaStore::default();
            let inner_blob = InMemoryBlobStore::default();
            let meta_gets = Arc::new(AtomicU64::new(0));
            let blob_gets = Arc::new(AtomicU64::new(0));

            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(11);
            let chunk = ChunkBlob {
                min_local: 11,
                max_local: 11,
                count: 1,
                crc32: 0,
                bitmap,
            };

            inner_meta
                .put(
                    &meta_key,
                    encode_stream_bitmap_meta(&StreamBitmapMeta {
                        block_num: 7,
                        count: 1,
                        min_local: 11,
                        max_local: 11,
                    }),
                    PutCond::Any,
                    FenceToken(1),
                )
                .await
                .expect("write stream page meta");
            inner_blob
                .put_blob(
                    &blob_key,
                    encode_chunk(&chunk).expect("encode stream page chunk"),
                )
                .await
                .expect("write stream page blob");

            let meta = CountingMetaStore {
                inner: inner_meta,
                target_key: meta_key.clone(),
                get_count: meta_gets.clone(),
            };
            let blob = CountingBlobStore {
                inner: inner_blob,
                target_key: blob_key.clone(),
                get_blob_count: blob_gets.clone(),
            };
            let cache = HashMapBytesCache::new(BytesCacheConfig {
                stream_page_meta: TableCacheConfig {
                    max_bytes: 4 * 1024,
                },
                stream_page_blobs: TableCacheConfig {
                    max_bytes: 4 * 1024,
                },
                ..BytesCacheConfig::disabled()
            });

            let first = load_stream_entries(&meta, &blob, &cache, stream, 0, 20)
                .await
                .expect("first load");
            let second = load_stream_entries(&meta, &blob, &cache, stream, 0, 20)
                .await
                .expect("second load");

            assert_eq!(first, second);
            assert_eq!(meta_gets.load(Ordering::Relaxed), 1);
            assert_eq!(blob_gets.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn load_stream_entries_can_cache_meta_without_caching_blobs() {
        block_on(async {
            let stream = "addr/test/00000000";
            let page_start = 0u32;
            let meta_key = stream_page_meta_key(stream, page_start);
            let blob_key = stream_page_blob_key(stream, page_start);

            let inner_meta = InMemoryMetaStore::default();
            let inner_blob = InMemoryBlobStore::default();
            let meta_gets = Arc::new(AtomicU64::new(0));
            let blob_gets = Arc::new(AtomicU64::new(0));

            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(11);
            let chunk = ChunkBlob {
                min_local: 11,
                max_local: 11,
                count: 1,
                crc32: 0,
                bitmap,
            };

            inner_meta
                .put(
                    &meta_key,
                    encode_stream_bitmap_meta(&StreamBitmapMeta {
                        block_num: 7,
                        count: 1,
                        min_local: 11,
                        max_local: 11,
                    }),
                    PutCond::Any,
                    FenceToken(1),
                )
                .await
                .expect("write stream page meta");
            inner_blob
                .put_blob(
                    &blob_key,
                    encode_chunk(&chunk).expect("encode stream page chunk"),
                )
                .await
                .expect("write stream page blob");

            let meta = CountingMetaStore {
                inner: inner_meta,
                target_key: meta_key.clone(),
                get_count: meta_gets.clone(),
            };
            let blob = CountingBlobStore {
                inner: inner_blob,
                target_key: blob_key.clone(),
                get_blob_count: blob_gets.clone(),
            };
            let cache = HashMapBytesCache::new(BytesCacheConfig {
                stream_page_meta: TableCacheConfig {
                    max_bytes: 4 * 1024,
                },
                ..BytesCacheConfig::disabled()
            });

            let first = load_stream_entries(&meta, &blob, &cache, stream, 0, 20)
                .await
                .expect("first load");
            let second = load_stream_entries(&meta, &blob, &cache, stream, 0, 20)
                .await
                .expect("second load");

            assert_eq!(first, second);
            assert_eq!(meta_gets.load(Ordering::Relaxed), 1);
            assert_eq!(blob_gets.load(Ordering::Relaxed), 2);
        });
    }
}
