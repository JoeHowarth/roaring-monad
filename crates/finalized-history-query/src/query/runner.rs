use std::collections::BTreeMap;
use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::core::directory_resolver::ResolvedPrimaryLocation;
use crate::core::ids::{LogId, TraceId, compose_log_id, compose_trace_id};
use crate::core::layout::MAX_LOCAL_ID;
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{ResolvedBlockRange, load_block_ref};
use crate::core::refs::BlockRef;
use crate::error::{Error, Result};
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::StreamBitmapMeta;
use crate::tables::{StreamTables, Tables};

use super::bitmap::load_prepared_clause_bitmap;
use super::planner::{IndexedClause, prepare_shard_clauses};
pub type ShardBitmapSet = BTreeMap<u64, RoaringBitmap>;

pub struct MaterializerCaches<F> {
    pub directory_fragment_cache: HashMap<u64, Vec<F>>,
    pub block_ref_cache: HashMap<u64, BlockRef>,
}

impl<F> Default for MaterializerCaches<F> {
    fn default() -> Self {
        Self {
            directory_fragment_cache: HashMap::new(),
            block_ref_cache: HashMap::new(),
        }
    }
}

pub trait QueryId: Copy + Ord {
    fn new(raw: u64) -> Self;
    fn get(self) -> u64;
    fn shard_raw(self) -> u64;
    fn local_raw(self) -> u32;
    fn compose(shard_raw: u64, local_raw: u32) -> Self;
}

impl QueryId for LogId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }

    fn shard_raw(self) -> u64 {
        self.shard().get()
    }

    fn local_raw(self) -> u32 {
        self.local().get()
    }

    fn compose(shard_raw: u64, local_raw: u32) -> Self {
        compose_log_id(
            crate::core::ids::LogShard::new(shard_raw).expect("query shard must fit LogShard"),
            crate::core::ids::LogLocalId::new(local_raw)
                .expect("query local id must fit LogLocalId"),
        )
    }
}

impl QueryId for TraceId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }

    fn shard_raw(self) -> u64 {
        self.shard().get()
    }

    fn local_raw(self) -> u32 {
        self.local().get()
    }

    fn compose(shard_raw: u64, local_raw: u32) -> Self {
        compose_trace_id(
            crate::core::ids::TraceShard::new(shard_raw).expect("query shard must fit TraceShard"),
            crate::core::ids::TraceLocalId::new(local_raw)
                .expect("query local id must fit TraceLocalId"),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryIdRange<I> {
    pub start: I,
    pub end_inclusive: I,
}

impl<I: QueryId> QueryIdRange<I> {
    pub fn new(start: I, end_inclusive: I) -> Option<Self> {
        (start <= end_inclusive).then_some(Self {
            start,
            end_inclusive,
        })
    }

    pub fn contains(&self, id: I) -> bool {
        self.start <= id && id <= self.end_inclusive
    }

    pub fn resume_strictly_after(&self, id: I) -> Option<Self> {
        let next_start = id.get().checked_add(1).map(I::new)?;
        Self::new(next_start, self.end_inclusive)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedQueryItem<I, T> {
    pub id: I,
    pub item: T,
    pub block_ref: BlockRef,
}

pub trait CandidateLocation: Copy {
    fn block_num(self) -> u64;
    fn local_ordinal(self) -> usize;
}

#[allow(async_fn_in_trait)]
pub trait QueryMaterializer {
    type Id: QueryId;
    type Item;
    type Filter;
    type Output;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>>;
    async fn load_by_id(&mut self, id: Self::Id) -> Result<Option<Self::Item>> {
        let Some(location) = self.resolve_id(id).await? else {
            return Ok(None);
        };
        Ok(self
            .load_run(&[(id, location)])
            .await?
            .into_iter()
            .next()
            .map(|(_, item)| item))
    }
    async fn load_run(
        &mut self,
        run: &[(Self::Id, ResolvedPrimaryLocation)],
    ) -> Result<Vec<(Self::Id, Self::Item)>>;
    async fn block_ref_for(&mut self, item: &Self::Item) -> Result<BlockRef>;
    fn exact_match(&self, item: &Self::Item, filter: &Self::Filter) -> bool;
    fn into_output(item: Self::Item) -> Self::Output;
}

pub async fn execute_candidates<I, M>(
    clause_sets: Vec<ShardBitmapSet>,
    id_range: QueryIdRange<I>,
    filter: &M::Filter,
    materializer: &mut M,
    take: usize,
) -> Result<Vec<MatchedQueryItem<I, M::Item>>>
where
    I: QueryId,
    M: QueryMaterializer<Id = I>,
{
    let mut out = Vec::new();

    if clause_sets.is_empty() {
        for raw_id in id_range.start.get()..=id_range.end_inclusive.get() {
            let id = I::new(raw_id);
            let Some(item) = materializer.load_by_id(id).await? else {
                continue;
            };
            if !materializer.exact_match(&item, filter) {
                continue;
            }

            let block_ref = materializer.block_ref_for(&item).await?;
            out.push(MatchedQueryItem {
                id,
                item,
                block_ref,
            });
            if out.len() >= take {
                break;
            }
        }
        return Ok(out);
    }

    for (shard_raw, bitmap) in intersect_sets(clause_sets, id_range) {
        let mut locals = bitmap.into_iter().peekable();
        while let Some(local_raw) = locals.next() {
            let id = I::compose(shard_raw, local_raw);
            let Some(location) = materializer.resolve_id(id).await? else {
                continue;
            };

            let run = collect_candidate_chunk(
                &mut locals,
                shard_raw,
                (id, location),
                remaining_needed_for_chunk(take, out.len()),
                materializer,
            )
            .await?;

            for (run_id, item) in materializer.load_run(&run).await? {
                if !materializer.exact_match(&item, filter) {
                    continue;
                }

                let block_ref = materializer.block_ref_for(&item).await?;
                out.push(MatchedQueryItem {
                    id: run_id,
                    item,
                    block_ref,
                });
                if out.len() >= take {
                    return Ok(out);
                }
            }
        }
    }

    Ok(out)
}

#[allow(async_fn_in_trait)]
pub(crate) trait QueryDescriptor {
    type Id: QueryId;
    type Filter;

    fn build_clause_specs(&self, filter: &Self::Filter) -> Vec<IndexedClause>;
    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard_raw: u64,
    ) -> (u32, u32);
}

pub(crate) fn empty_page<T>(block_range: &ResolvedBlockRange) -> QueryPage<T> {
    QueryPage {
        items: Vec::new(),
        meta: QueryPageMeta {
            resolved_from_block: block_range.resolved_from_ref,
            resolved_to_block: block_range.resolved_to_ref,
            cursor_block: block_range.examined_endpoint_ref,
            has_more: false,
            next_resume_id: None,
        },
    }
}

pub(crate) fn build_page<M: QueryMaterializer>(
    block_range: ResolvedBlockRange,
    effective_limit: usize,
    mut matched: Vec<MatchedQueryItem<M::Id, M::Item>>,
) -> QueryPage<M::Output> {
    let has_more = matched.len() > effective_limit;
    if has_more {
        matched.truncate(effective_limit);
    }

    let next_resume_id = if has_more {
        matched.last().map(|matched_item| matched_item.id.get())
    } else {
        None
    };
    let cursor_block = matched
        .last()
        .map(|matched_item| matched_item.block_ref)
        .unwrap_or(block_range.examined_endpoint_ref);
    let items = matched
        .into_iter()
        .map(|matched_item| M::into_output(matched_item.item))
        .collect();

    QueryPage {
        items,
        meta: QueryPageMeta {
            resolved_from_block: block_range.resolved_from_ref,
            resolved_to_block: block_range.resolved_to_ref,
            cursor_block,
            has_more,
            next_resume_id,
        },
    }
}

pub async fn cached_block_ref_with_fallback<M, B, Fut>(
    cache: &mut HashMap<u64, BlockRef>,
    tables: &Tables<M, B>,
    block_num: u64,
    block_hash: [u8; 32],
    fallback_parent_hash: Fut,
) -> Result<BlockRef>
where
    M: MetaStore,
    B: BlobStore,
    Fut: Future<Output = Result<Option<[u8; 32]>>>,
{
    if let Some(block_ref) = cache.get(&block_num).copied() {
        return Ok(block_ref);
    }

    let block_ref = if let Some(block_ref) = load_block_ref(tables, block_num).await? {
        block_ref
    } else {
        let Some(parent_hash) = fallback_parent_hash.await? else {
            return Err(Error::NotFound);
        };
        BlockRef {
            number: block_num,
            hash: block_hash,
            parent_hash,
        }
    };
    cache.insert(block_num, block_ref);
    Ok(block_ref)
}

pub async fn cached_parent_block_ref<M: MetaStore, B: BlobStore>(
    cache: &mut HashMap<u64, BlockRef>,
    tables: &Tables<M, B>,
    block_num: u64,
    block_hash: [u8; 32],
) -> Result<BlockRef> {
    cached_block_ref_with_fallback(cache, tables, block_num, block_hash, async {
        Ok(tables
            .block_records
            .get(block_num)
            .await?
            .map(|record| record.parent_hash))
    })
    .await
}

pub(crate) async fn execute_indexed_query<M, B, D, Q>(
    stream_tables: &StreamTables<M, B, StreamBitmapMeta>,
    descriptor: &D,
    filter: &D::Filter,
    id_window: (D::Id, D::Id),
    take: usize,
    materializer: &mut Q,
) -> Result<Vec<MatchedQueryItem<D::Id, Q::Item>>>
where
    M: MetaStore,
    B: BlobStore,
    D: QueryDescriptor,
    Q: QueryMaterializer<Id = D::Id, Filter = D::Filter>,
{
    let (from_id, to_id_inclusive) = id_window;
    let clause_specs = descriptor.build_clause_specs(filter);
    let mut matched = Vec::new();

    for shard_raw in from_id.shard_raw()..=to_id_inclusive.shard_raw() {
        let (local_from, local_to) =
            descriptor.local_range_for_shard(from_id, to_id_inclusive, shard_raw);
        let shard_clauses = prepare_shard_clauses(
            stream_tables,
            &clause_specs,
            shard_raw,
            local_from,
            local_to,
        )
        .await?;

        if shard_clauses.is_empty() {
            continue;
        }

        let mut shard_accumulator: Option<RoaringBitmap> = None;
        for prepared_clause in shard_clauses {
            let clause_bitmap =
                load_prepared_clause_bitmap(stream_tables, &prepared_clause, local_from, local_to)
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

        let mut locals = shard_accumulator.into_iter().peekable();
        while let Some(local_raw) = locals.next() {
            let id = D::Id::compose(shard_raw, local_raw);
            let Some(location) = materializer.resolve_id(id).await? else {
                continue;
            };

            let run = collect_contiguous_chunk(
                &mut locals,
                shard_raw,
                (id, location),
                remaining_needed_for_chunk(take, matched.len()),
                materializer,
            )
            .await?;

            for (run_id, item) in materializer.load_run(&run).await? {
                if !materializer.exact_match(&item, filter) {
                    continue;
                }

                let block_ref = materializer.block_ref_for(&item).await?;
                matched.push(MatchedQueryItem {
                    id: run_id,
                    item,
                    block_ref,
                });
                if matched.len() >= take {
                    return Ok(matched);
                }
            }
        }
    }

    Ok(matched)
}

async fn collect_contiguous_chunk<Iter, Q>(
    locals: &mut std::iter::Peekable<Iter>,
    shard_raw: u64,
    first: (Q::Id, ResolvedPrimaryLocation),
    max_len: usize,
    materializer: &mut Q,
) -> Result<Vec<(Q::Id, ResolvedPrimaryLocation)>>
where
    Iter: Iterator<Item = u32>,
    Q: QueryMaterializer,
{
    let target_len = max_len.max(1);
    let mut run = vec![first];
    while run.len() < target_len {
        let Some(&next_local_raw) = locals.peek() else {
            break;
        };
        let next_id = Q::Id::compose(shard_raw, next_local_raw);
        let Some(next_location) = materializer.resolve_id(next_id).await? else {
            let _ = locals.next();
            continue;
        };
        let previous = run.last().expect("run must be non-empty").1;
        if next_location.block_num != previous.block_num
            || next_location.local_ordinal != previous.local_ordinal + 1
        {
            break;
        }

        let _ = locals.next();
        run.push((next_id, next_location));
    }
    Ok(run)
}

async fn collect_candidate_chunk<I, Iter, M>(
    locals: &mut std::iter::Peekable<Iter>,
    shard_raw: u64,
    first: (I, ResolvedPrimaryLocation),
    max_len: usize,
    materializer: &mut M,
) -> Result<Vec<(I, ResolvedPrimaryLocation)>>
where
    I: QueryId,
    Iter: Iterator<Item = u32>,
    M: QueryMaterializer<Id = I>,
{
    let target_len = max_len.max(1);
    let mut run = vec![first];
    while run.len() < target_len {
        let Some(&next_local_raw) = locals.peek() else {
            break;
        };
        let next_id = I::compose(shard_raw, next_local_raw);
        let Some(next_location) = materializer.resolve_id(next_id).await? else {
            let _ = locals.next();
            continue;
        };
        let previous = run.last().expect("run must be non-empty").1;
        if next_location.block_num != previous.block_num
            || next_location.local_ordinal != previous.local_ordinal + 1
        {
            break;
        }

        let _ = locals.next();
        run.push((next_id, next_location));
    }
    Ok(run)
}

fn intersect_sets<I: QueryId>(
    sets: Vec<ShardBitmapSet>,
    id_range: QueryIdRange<I>,
) -> ShardBitmapSet {
    let mut it = sets.into_iter();
    let mut acc = it.next().unwrap_or_default();
    for set in it {
        acc.retain(|shard_raw, bitmap| {
            let Some(other) = set.get(shard_raw) else {
                return false;
            };
            *bitmap &= other;
            !bitmap.is_empty()
        });
        if acc.is_empty() {
            break;
        }
    }
    clip_shard_bitmaps_to_range(acc, id_range)
}

fn clip_shard_bitmaps_to_range<I: QueryId>(
    mut bitmaps: ShardBitmapSet,
    id_range: QueryIdRange<I>,
) -> ShardBitmapSet {
    let from_shard = id_range.start.shard_raw();
    let to_shard = id_range.end_inclusive.shard_raw();
    bitmaps.retain(|shard_raw, bitmap| {
        if *shard_raw < from_shard || *shard_raw > to_shard {
            return false;
        }
        if from_shard == to_shard {
            bitmap.remove_range(0..id_range.start.local_raw());
            bitmap.remove_range(
                id_range.end_inclusive.local_raw().saturating_add(1)
                    ..MAX_LOCAL_ID.saturating_add(1),
            );
        } else if *shard_raw == from_shard {
            bitmap.remove_range(0..id_range.start.local_raw());
        } else if *shard_raw == to_shard {
            bitmap.remove_range(
                id_range.end_inclusive.local_raw().saturating_add(1)
                    ..MAX_LOCAL_ID.saturating_add(1),
            );
        }
        !bitmap.is_empty()
    });
    bitmaps
}

fn remaining_needed_for_chunk(take: usize, matched_len: usize) -> usize {
    take.saturating_sub(matched_len).max(1)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures::executor::block_on;

    use super::{QueryMaterializer, collect_contiguous_chunk};
    use crate::core::directory_resolver::ResolvedPrimaryLocation;
    use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
    use crate::core::refs::BlockRef;
    use crate::error::Result;
    struct StubMaterializer {
        planned: VecDeque<(LogId, Result<Option<ResolvedPrimaryLocation>>)>,
    }

    impl QueryMaterializer for StubMaterializer {
        type Id = LogId;
        type Item = ();
        type Filter = ();
        type Output = ();

        async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<ResolvedPrimaryLocation>> {
            let (expected_id, outcome) = self.planned.pop_front().expect("planned resolution");
            assert_eq!(id, expected_id, "unexpected resolve order");
            outcome
        }

        async fn load_run(
            &mut self,
            _run: &[(Self::Id, ResolvedPrimaryLocation)],
        ) -> Result<Vec<(Self::Id, Self::Item)>> {
            unreachable!("not used in these tests")
        }

        async fn block_ref_for(&mut self, _item: &Self::Item) -> Result<BlockRef> {
            unreachable!("not used in these tests")
        }

        fn exact_match(&self, _item: &Self::Item, _filter: &Self::Filter) -> bool {
            unreachable!("not used in these tests")
        }

        fn into_output(_item: Self::Item) -> Self::Output {
            unreachable!("not used in these tests")
        }
    }

    #[test]
    fn collect_contiguous_chunk_does_not_resolve_past_requested_prefix() {
        block_on(async {
            let shard = LogShard::new(0).expect("shard");
            let shard_raw = shard.get();
            let mut locals = vec![1u32, 2u32].into_iter().peekable();
            let first = (
                compose_log_id(shard, LogLocalId::new(0).expect("local")),
                ResolvedPrimaryLocation {
                    block_num: 7,
                    local_ordinal: 0,
                },
            );
            let second_id = compose_log_id(shard, LogLocalId::new(1).expect("local"));
            let third_id = compose_log_id(shard, LogLocalId::new(2).expect("local"));
            let mut materializer = StubMaterializer {
                planned: VecDeque::from([
                    (
                        second_id,
                        Ok(Some(ResolvedPrimaryLocation {
                            block_num: 7,
                            local_ordinal: 1,
                        })),
                    ),
                    (
                        third_id,
                        Err(crate::Error::Backend(
                            "should not resolve third".to_string(),
                        )),
                    ),
                ]),
            };

            let chunk =
                collect_contiguous_chunk(&mut locals, shard_raw, first, 2, &mut materializer)
                    .await
                    .expect("collect chunk");

            assert_eq!(chunk.len(), 2);
            assert_eq!(locals.next(), Some(2));
        });
    }

    #[test]
    fn collect_contiguous_chunk_leaves_remaining_run_for_following_iterations() {
        block_on(async {
            let shard = LogShard::new(0).expect("shard");
            let shard_raw = shard.get();
            let mut locals = vec![1u32, 2u32, 3u32].into_iter().peekable();
            let first = (
                compose_log_id(shard, LogLocalId::new(0).expect("local")),
                ResolvedPrimaryLocation {
                    block_num: 7,
                    local_ordinal: 0,
                },
            );
            let second_id = compose_log_id(shard, LogLocalId::new(1).expect("local"));
            let fourth_id = compose_log_id(shard, LogLocalId::new(3).expect("local"));
            let mut materializer = StubMaterializer {
                planned: VecDeque::from([
                    (
                        second_id,
                        Ok(Some(ResolvedPrimaryLocation {
                            block_num: 7,
                            local_ordinal: 1,
                        })),
                    ),
                    (
                        fourth_id,
                        Ok(Some(ResolvedPrimaryLocation {
                            block_num: 7,
                            local_ordinal: 3,
                        })),
                    ),
                ]),
            };

            let first_chunk =
                collect_contiguous_chunk(&mut locals, shard_raw, first, 2, &mut materializer)
                    .await
                    .expect("first chunk");
            let next_local =
                LogLocalId::new(locals.next().expect("remaining local")).expect("local");
            let second_chunk = collect_contiguous_chunk(
                &mut locals,
                shard_raw,
                (
                    compose_log_id(shard, next_local),
                    ResolvedPrimaryLocation {
                        block_num: 7,
                        local_ordinal: 2,
                    },
                ),
                2,
                &mut materializer,
            )
            .await
            .expect("second chunk");

            assert_eq!(first_chunk.len(), 2);
            assert_eq!(second_chunk.len(), 2);
            assert!(locals.next().is_none());
        });
    }
}
