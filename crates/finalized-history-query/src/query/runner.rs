use roaring::RoaringBitmap;

use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::ResolvedBlockRange;
use crate::core::refs::BlockRef;
use crate::error::Result;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::planner::PreparedClause;
use super::types::PrimaryId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatchedQueryItem<I, T> {
    pub id: I,
    pub item: T,
    pub block_ref: BlockRef,
}

pub(crate) trait CandidateLocation: Copy {
    fn block_num(self) -> u64;
    fn local_ordinal(self) -> usize;
}

#[allow(async_fn_in_trait)]
pub(crate) trait QueryMaterializer {
    type Id: PrimaryId;
    type Location: CandidateLocation;
    type Item;
    type Filter;
    type Output;

    async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<Self::Location>>;
    async fn load_run(
        &mut self,
        run: &[(Self::Id, Self::Location)],
    ) -> Result<Vec<(Self::Id, Self::Item)>>;
    async fn block_ref_for(&mut self, item: &Self::Item) -> Result<BlockRef>;
    fn exact_match(&self, item: &Self::Item, filter: &Self::Filter) -> bool;
    fn into_output(item: Self::Item) -> Self::Output;
}

#[allow(async_fn_in_trait)]
pub(crate) trait QueryDescriptor {
    type Id: PrimaryId;
    type Shard: Copy;
    type ClauseKind: Copy;
    type ClauseSpec: Clone;
    type Filter;

    fn build_clause_specs(&self, filter: &Self::Filter) -> Vec<Self::ClauseSpec>;
    fn first_shard_raw(&self, id: Self::Id) -> u64;
    fn last_shard_raw(&self, id: Self::Id) -> u64;
    fn shard_from_raw(&self, shard_raw: u64) -> Self::Shard;
    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard: Self::Shard,
    ) -> (u32, u32);
    fn compose_id(&self, shard: Self::Shard, local_raw: u32) -> Self::Id;

    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard: Self::Shard,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>>;

    async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        prepared_clause: &PreparedClause<Self::ClauseKind>,
        local_from: u32,
        local_to: u32,
    ) -> Result<RoaringBitmap>;
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

pub(crate) async fn execute_indexed_query<M, B, D, Q>(
    tables: &Tables<M, B>,
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

    for shard_raw in
        descriptor.first_shard_raw(from_id)..=descriptor.last_shard_raw(to_id_inclusive)
    {
        let shard = descriptor.shard_from_raw(shard_raw);
        let (local_from, local_to) =
            descriptor.local_range_for_shard(from_id, to_id_inclusive, shard);
        let shard_clauses = descriptor
            .prepare_shard_clauses(tables, &clause_specs, shard, local_from, local_to)
            .await?;

        if shard_clauses.is_empty() {
            continue;
        }

        let mut shard_accumulator: Option<RoaringBitmap> = None;
        for prepared_clause in shard_clauses {
            let clause_bitmap = descriptor
                .load_prepared_clause_bitmap(tables, &prepared_clause, local_from, local_to)
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
            let id = descriptor.compose_id(shard, local_raw);
            let Some(location) = materializer.resolve_id(id).await? else {
                continue;
            };

            let run = collect_contiguous_chunk(
                &mut locals,
                descriptor,
                shard,
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

async fn collect_contiguous_chunk<I, D, Q>(
    locals: &mut std::iter::Peekable<I>,
    descriptor: &D,
    shard: D::Shard,
    first: (D::Id, Q::Location),
    max_len: usize,
    materializer: &mut Q,
) -> Result<Vec<(D::Id, Q::Location)>>
where
    I: Iterator<Item = u32>,
    D: QueryDescriptor,
    Q: QueryMaterializer<Id = D::Id>,
{
    let target_len = max_len.max(1);
    let mut run = vec![first];
    while run.len() < target_len {
        let Some(&next_local_raw) = locals.peek() else {
            break;
        };
        let next_id = descriptor.compose_id(shard, next_local_raw);
        let Some(next_location) = materializer.resolve_id(next_id).await? else {
            let _ = locals.next();
            continue;
        };
        let previous = run.last().expect("run must be non-empty").1;
        if next_location.block_num() != previous.block_num()
            || next_location.local_ordinal() != previous.local_ordinal() + 1
        {
            break;
        }

        let _ = locals.next();
        run.push((next_id, next_location));
    }
    Ok(run)
}

fn remaining_needed_for_chunk(take: usize, matched_len: usize) -> usize {
    take.saturating_sub(matched_len).max(1)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use futures::executor::block_on;
    use roaring::RoaringBitmap;

    use super::{CandidateLocation, QueryDescriptor, QueryMaterializer, collect_contiguous_chunk};
    use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
    use crate::core::refs::BlockRef;
    use crate::error::Result;
    use crate::query::planner::PreparedClause;
    use crate::store::traits::{BlobStore, MetaStore};
    use crate::tables::Tables;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct StubLocation {
        block_num: u64,
        local_ordinal: usize,
    }

    impl CandidateLocation for StubLocation {
        fn block_num(self) -> u64 {
            self.block_num
        }

        fn local_ordinal(self) -> usize {
            self.local_ordinal
        }
    }

    struct StubDescriptor;

    impl QueryDescriptor for StubDescriptor {
        type Id = LogId;
        type Shard = LogShard;
        type ClauseKind = ();
        type ClauseSpec = ();
        type Filter = ();

        fn build_clause_specs(&self, _filter: &Self::Filter) -> Vec<Self::ClauseSpec> {
            Vec::new()
        }

        fn first_shard_raw(&self, id: Self::Id) -> u64 {
            id.shard().get()
        }

        fn last_shard_raw(&self, id: Self::Id) -> u64 {
            id.shard().get()
        }

        fn shard_from_raw(&self, shard_raw: u64) -> Self::Shard {
            LogShard::new(shard_raw).expect("test shard")
        }

        fn local_range_for_shard(
            &self,
            _from: Self::Id,
            _to_inclusive: Self::Id,
            _shard: Self::Shard,
        ) -> (u32, u32) {
            (0, 0)
        }

        fn compose_id(&self, shard: Self::Shard, local_raw: u32) -> Self::Id {
            compose_log_id(shard, LogLocalId::new(local_raw).expect("test local id"))
        }

        async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
            &self,
            _tables: &Tables<M, B>,
            _clause_specs: &[Self::ClauseSpec],
            _shard: Self::Shard,
            _local_from: u32,
            _local_to: u32,
        ) -> Result<Vec<PreparedClause<Self::ClauseKind>>> {
            unreachable!("not used in these tests")
        }

        async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
            &self,
            _tables: &Tables<M, B>,
            _prepared_clause: &PreparedClause<Self::ClauseKind>,
            _local_from: u32,
            _local_to: u32,
        ) -> Result<RoaringBitmap> {
            unreachable!("not used in these tests")
        }
    }

    struct StubMaterializer {
        planned: VecDeque<(LogId, Result<Option<StubLocation>>)>,
    }

    impl QueryMaterializer for StubMaterializer {
        type Id = LogId;
        type Location = StubLocation;
        type Item = ();
        type Filter = ();
        type Output = ();

        async fn resolve_id(&mut self, id: Self::Id) -> Result<Option<Self::Location>> {
            let (expected_id, outcome) = self.planned.pop_front().expect("planned resolution");
            assert_eq!(id, expected_id, "unexpected resolve order");
            outcome
        }

        async fn load_run(
            &mut self,
            _run: &[(Self::Id, Self::Location)],
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
            let descriptor = StubDescriptor;
            let shard = LogShard::new(0).expect("shard");
            let mut locals = vec![1u32, 2u32].into_iter().peekable();
            let first = (
                compose_log_id(shard, LogLocalId::new(0).expect("local")),
                StubLocation {
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
                        Ok(Some(StubLocation {
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

            let chunk = collect_contiguous_chunk(
                &mut locals,
                &descriptor,
                shard,
                first,
                2,
                &mut materializer,
            )
            .await
            .expect("collect chunk");

            assert_eq!(chunk.len(), 2);
            assert_eq!(locals.next(), Some(2));
        });
    }

    #[test]
    fn collect_contiguous_chunk_leaves_remaining_run_for_following_iterations() {
        block_on(async {
            let descriptor = StubDescriptor;
            let shard = LogShard::new(0).expect("shard");
            let mut locals = vec![1u32, 2u32, 3u32].into_iter().peekable();
            let first = (
                compose_log_id(shard, LogLocalId::new(0).expect("local")),
                StubLocation {
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
                        Ok(Some(StubLocation {
                            block_num: 7,
                            local_ordinal: 1,
                        })),
                    ),
                    (
                        fourth_id,
                        Ok(Some(StubLocation {
                            block_num: 7,
                            local_ordinal: 3,
                        })),
                    ),
                ]),
            };

            let first_chunk = collect_contiguous_chunk(
                &mut locals,
                &descriptor,
                shard,
                first,
                2,
                &mut materializer,
            )
            .await
            .expect("first chunk");
            let next_local =
                LogLocalId::new(locals.next().expect("remaining local")).expect("local");
            let second_chunk = collect_contiguous_chunk(
                &mut locals,
                &descriptor,
                shard,
                (
                    compose_log_id(shard, next_local),
                    StubLocation {
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
