use roaring::RoaringBitmap;

use crate::api::{ExecutionBudget, QueryLogsRequest};
use crate::codec::log_ref::LogRef;
use crate::config::Config;
use crate::core::execution::{MatchedPrimary, PrimaryMaterializer};
use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
use crate::core::page::{QueryPage, QueryPageMeta};
use crate::core::range::{RangeResolver, ResolvedBlockRange};
use crate::domain::keys::local_range_for_shard;
use crate::error::{Error, Result};
use crate::logs::filter::LogFilter;
use crate::logs::index_spec::is_too_broad;
use crate::logs::materialize::{LogMaterializer, ResolvedLogLocation};
use crate::logs::types::Log;
use crate::logs::window::LogWindowResolver;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

use super::clause::{build_clause_specs, prepare_shard_clauses};
use super::stream_bitmap::load_prepared_clause_bitmap;

#[allow(async_fn_in_trait)]
trait LogLocationResolver {
    async fn resolve_log_id(&mut self, id: LogId) -> Result<Option<ResolvedLogLocation>>;
}

impl<M: MetaStore, B: BlobStore> LogLocationResolver for LogMaterializer<'_, M, B> {
    async fn resolve_log_id(&mut self, id: LogId) -> Result<Option<ResolvedLogLocation>> {
        LogMaterializer::resolve_log_id(self, id).await
    }
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

    pub async fn query_logs<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
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
                tables,
                publication_store,
                request.from_block,
                request.to_block,
                request.order,
            )
            .await?;
        if block_range.is_empty() {
            return Ok(self.empty_page(&block_range));
        }

        let Some(mut log_window) = self.window_resolver.resolve(tables, &block_range).await? else {
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
                tables,
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

    async fn execute_indexed_query<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        filter: &LogFilter,
        id_window: (LogId, LogId),
        take: usize,
    ) -> Result<Vec<MatchedPrimary<LogRef>>> {
        let (from_log_id, to_log_id_inclusive) = id_window;
        let clause_specs = build_clause_specs(filter);
        let mut matched = Vec::new();
        let mut materializer = LogMaterializer::new(tables);

        for shard_raw in from_log_id.shard().get()..=to_log_id_inclusive.shard().get() {
            let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
            let (local_from, local_to) =
                local_range_for_shard(from_log_id, to_log_id_inclusive, shard);
            let shard_clauses =
                prepare_shard_clauses(tables, &clause_specs, shard, local_from, local_to).await?;

            if shard_clauses.is_empty() {
                continue;
            }

            let mut shard_accumulator: Option<RoaringBitmap> = None;
            for prepared_clause in shard_clauses {
                let clause_bitmap = load_prepared_clause_bitmap(
                    tables,
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

            let mut locals = shard_accumulator.into_iter().peekable();
            while let Some(local_raw) = locals.next() {
                let local = LogLocalId::new(local_raw).expect("bitmap values must fit local-id");
                let id = compose_log_id(shard, local);
                let Some(location) = materializer.resolve_log_id(id).await? else {
                    continue;
                };

                let run = collect_contiguous_chunk(
                    &mut locals,
                    shard,
                    (id, location),
                    remaining_needed_for_chunk(take, matched.len()),
                    &mut materializer,
                )
                .await?;

                let run_items = materializer
                    .load_contiguous_run(
                        location.block_num,
                        run.first().expect("run must be non-empty").1.local_ordinal,
                        run.last().expect("run must be non-empty").1.local_ordinal,
                    )
                    .await?;
                if run_items.len() != run.len() {
                    return Err(Error::NotFound);
                }
                for ((run_id, _), item) in run.into_iter().zip(run_items) {
                    if !materializer.exact_match(&item, filter) {
                        continue;
                    }

                    let block_ref = materializer.block_ref_for(&item).await?;
                    matched.push(MatchedPrimary {
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
}

async fn collect_contiguous_chunk<I, R: LogLocationResolver>(
    locals: &mut std::iter::Peekable<I>,
    shard: LogShard,
    first: (LogId, ResolvedLogLocation),
    max_len: usize,
    materializer: &mut R,
) -> Result<Vec<(LogId, ResolvedLogLocation)>>
where
    I: Iterator<Item = u32>,
{
    let target_len = max_len.max(1);
    let mut run = vec![first];
    while run.len() < target_len {
        let Some(&next_local_raw) = locals.peek() else {
            break;
        };
        let next_local = LogLocalId::new(next_local_raw).expect("bitmap values must fit local-id");
        let next_id = compose_log_id(shard, next_local);
        let Some(next_location) = materializer.resolve_log_id(next_id).await? else {
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

fn remaining_needed_for_chunk(take: usize, matched_len: usize) -> usize {
    take.saturating_sub(matched_len).max(1)
}

#[cfg(test)]
mod tests {
    use super::{ResolvedLogLocation, collect_contiguous_chunk};
    use crate::core::ids::{LogId, LogLocalId, LogShard, compose_log_id};
    use std::collections::VecDeque;

    use futures::executor::block_on;

    struct StubResolver {
        planned: VecDeque<(LogId, crate::Result<Option<ResolvedLogLocation>>)>,
    }

    impl super::LogLocationResolver for StubResolver {
        async fn resolve_log_id(
            &mut self,
            id: LogId,
        ) -> crate::Result<Option<ResolvedLogLocation>> {
            let (expected_id, outcome) = self.planned.pop_front().expect("planned resolution");
            assert_eq!(id, expected_id, "unexpected resolve order");
            outcome
        }
    }

    #[test]
    fn collect_contiguous_chunk_does_not_resolve_past_requested_prefix() {
        block_on(async {
            let shard = LogShard::new(0).expect("shard");
            let mut locals = vec![1u32, 2u32].into_iter().peekable();
            let first = (
                compose_log_id(shard, LogLocalId::new(0).expect("local")),
                ResolvedLogLocation {
                    block_num: 7,
                    local_ordinal: 0,
                },
            );
            let second_id = compose_log_id(shard, LogLocalId::new(1).expect("local"));
            let third_id = compose_log_id(shard, LogLocalId::new(2).expect("local"));
            let mut resolver = StubResolver {
                planned: VecDeque::from([
                    (
                        second_id,
                        Ok(Some(ResolvedLogLocation {
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

            let chunk = collect_contiguous_chunk(&mut locals, shard, first, 2, &mut resolver)
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
            let mut locals = vec![1u32, 2u32, 3u32].into_iter().peekable();
            let first = (
                compose_log_id(shard, LogLocalId::new(0).expect("local")),
                ResolvedLogLocation {
                    block_num: 7,
                    local_ordinal: 0,
                },
            );
            let second_id = compose_log_id(shard, LogLocalId::new(1).expect("local"));
            let fourth_id = compose_log_id(shard, LogLocalId::new(3).expect("local"));
            let mut resolver = StubResolver {
                planned: VecDeque::from([
                    (
                        second_id,
                        Ok(Some(ResolvedLogLocation {
                            block_num: 7,
                            local_ordinal: 1,
                        })),
                    ),
                    (
                        fourth_id,
                        Ok(Some(ResolvedLogLocation {
                            block_num: 7,
                            local_ordinal: 3,
                        })),
                    ),
                ]),
            };

            let first_chunk = collect_contiguous_chunk(&mut locals, shard, first, 2, &mut resolver)
                .await
                .expect("first chunk");
            let next_local =
                LogLocalId::new(locals.next().expect("remaining local")).expect("local");
            let second_chunk = collect_contiguous_chunk(
                &mut locals,
                shard,
                (
                    compose_log_id(shard, next_local),
                    ResolvedLogLocation {
                        block_num: 7,
                        local_ordinal: 2,
                    },
                ),
                2,
                &mut resolver,
            )
            .await
            .expect("second chunk");

            assert_eq!(first_chunk.len(), 2);
            assert_eq!(second_chunk.len(), 2);
            assert!(locals.next().is_none());
        });
    }
}
