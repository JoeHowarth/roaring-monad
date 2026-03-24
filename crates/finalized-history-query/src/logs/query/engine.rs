use super::clause::{
    ClauseKind, IndexedClauseSpec, LogsStreamFamily, build_clause_specs, is_too_broad,
    prepare_shard_clauses,
};
use crate::api::{ExecutionBudget, QueryLogsRequest};
use crate::config::Config;
use crate::core::ids::{LogId, LogLocalId, LogShard};
use crate::core::page::QueryPage;
use crate::core::range::resolve_block_range;
use crate::error::{Error, Result};
use crate::logs::filter::LogFilter;
use crate::logs::materialize::LogMaterializer;
use crate::logs::state::resolve_log_window;
use crate::logs::table_specs;
use crate::logs::types::Log;
use crate::query::bitmap::load_prepared_clause_bitmap;
use crate::query::normalized::{effective_limit, normalize_query};
use crate::query::planner::PreparedClause;
use crate::query::runner::{QueryDescriptor, build_page, empty_page, execute_indexed_query};
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

#[derive(Debug, Clone)]
pub struct LogsQueryEngine {
    max_or_terms: usize,
}

impl LogsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
        }
    }

    pub async fn query_logs<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryLogsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<Log>> {
        if !request.filter.has_indexed_clause() {
            return Err(Error::InvalidParams(
                "query must include at least one indexed address or topic clause",
            ));
        }
        let effective_limit = effective_limit(request.limit, budget)?;

        let block_range = resolve_block_range(
            tables,
            publication_store,
            request.from_block,
            request.to_block,
            request.order,
        )
        .await?;
        if block_range.is_empty() {
            return Ok(empty_page(&block_range));
        }

        let Some(log_window) = resolve_log_window(tables, &block_range).await? else {
            return Ok(empty_page(&block_range));
        };

        if is_too_broad(&request.filter, self.max_or_terms) {
            let actual = request.filter.max_or_terms();
            return Err(Error::QueryTooBroad {
                actual,
                max: self.max_or_terms,
            });
        }

        let Some(normalized) = normalize_query(
            &block_range,
            log_window,
            request.resume_log_id.map(LogId::new),
            effective_limit,
            "resume_log_id outside resolved block window",
        )?
        else {
            return Ok(empty_page(&block_range));
        };
        let descriptor = LogsQueryDescriptor;
        let mut materializer = LogMaterializer::new(tables);
        let matched = execute_indexed_query(
            tables,
            &descriptor,
            &request.filter,
            (normalized.id_range.start, normalized.id_range.end_inclusive),
            normalized.take,
            &mut materializer,
        )
        .await?;

        Ok(build_page::<LogMaterializer<'_, M, B>>(
            normalized.block_range,
            normalized.effective_limit,
            matched,
        ))
    }
}

struct LogsQueryDescriptor;

impl QueryDescriptor for LogsQueryDescriptor {
    type Id = LogId;
    type ClauseKind = ClauseKind;
    type ClauseSpec = IndexedClauseSpec;
    type Filter = LogFilter;

    fn build_clause_specs(&self, filter: &Self::Filter) -> Vec<Self::ClauseSpec> {
        build_clause_specs(filter)
    }

    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard_raw: u64,
    ) -> (u32, u32) {
        let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
        let (local_from, local_to) = table_specs::local_range_for_shard(from, to_inclusive, shard);
        (local_from.get(), local_to.get())
    }

    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard_raw: u64,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>> {
        let shard = LogShard::new(shard_raw).expect("shard derived from LogId range");
        prepare_shard_clauses(
            tables,
            clause_specs,
            shard,
            LogLocalId::new(local_from).expect("local range start must fit local-id"),
            LogLocalId::new(local_to).expect("local range end must fit local-id"),
        )
        .await
    }

    async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        prepared_clause: &PreparedClause<Self::ClauseKind>,
        local_from: u32,
        local_to: u32,
    ) -> Result<roaring::RoaringBitmap> {
        load_prepared_clause_bitmap::<M, B, _, LogsStreamFamily>(
            tables,
            prepared_clause,
            local_from,
            local_to,
        )
        .await
    }
}
