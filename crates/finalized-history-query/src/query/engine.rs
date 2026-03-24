use std::marker::PhantomData;

use crate::api::{ExecutionBudget, QueryOrder};
use crate::core::ids::{FamilyIdRange, FamilyIdValue};
use crate::core::range::{ResolvedBlockRange, resolve_block_range};
use crate::core::state::load_block_num_by_hash;
use crate::error::{Error, Result};
use crate::query::bitmap::load_prepared_clause_bitmap;
use crate::query::normalized::{effective_limit, normalize_query};
use crate::query::planner::PreparedClause;
use crate::query::runner::{
    QueryDescriptor, QueryId, QueryMaterializer, build_page, empty_page, execute_indexed_query,
};
use crate::query::stream_family::StreamIndexFamily;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub(crate) trait IndexedFilter {
    fn has_indexed_clause(&self) -> bool;
    fn max_or_terms(&self) -> usize;
}

pub(crate) trait IndexedQueryRequest {
    type Filter: IndexedFilter;

    fn start_block_num(&self) -> Option<u64>;
    fn end_block_num(&self) -> Option<u64>;
    fn start_block_hash(&self) -> Option<[u8; 32]>;
    fn end_block_hash(&self) -> Option<[u8; 32]>;
    fn order(&self) -> QueryOrder;
    fn resume_id(&self) -> Option<u64>;
    fn limit(&self) -> usize;
    fn filter(&self) -> &Self::Filter;
}

pub(crate) trait IndexedQueryFamily {
    type Id: QueryId + FamilyIdValue;
    type Shard: Copy;
    type ClauseKind: Copy;
    type ClauseSpec: Clone;
    type Filter: IndexedFilter;
    type Output;
    type StreamFamily: StreamIndexFamily<Shard = Self::Shard, ClauseKind = Self::ClauseKind>;
    type Materializer<'a, M, B>: QueryMaterializer<Id = Self::Id, Filter = Self::Filter, Output = Self::Output>
    where
        M: MetaStore + 'a,
        B: BlobStore + 'a,
        Self: 'a;

    fn build_clause_specs(filter: &Self::Filter) -> Vec<Self::ClauseSpec>;
    fn shard_from_raw(shard_raw: u64) -> Self::Shard;
    fn local_range_for_shard(
        from: Self::Id,
        to_inclusive: Self::Id,
        shard: Self::Shard,
    ) -> (u32, u32);
    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard: Self::Shard,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>>;
    async fn resolve_window<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
        block_range: &ResolvedBlockRange,
    ) -> Result<Option<FamilyIdRange<Self::Id>>>;
    fn make_materializer<'a, M: MetaStore, B: BlobStore>(
        tables: &'a Tables<M, B>,
    ) -> Self::Materializer<'a, M, B>;
    fn indexed_clause_error() -> &'static str;
    fn resume_error() -> &'static str;
}

struct FamilyQueryDescriptor<F>(PhantomData<F>);

impl<F: IndexedQueryFamily> QueryDescriptor for FamilyQueryDescriptor<F> {
    type Id = F::Id;
    type ClauseKind = F::ClauseKind;
    type ClauseSpec = F::ClauseSpec;
    type Filter = F::Filter;

    fn build_clause_specs(&self, filter: &Self::Filter) -> Vec<Self::ClauseSpec> {
        F::build_clause_specs(filter)
    }

    fn local_range_for_shard(
        &self,
        from: Self::Id,
        to_inclusive: Self::Id,
        shard_raw: u64,
    ) -> (u32, u32) {
        F::local_range_for_shard(from, to_inclusive, F::shard_from_raw(shard_raw))
    }

    async fn prepare_shard_clauses<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        clause_specs: &[Self::ClauseSpec],
        shard_raw: u64,
        local_from: u32,
        local_to: u32,
    ) -> Result<Vec<PreparedClause<Self::ClauseKind>>> {
        F::prepare_shard_clauses(
            tables,
            clause_specs,
            F::shard_from_raw(shard_raw),
            local_from,
            local_to,
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
        load_prepared_clause_bitmap::<M, B, _, F::StreamFamily>(
            tables,
            prepared_clause,
            local_from,
            local_to,
        )
        .await
    }
}

pub(crate) async fn resolve_request_block_bounds<
    M: MetaStore,
    B: BlobStore,
    R: IndexedQueryRequest,
>(
    tables: &Tables<M, B>,
    request: &R,
) -> Result<(u64, u64)> {
    let from_block = match (request.start_block_num(), request.start_block_hash()) {
        (Some(number), None) => number,
        (None, Some(hash)) => load_block_num_by_hash(tables, &hash)
            .await?
            .ok_or(Error::InvalidParams("unknown from_block_hash"))?,
        _ => {
            return Err(Error::InvalidParams(
                "exactly one of from_block or from_block_hash is required",
            ));
        }
    };
    let to_block = match (request.end_block_num(), request.end_block_hash()) {
        (Some(number), None) => number,
        (None, Some(hash)) => load_block_num_by_hash(tables, &hash)
            .await?
            .ok_or(Error::InvalidParams("unknown to_block_hash"))?,
        _ => {
            return Err(Error::InvalidParams(
                "exactly one of to_block or to_block_hash is required",
            ));
        }
    };
    Ok((from_block, to_block))
}

pub(crate) async fn execute_family_query<M, P, B, R, F>(
    tables: &Tables<M, B>,
    publication_store: &P,
    request: &R,
    budget: ExecutionBudget,
    max_or_terms: usize,
) -> Result<crate::core::page::QueryPage<F::Output>>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
    R: IndexedQueryRequest<Filter = F::Filter>,
    F: IndexedQueryFamily,
{
    if !request.filter().has_indexed_clause() {
        return Err(Error::InvalidParams(F::indexed_clause_error()));
    }
    if request.filter().max_or_terms() > max_or_terms {
        return Err(Error::QueryTooBroad {
            actual: request.filter().max_or_terms(),
            max: max_or_terms,
        });
    }

    let effective_limit = effective_limit(request.limit(), budget)?;
    let (from_block, to_block) = resolve_request_block_bounds(tables, request).await?;
    let block_range = resolve_block_range(
        tables,
        publication_store,
        from_block,
        to_block,
        request.order(),
    )
    .await?;
    if block_range.is_empty() {
        return Ok(empty_page(&block_range));
    }

    let Some(id_window) = F::resolve_window(tables, &block_range).await? else {
        return Ok(empty_page(&block_range));
    };
    let Some(normalized) = normalize_query(
        &block_range,
        id_window,
        request
            .resume_id()
            .map(<<F as IndexedQueryFamily>::Id as QueryId>::new),
        effective_limit,
        F::resume_error(),
    )?
    else {
        return Ok(empty_page(&block_range));
    };

    let descriptor = FamilyQueryDescriptor::<F>(PhantomData);
    let mut materializer = F::make_materializer(tables);
    let matched = execute_indexed_query(
        tables,
        &descriptor,
        request.filter(),
        (normalized.id_range.start, normalized.id_range.end_inclusive),
        normalized.take,
        &mut materializer,
    )
    .await?;

    Ok(build_page::<F::Materializer<'_, M, B>>(
        normalized.block_range,
        normalized.effective_limit,
        matched,
    ))
}
