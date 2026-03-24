use crate::api::{ExecutionBudget, QueryOrder};
use crate::core::ids::FamilyIdValue;
use crate::core::range::resolve_block_range;
use crate::error::{Error, Result};
use crate::query::normalized::{effective_limit, plan_page};
use crate::query::planner::IndexedClause;
use crate::query::runner::{QueryMaterializer, build_page, empty_page, execute_indexed_query};
use crate::query::window::resolve_primary_window;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::StreamBitmapMeta;
use crate::tables::{StreamTables, Tables};

pub(crate) trait IndexedFilter {
    fn has_indexed_clause(&self) -> bool;
    fn max_or_terms(&self) -> usize;
    fn indexed_clauses(&self) -> Vec<IndexedClause>;
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

pub(crate) struct QueryLimits {
    pub budget: ExecutionBudget,
    pub max_or_terms: usize,
}

pub(crate) struct FamilyQueryTables<'a, M: MetaStore, B: BlobStore> {
    pub tables: &'a Tables<M, B>,
    pub stream_tables: &'a StreamTables<M, B, StreamBitmapMeta>,
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
        (None, Some(hash)) => tables
            .block_hash_index
            .get(&hash)
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
        (None, Some(hash)) => tables
            .block_hash_index
            .get(&hash)
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

pub(crate) async fn execute_family_query<M, P, B, R, Q, W>(
    family_tables: FamilyQueryTables<'_, M, B>,
    publication_store: &P,
    request: &R,
    limits: QueryLimits,
    materializer: &mut Q,
    select_window: W,
) -> Result<crate::core::page::QueryPage<Q::Output>>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
    R: IndexedQueryRequest,
    Q: QueryMaterializer<Filter = R::Filter>,
    Q::Id: FamilyIdValue,
    W: Fn(&crate::core::state::BlockRecord) -> Option<crate::core::state::PrimaryWindowRecord>,
{
    let tables = family_tables.tables;

    if !request.filter().has_indexed_clause() {
        return Err(Error::InvalidParams(
            "query must include at least one indexed clause",
        ));
    }
    if request.filter().max_or_terms() > limits.max_or_terms {
        return Err(Error::QueryTooBroad {
            actual: request.filter().max_or_terms(),
            max: limits.max_or_terms,
        });
    }

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

    let Some(id_window) =
        resolve_primary_window::<_, _, Q::Id, _>(tables, &block_range, select_window).await?
    else {
        return Ok(empty_page(&block_range));
    };
    let Some(normalized) = plan_page(
        &block_range,
        id_window,
        request.resume_id().map(Q::Id::new),
        effective_limit(request.limit(), limits.budget)?,
        "resume_id outside resolved block window",
    )?
    else {
        return Ok(empty_page(&block_range));
    };

    let matched = execute_indexed_query(
        family_tables.stream_tables,
        request.filter(),
        (normalized.id_range.start, normalized.id_range.end_inclusive),
        normalized.take,
        materializer,
    )
    .await?;

    Ok(build_page::<Q>(
        normalized.block_range,
        normalized.effective_limit,
        matched,
    ))
}
