use crate::api::{ExecutionBudget, IndexedQueryRequest};
use crate::core::ids::FamilyIdValue;
use crate::core::range::resolve_block_range;
use crate::error::{Error, Result};
use crate::query::bounds::resolve_request_block_bounds;
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

pub(crate) struct QueryLimits {
    pub budget: ExecutionBudget,
    pub max_or_terms: usize,
}

pub(crate) struct FamilyQueryTables<'a, M: MetaStore, B: BlobStore> {
    pub tables: &'a Tables<M, B>,
    pub stream_tables: &'a StreamTables<M, B, StreamBitmapMeta>,
}

/// Drives the shared indexed-query pipeline for one family by resolving block
/// bounds, mapping them to primary IDs, executing bitmap search, and building a page.
pub(crate) async fn execute_family_query<M, P, B, F, Q, W>(
    family_tables: FamilyQueryTables<'_, M, B>,
    publication_store: &P,
    request: &IndexedQueryRequest<F>,
    limits: QueryLimits,
    materializer: &mut Q,
    select_window: W,
) -> Result<crate::core::page::QueryPage<Q::Output>>
where
    M: MetaStore,
    P: PublicationStore,
    B: BlobStore,
    F: IndexedFilter,
    Q: QueryMaterializer<Filter = F>,
    Q::Id: FamilyIdValue,
    W: Fn(&crate::core::state::BlockRecord) -> Option<crate::core::state::PrimaryWindowRecord>,
{
    let tables = family_tables.tables;

    if !request.filter.has_indexed_clause() {
        return Err(Error::InvalidParams(
            "query must include at least one indexed clause",
        ));
    }
    if request.filter.max_or_terms() > limits.max_or_terms {
        return Err(Error::QueryTooBroad {
            actual: request.filter.max_or_terms(),
            max: limits.max_or_terms,
        });
    }

    let (from_block, to_block) = resolve_request_block_bounds(
        tables,
        request.from_block,
        request.to_block,
        request.from_block_hash,
        request.to_block_hash,
    )
    .await?;
    let effective_limit = effective_limit(request.limit, limits.budget)?;
    let block_range = resolve_block_range(
        tables,
        publication_store,
        from_block,
        to_block,
        request.order,
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
        request.resume_id.map(Q::Id::new),
        effective_limit,
        "resume_id outside resolved block window",
    )?
    else {
        return Ok(empty_page(&block_range));
    };

    let matched = execute_indexed_query(
        family_tables.stream_tables,
        &request.filter,
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
