use crate::Clause;
use crate::api::{ExecutionBudget, QueryTransactionsRequest};
use crate::config::Config;
use crate::core::ids::TxId;
use crate::core::page::QueryPage;
use crate::core::range::resolve_block_range;
use crate::error::{Error, Result};
use crate::query::bounds::resolve_request_block_bounds;
use crate::query::engine::{FamilyQueryTables, IndexedFilter, QueryLimits, execute_family_query};
use crate::query::normalized::{effective_limit, plan_page};
use crate::query::runner::{MatchedQueryItem, QueryMaterializer, build_page, empty_page};
use crate::query::window::resolve_primary_window;
use crate::store::publication::PublicationStore;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::txs::filter::TxFilter;
use crate::txs::materialize::TxMaterializer;
use crate::txs::view::TxRef;

#[derive(Debug, Clone)]
pub struct TxsQueryEngine {
    max_or_terms: usize,
}

impl TxsQueryEngine {
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
        }
    }

    pub async fn query_transactions<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryTransactionsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<TxRef>> {
        if request.filter.tx_hash.is_some() {
            return self
                .query_transactions_by_hash(tables, publication_store, request, budget)
                .await;
        }

        let mut materializer = TxMaterializer::new(tables);
        execute_family_query(
            FamilyQueryTables {
                tables,
                stream_tables: &tables.tx_streams,
            },
            publication_store,
            &request,
            QueryLimits {
                budget,
                max_or_terms: self.max_or_terms,
            },
            &mut materializer,
            |record| record.txs,
        )
        .await
    }

    async fn query_transactions_by_hash<M: MetaStore, P: PublicationStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        publication_store: &P,
        request: QueryTransactionsRequest,
        budget: ExecutionBudget,
    ) -> Result<QueryPage<TxRef>> {
        if request.filter.max_or_terms() > self.max_or_terms {
            return Err(Error::QueryTooBroad {
                actual: request.filter.max_or_terms(),
                max: self.max_or_terms,
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
        let effective_limit = effective_limit(request.limit, budget)?;
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
            resolve_primary_window::<_, _, TxId, _>(tables, &block_range, |record| record.txs)
                .await?
        else {
            return Ok(empty_page(&block_range));
        };
        let Some(normalized) = plan_page(
            &block_range,
            id_window,
            request.resume_id.map(TxId::new),
            effective_limit,
            "resume_id outside resolved block window",
        )?
        else {
            return Ok(empty_page(&block_range));
        };

        let mut materializer = TxMaterializer::new(tables);
        let mut matched = Vec::<MatchedQueryItem<TxId, TxRef>>::new();
        for tx_hash in tx_hash_values(&request.filter) {
            let Some(location) = tables.tx_hash_index.get(&tx_hash).await? else {
                continue;
            };
            let Some(block_record) = tables.block_records.get(location.block_num).await? else {
                continue;
            };
            let Some(window) = block_record.txs else {
                continue;
            };
            if location.tx_idx >= window.count {
                return Err(Error::Decode("tx_hash_index points past block tx window"));
            }

            let tx_id = TxId::new(
                window
                    .first_primary_id
                    .saturating_add(u64::from(location.tx_idx)),
            );
            if tx_id < normalized.id_range.start || tx_id > normalized.id_range.end_inclusive {
                continue;
            }

            let Some(item) = tables
                .block_tx_blobs
                .load_tx_at(location.block_num, location.tx_idx)
                .await?
            else {
                continue;
            };
            if !materializer.exact_match(&item, &request.filter) {
                continue;
            }

            let block_ref = materializer.block_ref_for(&item).await?;
            matched.push(MatchedQueryItem {
                id: tx_id,
                item,
                block_ref,
            });
        }

        matched.sort_by_key(|item| item.id);
        if matched.len() > normalized.take {
            matched.truncate(normalized.take);
        }

        Ok(build_page::<TxMaterializer<'_, M, B>>(
            normalized.block_range,
            normalized.effective_limit,
            matched,
        ))
    }
}

fn tx_hash_values(filter: &TxFilter) -> Vec<[u8; 32]> {
    match &filter.tx_hash {
        None | Some(Clause::Any) => Vec::new(),
        Some(Clause::One(value)) => vec![*value],
        Some(Clause::Or(values)) => {
            let mut unique = values.clone();
            unique.sort_unstable();
            unique.dedup();
            unique
        }
    }
}
