use crate::codec::log::{decode_block_meta, decode_meta_state, decode_u64};
use crate::config::{BroadQueryPolicy, Config};
use crate::domain::filter::{LogFilter, QueryOptions};
use crate::domain::keys::{META_STATE_KEY, block_hash_to_num_key, block_meta_key};
use crate::domain::types::Log;
use crate::error::{Error, Result};
use crate::query::executor::execute_plan;
use crate::query::planner::{
    QueryPlan, build_clause_order, should_error_too_broad, should_force_block_scan,
};
use crate::store::traits::{BlobStore, MetaStore};

#[derive(Debug, Clone)]
pub struct QueryEngine {
    pub max_or_terms: usize,
    pub broad_query_policy: BroadQueryPolicy,
}

impl QueryEngine {
    pub fn new(max_or_terms: usize) -> Self {
        Self {
            max_or_terms,
            broad_query_policy: BroadQueryPolicy::Error,
        }
    }

    pub fn from_config(config: &Config) -> Self {
        Self {
            max_or_terms: config.planner_max_or_terms,
            broad_query_policy: config.planner_broad_query_policy,
        }
    }

    pub async fn query_finalized<M: MetaStore, B: BlobStore>(
        &self,
        meta_store: &M,
        blob_store: &B,
        filter: LogFilter,
        options: QueryOptions,
    ) -> Result<Vec<Log>> {
        if should_error_too_broad(&filter, self.max_or_terms, self.broad_query_policy) {
            let max_terms = filter.max_or_terms();
            return Err(Error::QueryTooBroad {
                actual: max_terms,
                max: self.max_or_terms,
            });
        }

        let state = match meta_store.get(META_STATE_KEY).await? {
            Some(r) => decode_meta_state(&r.value)?,
            None => return Ok(Vec::new()),
        };
        let finalized_head = state.indexed_finalized_head;

        if filter.block_hash.is_some() && (filter.from_block.is_some() || filter.to_block.is_some())
        {
            return Err(Error::InvalidParams(
                "blockHash cannot be combined with fromBlock/toBlock",
            ));
        }

        if let Some(block_hash) = filter.block_hash {
            let num = match meta_store.get(&block_hash_to_num_key(&block_hash)).await? {
                Some(r) => decode_u64(&r.value)?,
                None => return Err(Error::NotFound),
            };
            let meta = match meta_store.get(&block_meta_key(num)).await? {
                Some(r) => decode_block_meta(&r.value)?,
                None => return Err(Error::NotFound),
            };
            if meta.block_hash != block_hash {
                return Err(Error::NotFound);
            }
            let filter = LogFilter {
                from_block: Some(num),
                to_block: Some(num),
                block_hash: Some(block_hash),
                address: filter.address,
                topic0: filter.topic0,
                topic1: filter.topic1,
                topic2: filter.topic2,
                topic3: filter.topic3,
            };
            let plan = QueryPlan {
                filter,
                options,
                clipped_from_block: num,
                clipped_to_block: num,
                from_log_id: meta.first_log_id,
                to_log_id_inclusive: meta.first_log_id + (meta.count as u64).saturating_sub(1),
                clause_order: Vec::new(),
                force_block_scan: false,
            };
            return execute_plan(meta_store, blob_store, plan).await;
        }

        let from = filter.from_block.unwrap_or(0);
        let to = filter.to_block.unwrap_or(finalized_head);
        if from > finalized_head {
            return Ok(Vec::new());
        }
        let clipped_from = from;
        let clipped_to = to.min(finalized_head);
        if clipped_from > clipped_to {
            return Ok(Vec::new());
        }

        let from_meta = match meta_store.get(&block_meta_key(clipped_from)).await? {
            Some(r) => decode_block_meta(&r.value)?,
            None => return Ok(Vec::new()),
        };
        let to_meta = match meta_store.get(&block_meta_key(clipped_to)).await? {
            Some(r) => decode_block_meta(&r.value)?,
            None => return Ok(Vec::new()),
        };

        let from_log_id = from_meta.first_log_id;
        let to_log_id_inclusive = to_meta.first_log_id + (to_meta.count as u64).saturating_sub(1);
        if from_log_id > to_log_id_inclusive {
            return Ok(Vec::new());
        }

        let force_block_scan =
            should_force_block_scan(&filter, self.max_or_terms, self.broad_query_policy);
        let clause_order = if force_block_scan {
            Vec::new()
        } else {
            build_clause_order(meta_store, &filter, from_log_id, to_log_id_inclusive).await?
        };

        let plan = QueryPlan {
            filter,
            options,
            clipped_from_block: clipped_from,
            clipped_to_block: clipped_to,
            from_log_id,
            to_log_id_inclusive,
            clause_order,
            force_block_scan,
        };

        execute_plan(meta_store, blob_store, plan).await
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self {
            max_or_terms: 128,
            broad_query_policy: BroadQueryPolicy::Error,
        }
    }
}
