use crate::core::clause::Clause;
use crate::error::Result;
use crate::kernel::sharded_streams::overlaps;
use crate::store::traits::{BlobStore, MetaStore};
use crate::streams::decode_bitmap_blob;
use crate::tables::Tables;

#[derive(Debug, Clone)]
pub(crate) struct StreamSelector {
    pub stream_kind: &'static str,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedClause<K> {
    pub kind: K,
    pub selectors: Vec<StreamSelector>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedClause<K> {
    pub kind: K,
    pub stream_ids: Vec<String>,
    pub estimated_count: u64,
}

pub(crate) trait StreamPlanner {
    type Shard: Copy;
    type ClauseKind: Copy;
    type BitmapMeta;

    fn stream_id(&self, selector: &StreamSelector, shard: Self::Shard) -> String;
    fn clause_sort_rank(&self, kind: Self::ClauseKind) -> u8;
    fn first_page_start(&self, local_from: u32) -> u32;
    fn last_page_start(&self, local_to: u32) -> u32;
    fn next_page_start(&self, page_start: u32) -> u32;
    fn meta_overlaps(&self, meta: &Self::BitmapMeta, local_from: u32, local_to: u32) -> bool;
    fn meta_count(&self, meta: &Self::BitmapMeta) -> u32;

    async fn load_bitmap_page_meta<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream_id: &str,
        page_start: u32,
    ) -> Result<Option<Self::BitmapMeta>>;

    async fn load_page_fragments<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream_id: &str,
        page_start: u32,
    ) -> Result<Vec<bytes::Bytes>>;
}

pub(crate) async fn prepare_shard_clauses<M: MetaStore, B: BlobStore, P: StreamPlanner>(
    tables: &Tables<M, B>,
    planner: &P,
    clause_specs: &[IndexedClause<P::ClauseKind>],
    shard: P::Shard,
    local_from: u32,
    local_to: u32,
) -> Result<Vec<PreparedClause<P::ClauseKind>>> {
    let mut prepared = Vec::with_capacity(clause_specs.len());

    for clause_spec in clause_specs {
        let mut stream_ids = Vec::with_capacity(clause_spec.selectors.len());
        let mut estimated_count = 0u64;

        for selector in &clause_spec.selectors {
            let stream_id = planner.stream_id(selector, shard);
            estimated_count = estimated_count.saturating_add(
                estimate_stream_overlap(tables, planner, &stream_id, local_from, local_to).await?,
            );
            stream_ids.push(stream_id);
        }

        prepared.push(PreparedClause {
            kind: clause_spec.kind,
            stream_ids,
            estimated_count,
        });
    }

    prepared.sort_by_key(|clause| {
        (
            clause.estimated_count,
            planner.clause_sort_rank(clause.kind),
        )
    });
    Ok(prepared)
}

async fn estimate_stream_overlap<M: MetaStore, B: BlobStore, P: StreamPlanner>(
    tables: &Tables<M, B>,
    planner: &P,
    stream_id: &str,
    local_from: u32,
    local_to: u32,
) -> Result<u64> {
    let mut estimated = 0u64;
    let mut page_start = planner.first_page_start(local_from);
    let last_page_start = planner.last_page_start(local_to);

    loop {
        if let Some(meta) = planner
            .load_bitmap_page_meta(tables, stream_id, page_start)
            .await?
        {
            if planner.meta_overlaps(&meta, local_from, local_to) {
                estimated = estimated.saturating_add(u64::from(planner.meta_count(&meta)));
            }
        } else {
            for bytes in planner
                .load_page_fragments(tables, stream_id, page_start)
                .await?
            {
                let meta = decode_bitmap_blob(&bytes)?;
                if overlaps(meta.min_local, meta.max_local, local_from, local_to) {
                    estimated = estimated.saturating_add(u64::from(meta.count));
                }
            }
        }

        if page_start == last_page_start {
            break;
        }
        page_start = planner.next_page_start(page_start);
    }

    Ok(estimated)
}

pub(crate) fn clause_values<T>(clause: &Clause<T>) -> Vec<Vec<u8>>
where
    T: Copy + Into<Vec<u8>>,
{
    match clause {
        Clause::Any => Vec::new(),
        Clause::One(value) => vec![(*value).into()],
        Clause::Or(values) => values.iter().copied().map(Into::into).collect(),
    }
}
