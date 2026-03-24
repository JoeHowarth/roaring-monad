use roaring::RoaringBitmap;

use super::clause::{PreparedShardClause, TracesStreamFamily};
use crate::error::Result;
use crate::query::bitmap::load_prepared_clause_bitmap as load_query_prepared_clause_bitmap;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub(in crate::traces) async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    load_query_prepared_clause_bitmap::<M, B, _, TracesStreamFamily>(
        tables,
        prepared_clause,
        local_from,
        local_to,
    )
    .await
}
