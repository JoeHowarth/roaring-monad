use bytes::Bytes;
use roaring::RoaringBitmap;

use super::clause::PreparedShardClause;
use super::clause::is_full_shard_range;
use crate::error::Result;
use crate::kernel::sharded_streams::overlaps;
use crate::query::bitmap::{
    StreamBitmapLoader, load_prepared_clause_bitmap as load_query_prepared_clause_bitmap,
};
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::keys::TRACE_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::traces::table_specs;

pub(in crate::traces) async fn load_prepared_clause_bitmap<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    prepared_clause: &PreparedShardClause,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    load_query_prepared_clause_bitmap(
        tables,
        &TracesBitmapLoader,
        prepared_clause,
        local_from,
        local_to,
    )
    .await
}

pub(in crate::traces) async fn load_trace_bitmap_page_meta<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
) -> Result<Option<crate::traces::types::StreamBitmapMeta>> {
    tables
        .trace_streams()
        .get_page_meta(stream, page_start)
        .await
}

async fn load_bitmap_page_blob<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    stream: &str,
    page_start: u32,
) -> Result<Option<Bytes>> {
    tables
        .trace_streams()
        .get_page_blob(stream, page_start)
        .await
}

struct TracesBitmapLoader;

impl StreamBitmapLoader for TracesBitmapLoader {
    type BitmapMeta = crate::traces::types::StreamBitmapMeta;

    fn is_full_shard_range(&self, local_from: u32, local_to: u32) -> bool {
        is_full_shard_range(local_from, local_to)
    }

    fn first_page_start(&self, local_from: u32) -> u32 {
        table_specs::stream_page_start_local(local_from)
    }

    fn last_page_start(&self, local_to: u32) -> u32 {
        table_specs::stream_page_start_local(local_to)
    }

    fn next_page_start(&self, page_start: u32) -> u32 {
        page_start.saturating_add(TRACE_STREAM_PAGE_LOCAL_ID_SPAN)
    }

    fn meta_overlaps(&self, meta: &Self::BitmapMeta, local_from: u32, local_to: u32) -> bool {
        overlaps(meta.min_local, meta.max_local, local_from, local_to)
    }

    async fn load_bitmap_page_meta<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream: &str,
        page_start: u32,
    ) -> Result<Option<Self::BitmapMeta>> {
        load_trace_bitmap_page_meta(tables, stream, page_start).await
    }

    async fn load_bitmap_page_blob<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream: &str,
        page_start: u32,
    ) -> Result<Option<Bytes>> {
        load_bitmap_page_blob(tables, stream, page_start).await
    }

    async fn load_page_fragments<M: MetaStore, B: BlobStore>(
        &self,
        tables: &Tables<M, B>,
        stream: &str,
        page_start: u32,
    ) -> Result<Vec<Bytes>> {
        tables
            .trace_streams()
            .load_page_fragments(stream, page_start)
            .await
    }
}
