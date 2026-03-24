use crate::kernel::codec::StorageCodec;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::{StreamTables, Tables};

use super::planner::StreamSelector;

pub(crate) trait StreamIndexFamily {
    type Shard: Copy;
    type ClauseKind: Copy;
    type BitmapMeta: StorageCodec;

    fn stream_tables<M: MetaStore, B: BlobStore>(
        tables: &Tables<M, B>,
    ) -> &StreamTables<M, B, Self::BitmapMeta>;
    fn stream_id(selector: &StreamSelector, shard: Self::Shard) -> String;
    fn clause_sort_rank(kind: Self::ClauseKind) -> u8;
    fn first_page_start(local_from: u32) -> u32;
    fn last_page_start(local_to: u32) -> u32;
    fn next_page_start(page_start: u32) -> u32;
    fn is_full_shard_range(local_from: u32, local_to: u32) -> bool;
    fn meta_overlaps(meta: &Self::BitmapMeta, local_from: u32, local_to: u32) -> bool;
    fn meta_count(meta: &Self::BitmapMeta) -> u32;
}
