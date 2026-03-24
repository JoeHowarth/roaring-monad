use std::collections::{BTreeMap, BTreeSet};

use crate::core::ids::LogId;
use crate::error::Result;
use crate::family::FinalizedBlock;
use crate::ingest::bitmap_pages;
use crate::logs::keys::STREAM_PAGE_LOCAL_ID_SPAN;
use crate::logs::table_specs;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;

pub fn collect_stream_appends(
    block: &FinalizedBlock,
    first_log_id: u64,
) -> BTreeMap<String, Vec<u32>> {
    let mut out: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();

    for (index, log) in block.logs.iter().enumerate() {
        let global_log_id = LogId::new(first_log_id + index as u64);
        let shard = global_log_id.shard();
        let local = global_log_id.local().get();

        out.entry(table_specs::stream_id("addr", &log.address, shard))
            .or_default()
            .insert(local);

        if let Some(topic0) = log.topics.first() {
            out.entry(table_specs::stream_id("topic0", topic0, shard))
                .or_default()
                .insert(local);
        }

        for (topic_index, topic) in log.topics.iter().enumerate().skip(1).take(3) {
            let kind = match topic_index {
                1 => "topic1",
                2 => "topic2",
                3 => "topic3",
                _ => continue,
            };
            out.entry(table_specs::stream_id(kind, topic, shard))
                .or_default()
                .insert(local);
        }
    }

    out.into_iter()
        .map(|(stream, values)| (stream, values.into_iter().collect()))
        .collect()
}

pub async fn persist_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block: &FinalizedBlock,
    first_log_id: u64,
) -> Result<Vec<(String, u32)>> {
    let grouped_values = collect_stream_appends(block, first_log_id)
        .into_iter()
        .flat_map(|(stream, values)| values.into_iter().map(move |value| (stream.clone(), value)));
    bitmap_pages::persist_stream_fragments(
        tables.log_streams(),
        block.block_num,
        grouped_values,
        STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await
}
