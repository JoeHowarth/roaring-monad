use std::collections::BTreeMap;

use bytes::Bytes;

use crate::core::offsets::BucketedOffsets;
use crate::error::{Error, Result};
use crate::ingest::bitmap_pages;
use crate::ingest::indexed_family::{collect_grouped_stream_appends, iter_grouped_stream_appends};
use crate::kernel::sharded_streams::sharded_stream_id;
use crate::store::traits::{BlobStore, MetaStore};
use crate::tables::Tables;
use crate::traces::TRACE_STREAM_PAGE_LOCAL_ID_SPAN;
use crate::traces::ingest_iter::BlockTraceIter;
use crate::traces::types::{Address20, BlockTraceHeader, Selector4};

pub const TRACE_ENCODING_VERSION: u32 = 1;

pub struct TraceIngestPlan {
    pub header: BlockTraceHeader,
    pub block_blob: Bytes,
    pub stream_appends_by_stream: BTreeMap<String, Vec<u32>>,
}

#[derive(Clone, Copy)]
struct TraceStreamFields {
    from_addr: Address20,
    to_addr: Option<Address20>,
    selector: Option<Selector4>,
    has_value: bool,
}

pub async fn persist_trace_artifacts<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    plan: &TraceIngestPlan,
) -> Result<usize> {
    tables
        .block_trace_blobs
        .put_block(block_num, plan.block_blob.clone(), &plan.header)
        .await?;
    Ok(plan.header.trace_count())
}

pub fn plan_trace_ingest(trace_rlp: &[u8], first_trace_id: u64) -> Result<TraceIngestPlan> {
    if trace_rlp.is_empty() {
        return Ok(TraceIngestPlan {
            header: empty_trace_header(),
            block_blob: Bytes::new(),
            stream_appends_by_stream: BTreeMap::new(),
        });
    }

    let mut offsets = BucketedOffsets::new();
    let mut tx_starts = Vec::new();
    let mut flat_blob = Vec::<u8>::new();
    let mut stream_fields = Vec::new();

    for iterated in BlockTraceIter::new(trace_rlp)? {
        let iterated = iterated?;
        if iterated.trace_idx == 0 {
            tx_starts.push(iterated.tx_idx);
        }

        offsets.push(
            u64::try_from(flat_blob.len())
                .map_err(|_| Error::Decode("trace blob offset overflow"))?,
        )?;
        flat_blob.extend_from_slice(iterated.view.frame_bytes);
        let view = iterated.view;

        stream_fields.push(TraceStreamFields {
            from_addr: *view.from_addr()?,
            to_addr: view.to_addr()?.copied(),
            selector: view.selector()?.copied(),
            has_value: view.has_value()?,
        });
    }

    if stream_fields.is_empty() {
        return Ok(TraceIngestPlan {
            header: empty_trace_header(),
            block_blob: Bytes::new(),
            stream_appends_by_stream: BTreeMap::new(),
        });
    }

    offsets.push(
        u64::try_from(flat_blob.len()).map_err(|_| Error::Decode("trace blob size overflow"))?,
    )?;

    Ok(TraceIngestPlan {
        header: BlockTraceHeader {
            encoding_version: TRACE_ENCODING_VERSION,
            offsets,
            tx_starts,
        },
        block_blob: Bytes::from(flat_blob),
        stream_appends_by_stream: collect_trace_stream_appends(first_trace_id, &stream_fields)?,
    })
}

pub async fn persist_trace_stream_fragments<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_num: u64,
    grouped_values: &BTreeMap<String, Vec<u32>>,
) -> Result<Vec<(String, u32)>> {
    bitmap_pages::persist_stream_fragments(
        &tables.trace_streams,
        block_num,
        iter_grouped_stream_appends(grouped_values),
        TRACE_STREAM_PAGE_LOCAL_ID_SPAN,
    )
    .await
}

fn collect_trace_stream_appends(
    first_trace_id: u64,
    stream_fields: &[TraceStreamFields],
) -> Result<BTreeMap<String, Vec<u32>>> {
    collect_grouped_stream_appends(
        first_trace_id,
        stream_fields.iter(),
        |fields, primary_id| {
            let global_trace_id = crate::core::ids::TraceId::new(primary_id);
            let shard = global_trace_id.shard().get();
            let local = global_trace_id.local().get();
            let mut values = Vec::with_capacity(4);

            values.push((sharded_stream_id("from", &fields.from_addr, shard), local));
            if let Some(to_addr) = fields.to_addr {
                values.push((sharded_stream_id("to", &to_addr, shard), local));
            }
            if let Some(selector) = fields.selector {
                values.push((sharded_stream_id("selector", &selector, shard), local));
            }
            if fields.has_value {
                values.push((sharded_stream_id("has_value", b"\x01", shard), local));
            }

            Ok(values)
        },
    )
}

fn empty_trace_header() -> BlockTraceHeader {
    BlockTraceHeader {
        encoding_version: TRACE_ENCODING_VERSION,
        offsets: BucketedOffsets::new(),
        tx_starts: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use alloy_rlp::Encodable;
    use futures::executor::block_on;

    use crate::core::layout::DIRECTORY_SUB_BUCKET_SIZE;
    use crate::kernel::codec::StorageCodec;
    use crate::kernel::table_specs::ScannableTableSpec;
    use crate::kernel::table_specs::u64_key;
    use crate::kernel::table_specs::{BlobTableSpec, PointTableSpec};
    use crate::store::blob::InMemoryBlobStore;
    use crate::store::meta::InMemoryMetaStore;
    use crate::store::traits::BlobStore;
    use crate::traces::table_specs::TraceDirByBlockSpec;
    use crate::traces::table_specs::{BlockTraceBlobSpec, BlockTraceHeaderSpec};
    use crate::traces::types::{BlockTraceHeader, DirByBlock};
    use crate::{store::traits::MetaStore, tables::Tables};

    use super::{persist_trace_artifacts, plan_trace_ingest};

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_field<T: Encodable>(value: T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        out
    }

    fn encode_frame(input: &[u8], seed: u8) -> Vec<u8> {
        let fields = vec![
            encode_field(0u8),
            encode_field(0u64),
            encode_bytes(&[seed; 20]),
            encode_bytes(&[seed + 1; 20]),
            encode_bytes(&[seed]),
            encode_field(100u64),
            encode_field(90u64),
            encode_bytes(input),
            encode_bytes(&[]),
            encode_field(1u8),
            encode_field(0u64),
        ];
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: fields.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for field in fields {
            out.extend_from_slice(&field);
        }
        out
    }

    fn encode_trace_block(txs: Vec<Vec<Vec<u8>>>) -> Vec<u8> {
        let tx_blobs = txs
            .into_iter()
            .map(|frames| {
                let mut tx = Vec::new();
                alloy_rlp::Header {
                    list: true,
                    payload_length: frames.iter().map(Vec::len).sum(),
                }
                .encode(&mut tx);
                for frame in frames {
                    tx.extend_from_slice(&frame);
                }
                tx
            })
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: tx_blobs.iter().map(Vec::len).sum(),
        }
        .encode(&mut out);
        for tx in tx_blobs {
            out.extend_from_slice(&tx);
        }
        out
    }

    #[test]
    fn persist_trace_dir_fragments_write_each_spanned_sub_bucket() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let tables = Tables::without_cache(meta.clone(), InMemoryBlobStore::default());
            let first_trace_id = DIRECTORY_SUB_BUCKET_SIZE - 3;
            let count = 7u32;

            tables
                .trace_dir
                .persist_block_fragment(700, first_trace_id, count)
                .await
                .expect("persist fragments");

            for sub_bucket_start in [0, DIRECTORY_SUB_BUCKET_SIZE] {
                let fragment = meta
                    .scan_get(
                        TraceDirByBlockSpec::TABLE,
                        &u64_key(sub_bucket_start),
                        &u64_key(700),
                    )
                    .await
                    .expect("load directory fragment")
                    .expect("directory fragment present");
                let fragment = DirByBlock::decode(&fragment.value).expect("decode directory");
                assert_eq!(fragment.block_num, 700);
                assert_eq!(fragment.first_primary_id, first_trace_id);
                assert_eq!(
                    fragment.end_primary_id_exclusive,
                    first_trace_id + u64::from(count)
                );
            }
        });
    }

    #[test]
    fn persist_trace_artifacts_flattens_frames_and_writes_sentinel_offsets() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta.clone(), blob.clone());

            let frame0 = encode_frame(&[1, 2, 3, 4], 7);
            let frame1 = encode_frame(&[5, 6, 7, 8], 9);
            let trace_rlp = encode_trace_block(vec![vec![frame0.clone()], vec![frame1.clone()]]);

            let plan = plan_trace_ingest(&trace_rlp, 0).expect("plan trace ingest");
            let trace_count = persist_trace_artifacts(&tables, 700, &plan)
                .await
                .expect("persist trace artifacts");

            assert_eq!(trace_count, 2);

            let stored_blob = blob
                .get_blob(BlockTraceBlobSpec::TABLE, &BlockTraceBlobSpec::key(700))
                .await
                .expect("load blob")
                .expect("trace blob present");
            let mut expected_blob = Vec::new();
            expected_blob.extend_from_slice(&frame0);
            expected_blob.extend_from_slice(&frame1);
            assert_eq!(stored_blob.as_ref(), expected_blob.as_slice());

            let header_bytes = meta
                .get(BlockTraceHeaderSpec::TABLE, &BlockTraceHeaderSpec::key(700))
                .await
                .expect("load header")
                .expect("trace header present");
            let header = BlockTraceHeader::decode(&header_bytes.value).expect("decode header");
            assert_eq!(header.trace_count(), 2);
            assert_eq!(header.offset(0).expect("offset 0"), 0);
            assert_eq!(
                header.offset(1).expect("offset 1"),
                u64::try_from(frame0.len()).expect("frame len"),
            );
            assert_eq!(
                header.offset(2).expect("offset 2"),
                u64::try_from(frame0.len() + frame1.len()).expect("blob len"),
            );
            assert_eq!(header.tx_starts, vec![0, 1]);
        });
    }

    #[test]
    fn trace_payload_loading_does_not_bleed_across_tx_boundaries() {
        block_on(async {
            let meta = InMemoryMetaStore::default();
            let blob = InMemoryBlobStore::default();
            let tables = Tables::without_cache(meta, blob);

            let frame0 = encode_frame(&[1, 2, 3, 4, 5], 7);
            let frame1 = encode_frame(&[9, 8, 7, 6, 5], 9);
            let trace_rlp = encode_trace_block(vec![vec![frame0], vec![frame1]]);
            let plan = plan_trace_ingest(&trace_rlp, 0).expect("plan trace ingest");
            persist_trace_artifacts(&tables, 700, &plan)
                .await
                .expect("persist trace artifacts");

            tables
                .block_records
                .put(
                    700,
                    &crate::core::state::BlockRecord {
                        block_hash: [7u8; 32],
                        parent_hash: [6u8; 32],
                        logs: None,
                        txs: None,
                        traces: Some(crate::core::state::PrimaryWindowRecord {
                            first_primary_id: 0,
                            count: 2,
                        }),
                    },
                )
                .await
                .expect("block record");

            let first = tables
                .block_trace_blobs
                .load_trace_at(700, 0)
                .await
                .expect("load first trace")
                .expect("first trace present");
            let second = tables
                .block_trace_blobs
                .load_trace_at(700, 1)
                .await
                .expect("load second trace")
                .expect("second trace present");

            assert_eq!(first.tx_idx(), 0);
            assert_eq!(first.trace_idx(), 0);
            assert_eq!(
                first.call_frame().input().expect("first input"),
                &[1, 2, 3, 4, 5]
            );
            assert_eq!(second.tx_idx(), 1);
            assert_eq!(second.trace_idx(), 0);
            assert_eq!(
                second.call_frame().input().expect("second input"),
                &[9, 8, 7, 6, 5]
            );
        });
    }
}
