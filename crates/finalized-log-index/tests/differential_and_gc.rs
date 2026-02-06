use finalized_log_index::api::{FinalizedIndexService, FinalizedLogIndex};
use finalized_log_index::codec::chunk::{ChunkBlob, encode_chunk};
use finalized_log_index::codec::manifest::encode_tail;
use finalized_log_index::config::Config;
use finalized_log_index::domain::filter::{Clause, LogFilter, QueryOptions};
use finalized_log_index::domain::keys::{chunk_blob_key, stream_id, tail_key};
use finalized_log_index::domain::types::{Block, Log};
use finalized_log_index::gc::worker::GcWorker;
use finalized_log_index::recovery::startup::startup_plan;
use finalized_log_index::store::blob::InMemoryBlobStore;
use finalized_log_index::store::meta::InMemoryMetaStore;
use finalized_log_index::store::traits::{BlobStore, FenceToken, MetaStore, PutCond};
use futures::executor::block_on;
use roaring::RoaringBitmap;

fn mk_log(address: u8, topic0: u8, topic1: u8, block_num: u64, tx_idx: u32, log_idx: u32) -> Log {
    Log {
        address: [address; 20],
        topics: vec![[topic0; 32], [topic1; 32]],
        data: vec![address, topic0, topic1],
        block_num,
        tx_idx,
        log_idx,
        block_hash: [block_num as u8; 32],
    }
}

fn mk_block(block_num: u64, parent_hash: [u8; 32], logs: Vec<Log>) -> Block {
    Block {
        block_num,
        block_hash: [block_num as u8; 32],
        parent_hash,
        logs,
    }
}

fn naive_query(blocks: &[Block], filter: &LogFilter, max_results: Option<usize>) -> Vec<Log> {
    let mut out = Vec::new();
    let from = filter.from_block.unwrap_or(0);
    let to = filter.to_block.unwrap_or(u64::MAX);

    for b in blocks {
        if b.block_num < from || b.block_num > to {
            continue;
        }
        if let Some(h) = filter.block_hash
            && h != b.block_hash
        {
            continue;
        }

        for l in &b.logs {
            if !matches_address(l, &filter.address)
                || !matches_topic(l.topics.first().copied(), &filter.topic0)
                || !matches_topic(l.topics.get(1).copied(), &filter.topic1)
                || !matches_topic(l.topics.get(2).copied(), &filter.topic2)
                || !matches_topic(l.topics.get(3).copied(), &filter.topic3)
            {
                continue;
            }
            out.push(l.clone());
            if let Some(limit) = max_results
                && out.len() >= limit
            {
                return out;
            }
        }
    }

    out.sort_by_key(|l| (l.block_num, l.tx_idx, l.log_idx));
    out
}

fn matches_address(log: &Log, clause: &Option<Clause<[u8; 20]>>) -> bool {
    match clause {
        None => true,
        Some(Clause::Any) => true,
        Some(Clause::One(v)) => &log.address == v,
        Some(Clause::Or(vs)) => vs.iter().any(|v| v == &log.address),
    }
}

fn matches_topic(topic: Option<[u8; 32]>, clause: &Option<Clause<[u8; 32]>>) -> bool {
    match clause {
        None => true,
        Some(Clause::Any) => true,
        Some(Clause::One(v)) => topic.as_ref() == Some(v),
        Some(Clause::Or(vs)) => topic
            .as_ref()
            .map(|t| vs.iter().any(|v| v == t))
            .unwrap_or(false),
    }
}

#[test]
fn differential_query_matches_naive() {
    block_on(async {
        let svc = FinalizedIndexService::new(
            Config {
                target_entries_per_chunk: 2,
                planner_max_or_terms: 10,
                ..Config::default()
            },
            InMemoryMetaStore::default(),
            InMemoryBlobStore::default(),
            1,
        );

        let blocks = vec![
            mk_block(
                1,
                [0; 32],
                vec![mk_log(1, 10, 20, 1, 0, 0), mk_log(2, 11, 21, 1, 0, 1)],
            ),
            mk_block(
                2,
                [1; 32],
                vec![mk_log(1, 10, 22, 2, 0, 0), mk_log(3, 12, 23, 2, 0, 1)],
            ),
            mk_block(
                3,
                [2; 32],
                vec![mk_log(2, 10, 24, 3, 0, 0), mk_log(4, 13, 25, 3, 0, 1)],
            ),
        ];

        for b in &blocks {
            svc.ingest_finalized_block(b.clone()).await.expect("ingest");
        }

        let filters = vec![
            LogFilter {
                from_block: Some(1),
                to_block: Some(3),
                block_hash: None,
                address: Some(Clause::Or(vec![[1; 20], [2; 20]])),
                topic0: Some(Clause::One([10; 32])),
                topic1: None,
                topic2: None,
                topic3: None,
            },
            LogFilter {
                from_block: Some(2),
                to_block: Some(3),
                block_hash: None,
                address: None,
                topic0: Some(Clause::Or(vec![[12; 32], [13; 32]])),
                topic1: Some(Clause::Any),
                topic2: None,
                topic3: None,
            },
            LogFilter {
                from_block: Some(1),
                to_block: Some(3),
                block_hash: None,
                address: None,
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
            },
        ];

        for f in filters {
            let got = svc
                .query_finalized(
                    f.clone(),
                    QueryOptions {
                        max_results: Some(3),
                    },
                )
                .await
                .expect("query");
            let want = naive_query(&blocks, &f, Some(3));
            assert_eq!(got, want);
        }
    });
}

#[test]
fn recovery_and_gc_cleanup() {
    block_on(async {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();

        // Create stale tail for stream with no manifest.
        let stale_stream = stream_id("addr", &[0xabu8; 20], 0);
        let mut stale_tail = RoaringBitmap::new();
        stale_tail.insert(7);
        let _ = meta
            .put(
                &tail_key(&stale_stream),
                encode_tail(&stale_tail).expect("tail encode"),
                PutCond::Any,
                FenceToken(0),
            )
            .await
            .expect("put stale tail");

        // Create orphan chunk not referenced by any manifest.
        let mut bm = RoaringBitmap::new();
        bm.insert(9);
        let orphan = ChunkBlob {
            min_local: 9,
            max_local: 9,
            count: 1,
            crc32: 0,
            bitmap: bm,
        };
        let orphan_key = chunk_blob_key("addr/deadbeef/00000000", 1);
        blob.put_blob(&orphan_key, encode_chunk(&orphan).expect("chunk encode"))
            .await
            .expect("put orphan blob");

        let cfg = Config::default();
        let worker = GcWorker::new(&meta, &blob, &cfg);
        let stats = worker.run_once().await.expect("gc run");

        assert_eq!(stats.deleted_stale_tails, 1);
        assert_eq!(stats.deleted_orphan_chunks, 1);

        let rec = startup_plan(&meta, 0).await.expect("startup plan");
        assert_eq!(rec.state.indexed_finalized_head, 0);
    });
}
