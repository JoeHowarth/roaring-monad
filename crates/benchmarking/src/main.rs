use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use finalized_history_query::api::{
    ExecutionBudget, FinalizedHistoryService, QueryLogsRequest, QueryOrder,
};
use finalized_history_query::config::Config as IndexConfig;
use finalized_history_query::store::minio::MinioBlobStore;
use finalized_history_query::store::publication::MetaPublicationStore;
use finalized_history_query::store::scylla::ScyllaMetaStore;
use finalized_history_query::{
    Clause, EvmBlockHeader, FinalizedBlock, LeaseAuthority, Log, LogFilter,
};
use log_workload_gen::config::GeneratorConfig;
use log_workload_gen::pipeline::run_collect_and_generate;
use log_workload_gen::types::{
    ChainEvent, LogEntry, Message, TraceEntry, TraceProfile as WorkloadTraceProfile,
};
use monad_archive::cli::{ArchiveArgs as ArchiveSinkArgs, BlockDataReaderArgs};
use monad_archive::kvstore::WritePolicy;
use monad_archive::prelude::{
    Block as ArchiveBlock, BlockDataArchive, BlockDataReader, BlockReceipts, FsStorage, LatestKind,
    Metrics,
};
use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(name = "benchmarking", about = "Archive -> ingest -> benchmark runner")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Mirror(MirrorArgs),
    SourceLatest(SourceLatestArgs),
    CollectGenerate(CollectGenerateArgs),
    IngestDistributed(IngestDistributedArgs),
    Benchmark(BenchmarkArgs),
    ScanDensity(ScanDensityArgs),
    RunAll(RunAllArgs),
}

#[derive(Args, Debug, Clone)]
struct RangeArgs {
    #[arg(long)]
    start_block: u64,
    #[arg(long)]
    end_block: u64,
}

#[derive(Args, Debug, Clone)]
struct ArchiveRootArgs {
    #[arg(
        long,
        default_value = "/home/jhow/roaring-monad/data/archive-mainnet-deu-009-0"
    )]
    archive_root: PathBuf,
}

#[derive(Args, Debug, Clone)]
struct MappingArgs {
    #[arg(long, default_value_t = true)]
    rebase_from_start: bool,
    #[arg(long, default_value_t = 1)]
    chain_id: u64,
}

#[derive(Args, Debug, Clone)]
struct MirrorArgs {
    #[command(flatten)]
    archive: ArchiveRootArgs,
    #[command(flatten)]
    range: RangeArgs,

    #[arg(long, default_value = "aws mainnet-deu-009-0 50")]
    source: String,

    #[arg(long, default_value_t = 250)]
    log_every: u64,
}

#[derive(Args, Debug, Clone)]
struct SourceLatestArgs {
    #[arg(long, default_value = "aws mainnet-deu-009-0 50")]
    source: String,

    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
struct CollectGenerateArgs {
    #[command(flatten)]
    archive: ArchiveRootArgs,
    #[command(flatten)]
    range: RangeArgs,
    #[command(flatten)]
    mapping: MappingArgs,

    #[arg(long)]
    dataset_dir: PathBuf,

    #[arg(long, default_value_t = true)]
    clean_dataset_dir: bool,

    #[arg(long, default_value_t = 7)]
    seed: u64,

    #[arg(long, default_value_t = 100_000)]
    trace_size_per_profile: u64,

    #[arg(long, default_value_t = 4096)]
    channel_capacity: usize,

    #[arg(long, default_value_t = 1000)]
    log_every: u64,
}

#[derive(Args, Debug, Clone)]
struct DistributedArgs {
    #[arg(long, default_value = "127.0.0.1:9042")]
    scylla_node: String,

    #[arg(long)]
    scylla_keyspace: String,

    #[arg(long, default_value = "http://127.0.0.1:9000")]
    minio_endpoint: String,

    #[arg(long, default_value = "us-east-1")]
    minio_region: String,

    #[arg(long, default_value = "minioadmin")]
    minio_access_key: String,

    #[arg(long, default_value = "minioadmin")]
    minio_secret_key: String,

    #[arg(long, default_value = "finalized-index-bench")]
    minio_bucket: String,

    #[arg(long)]
    minio_prefix: String,

    #[arg(long, default_value_t = 1)]
    writer_epoch: u64,
}

#[derive(Args, Debug, Clone)]
struct IngestDistributedArgs {
    #[command(flatten)]
    archive: ArchiveRootArgs,
    #[command(flatten)]
    range: RangeArgs,
    #[command(flatten)]
    mapping: MappingArgs,
    #[command(flatten)]
    distributed: DistributedArgs,

    #[arg(long, value_enum, default_value = "strict-cas")]
    ingest_mode: IngestModeArg,

    #[arg(long, default_value_t = false)]
    assume_empty_streams: bool,

    #[arg(long, default_value_t = 256)]
    log_locator_write_concurrency: usize,

    #[arg(long, default_value_t = 96)]
    stream_append_concurrency: usize,

    #[arg(long, default_value_t = 250)]
    log_every: u64,

    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum TraceProfileArg {
    Expected,
    Stress,
    Adversarial,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum IngestModeArg {
    StrictCas,
    SingleWriterFast,
}

#[derive(Args, Debug, Clone)]
struct BenchmarkArgs {
    #[arg(long)]
    dataset_dir: PathBuf,

    #[command(flatten)]
    distributed: DistributedArgs,

    #[arg(long, value_enum, value_delimiter = ',', default_value = "all")]
    profiles: Vec<TraceProfileArg>,

    #[arg(long)]
    trace_limit: Option<usize>,

    #[arg(long, default_value_t = 10_000)]
    max_results: usize,

    #[arg(long, default_value_t = 1_000)]
    log_every: usize,

    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
struct RunAllArgs {
    #[command(flatten)]
    archive: ArchiveRootArgs,
    #[command(flatten)]
    range: RangeArgs,
    #[command(flatten)]
    mapping: MappingArgs,
    #[command(flatten)]
    distributed: DistributedArgs,

    #[arg(long)]
    dataset_dir: PathBuf,

    #[arg(long, default_value = "aws mainnet-deu-009-0 50")]
    source: String,

    #[arg(long, default_value_t = 100_000)]
    trace_size_per_profile: u64,

    #[arg(long, default_value_t = 7)]
    seed: u64,

    #[arg(long, default_value_t = 10_000)]
    max_results: usize,

    #[arg(long, default_value_t = false)]
    skip_mirror: bool,
}

#[derive(Args, Debug, Clone)]
struct ScanDensityArgs {
    #[command(flatten)]
    range: RangeArgs,

    #[arg(long, default_value = "aws mainnet-deu-009-0 50")]
    source: String,

    #[arg(long, default_value_t = 1)]
    step: u64,

    #[arg(long, default_value_t = 50)]
    top: usize,

    #[arg(long, default_value_t = 250)]
    log_every: u64,

    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct DensityEntry {
    block_num: u64,
    txs: u64,
    logs: u64,
}

#[derive(Debug, Serialize)]
struct ScanDensityReport {
    start_block: u64,
    end_block: u64,
    step: u64,
    sampled_blocks: u64,
    avg_logs_per_sample: f64,
    max_logs_in_sample: u64,
    top_blocks: Vec<DensityEntry>,
}

#[derive(Debug, Serialize)]
struct SourceLatestReport {
    source: String,
    latest_uploaded_block: u64,
}

#[derive(Debug, Serialize)]
struct IngestReport {
    start_block: u64,
    end_block: u64,
    mapped_start_block: u64,
    mapped_end_block: u64,
    blocks_ingested: u64,
    logs_ingested: u64,
    elapsed_seconds: f64,
    blocks_per_second: f64,
    logs_per_second: f64,
    scylla_keyspace: String,
    minio_bucket: String,
    minio_prefix: String,
    ingest_mode: String,
    assume_empty_streams: bool,
    log_locator_write_concurrency: usize,
    stream_append_concurrency: usize,
}

#[derive(Debug, Serialize)]
struct LatencyStats {
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
    mean_us: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    traces_total: usize,
    traces_by_profile: BTreeMap<String, usize>,
    total_results: usize,
    elapsed_seconds: f64,
    qps: f64,
    latency_us: LatencyStats,
    scylla_keyspace: String,
    minio_bucket: String,
    minio_prefix: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Mirror(args) => cmd_mirror(args).await,
        Command::SourceLatest(args) => cmd_source_latest(args).await,
        Command::CollectGenerate(args) => cmd_collect_generate(args).await,
        Command::IngestDistributed(args) => cmd_ingest_distributed(args).await,
        Command::Benchmark(args) => cmd_benchmark(args).await,
        Command::ScanDensity(args) => cmd_scan_density(args).await,
        Command::RunAll(args) => cmd_run_all(args).await,
    }
}

async fn cmd_mirror(args: MirrorArgs) -> Result<()> {
    validate_range(&args.range)?;

    fs::create_dir_all(&args.archive.archive_root).with_context(|| {
        format!(
            "create archive root {}",
            args.archive.archive_root.display()
        )
    })?;

    let metrics = Metrics::none();
    let source_args = BlockDataReaderArgs::from_str(args.source.trim())
        .map_err(|e| anyhow::anyhow!("parse block data source '{}': {e}", args.source))?;
    let source = source_args
        .build(&metrics)
        .await
        .map_err(|e| anyhow::anyhow!("build source '{}': {e}", args.source))?;

    let sink_spec = format!("fs {}", args.archive.archive_root.display());
    let sink_args = ArchiveSinkArgs::from_str(&sink_spec)
        .map_err(|e| anyhow::anyhow!("parse archive sink '{sink_spec}': {e}"))?;
    let sink = sink_args
        .build_block_data_archive(&metrics)
        .await
        .map_err(|e| anyhow::anyhow!("build sink '{}': {e}", sink_spec))?;

    let started = Instant::now();
    for block_num in args.range.start_block..=args.range.end_block {
        let block = source
            .get_block_by_number(block_num)
            .await
            .map_err(|e| anyhow::anyhow!("mirror read block {block_num}: {e}"))?;
        let receipts = source
            .get_block_receipts(block_num)
            .await
            .map_err(|e| anyhow::anyhow!("mirror read receipts {block_num}: {e}"))?;
        let traces = source
            .get_block_traces(block_num)
            .await
            .map_err(|e| anyhow::anyhow!("mirror read traces {block_num}: {e}"))?;

        sink.archive_block(block, WritePolicy::NoClobber)
            .await
            .map_err(|e| anyhow::anyhow!("mirror write block {block_num}: {e}"))?;
        sink.archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .map_err(|e| anyhow::anyhow!("mirror write receipts {block_num}: {e}"))?;
        sink.archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .map_err(|e| anyhow::anyhow!("mirror write traces {block_num}: {e}"))?;
        sink.update_latest(block_num, LatestKind::Uploaded)
            .await
            .map_err(|e| anyhow::anyhow!("mirror update latest {block_num}: {e}"))?;

        if block_num == args.range.start_block
            || block_num == args.range.end_block
            || (args.log_every > 0 && block_num.is_multiple_of(args.log_every))
        {
            let elapsed = started.elapsed().as_secs_f64();
            let done = block_num - args.range.start_block + 1;
            let bps = if elapsed > 0.0 {
                done as f64 / elapsed
            } else {
                0.0
            };
            println!(
                "mirror_progress block={block_num} done={done} elapsed_s={elapsed:.2} blocks_per_sec={bps:.2}"
            );
        }
    }

    println!(
        "mirror_done start_block={} end_block={} archive_root={}",
        args.range.start_block,
        args.range.end_block,
        args.archive.archive_root.display()
    );
    Ok(())
}

async fn cmd_source_latest(args: SourceLatestArgs) -> Result<()> {
    let metrics = Metrics::none();
    let source_args = BlockDataReaderArgs::from_str(args.source.trim())
        .map_err(|e| anyhow::anyhow!("parse block data source '{}': {e}", args.source))?;
    let source = source_args
        .build(&metrics)
        .await
        .map_err(|e| anyhow::anyhow!("build source '{}': {e}", args.source))?;

    let latest_uploaded_block = source
        .get_latest(LatestKind::Uploaded)
        .await
        .map_err(|e| anyhow::anyhow!("source latest uploaded lookup: {e}"))?
        .unwrap_or(0);

    let report = SourceLatestReport {
        source: args.source,
        latest_uploaded_block,
    };

    // Keep stdout machine-readable for scripts.
    println!("{}", report.latest_uploaded_block);
    if let Some(path) = &args.output_json {
        write_json(path, &report)?;
    }

    Ok(())
}

async fn cmd_collect_generate(args: CollectGenerateArgs) -> Result<()> {
    validate_range(&args.range)?;

    if args.clean_dataset_dir && args.dataset_dir.exists() {
        fs::remove_dir_all(&args.dataset_dir)
            .with_context(|| format!("remove dataset dir {}", args.dataset_dir.display()))?;
    }

    let archive = open_fs_archive(&args.archive.archive_root)?;
    let mapper = BlockMapper::new(args.range.start_block, args.mapping.rebase_from_start);

    let (tx, rx) = mpsc::channel(args.channel_capacity);
    let producer_archive = archive.clone();
    let producer_args = args.clone();
    let producer = tokio::spawn(async move {
        let mut blocks_seen = 0u64;
        let mut logs_seen = 0u64;

        for raw_block_num in producer_args.range.start_block..=producer_args.range.end_block {
            let block = producer_archive
                .get_block_by_number(raw_block_num)
                .await
                .map_err(|e| anyhow::anyhow!("collect read block {raw_block_num}: {e}"))?;
            let receipts = producer_archive
                .get_block_receipts(raw_block_num)
                .await
                .map_err(|e| anyhow::anyhow!("collect read receipts {raw_block_num}: {e}"))?;

            let mapped_block_num = mapper.map_block_number(raw_block_num)?;
            let event = to_chain_event(
                producer_args.mapping.chain_id,
                mapped_block_num,
                &block,
                &receipts,
            )?;

            logs_seen = logs_seen.saturating_add(event.logs.len() as u64);
            blocks_seen = blocks_seen.saturating_add(1);

            tx.send(Message::ChainEvent(event))
                .await
                .context("send chain event")?;

            if raw_block_num == producer_args.range.start_block
                || raw_block_num == producer_args.range.end_block
                || raw_block_num % producer_args.log_every == 0
            {
                println!(
                    "collect_progress raw_block={} mapped_block={} blocks_seen={} logs_seen={}",
                    raw_block_num, mapped_block_num, blocks_seen, logs_seen
                );
            }
        }

        tx.send(Message::EndOfStream {
            expected_end_block: mapper.map_block_number(producer_args.range.end_block)?,
        })
        .await
        .context("send end of stream")?;

        Result::<(u64, u64)>::Ok((blocks_seen, logs_seen))
    });

    let config = GeneratorConfig {
        trace_size_per_profile: args.trace_size_per_profile,
        ..GeneratorConfig::default()
    };

    let started = Instant::now();
    let trace_summary =
        run_collect_and_generate(config.clone(), rx, &args.dataset_dir, args.seed).await?;
    let elapsed = started.elapsed().as_secs_f64();

    let (blocks_seen, logs_seen) = producer.await.context("join producer")??;

    println!(
        "collect_generate_done blocks_seen={} logs_seen={} trace_expected={} trace_stress={} trace_adversarial={} elapsed_s={:.2} dataset_dir={}",
        blocks_seen,
        logs_seen,
        trace_summary.expected,
        trace_summary.stress,
        trace_summary.adversarial,
        elapsed,
        args.dataset_dir.display()
    );
    Ok(())
}

async fn cmd_ingest_distributed(args: IngestDistributedArgs) -> Result<()> {
    validate_range(&args.range)?;

    if !args.mapping.rebase_from_start && args.range.start_block != 1 {
        bail!(
            "ingest without rebasing requires start_block=1, got {}",
            args.range.start_block
        );
    }

    let archive = open_fs_archive(&args.archive.archive_root)?;
    let mapper = BlockMapper::new(args.range.start_block, args.mapping.rebase_from_start);

    let meta = ScyllaMetaStore::new(
        std::slice::from_ref(&args.distributed.scylla_node),
        &args.distributed.scylla_keyspace,
    )
    .await
    .with_context(|| {
        format!(
            "connect scylla node={} keyspace={}",
            args.distributed.scylla_node, args.distributed.scylla_keyspace
        )
    })?;
    meta.set_min_epoch(args.distributed.writer_epoch)
        .await
        .context("set scylla min epoch")?;

    let blob = MinioBlobStore::new(
        &args.distributed.minio_endpoint,
        &args.distributed.minio_region,
        &args.distributed.minio_access_key,
        &args.distributed.minio_secret_key,
        &args.distributed.minio_bucket,
        &args.distributed.minio_prefix,
    )
    .await
    .with_context(|| {
        format!(
            "connect minio endpoint={} bucket={} prefix={}",
            args.distributed.minio_endpoint,
            args.distributed.minio_bucket,
            args.distributed.minio_prefix
        )
    })?;

    let config = IndexConfig {
        assume_empty_streams: args.assume_empty_streams,
        stream_append_concurrency: args.stream_append_concurrency.max(1),
        ..IndexConfig::default()
    };

    let svc = FinalizedHistoryService::new_reader_writer(
        IndexConfig {
            observe_upstream_finalized_block: Arc::new(move || Some(args.range.end_block)),
            ..config
        },
        meta,
        blob,
        args.distributed.writer_epoch,
    );

    let started = Instant::now();
    let mut blocks_ingested = 0u64;
    let mut logs_ingested = 0u64;

    for raw_block_num in args.range.start_block..=args.range.end_block {
        let block = archive
            .get_block_by_number(raw_block_num)
            .await
            .map_err(|e| anyhow::anyhow!("read block {raw_block_num} from fs archive: {e}"))?;
        let receipts = archive
            .get_block_receipts(raw_block_num)
            .await
            .map_err(|e| anyhow::anyhow!("read receipts {raw_block_num} from fs archive: {e}"))?;

        let mapped_block = to_index_block(raw_block_num, &block, &receipts, &mapper)?;
        let outcome = svc
            .ingest_finalized_block(mapped_block)
            .await
            .with_context(|| format!("ingest mapped block from raw_block={raw_block_num}"))?;

        blocks_ingested = blocks_ingested.saturating_add(1);
        logs_ingested = logs_ingested.saturating_add(outcome.written_logs as u64);

        if raw_block_num == args.range.start_block
            || raw_block_num == args.range.end_block
            || raw_block_num % args.log_every == 0
        {
            let elapsed = started.elapsed().as_secs_f64();
            let bps = if elapsed > 0.0 {
                blocks_ingested as f64 / elapsed
            } else {
                0.0
            };
            let lps = if elapsed > 0.0 {
                logs_ingested as f64 / elapsed
            } else {
                0.0
            };
            println!(
                "ingest_progress raw_block={} mapped_block={} blocks_ingested={} logs_ingested={} elapsed_s={elapsed:.2} blocks_per_sec={bps:.2} logs_per_sec={lps:.2}",
                raw_block_num,
                mapper.map_block_number(raw_block_num)?,
                blocks_ingested,
                logs_ingested
            );
        }
    }

    let elapsed_seconds = started.elapsed().as_secs_f64();
    let blocks_per_second = if elapsed_seconds > 0.0 {
        blocks_ingested as f64 / elapsed_seconds
    } else {
        0.0
    };
    let logs_per_second = if elapsed_seconds > 0.0 {
        logs_ingested as f64 / elapsed_seconds
    } else {
        0.0
    };

    let report = IngestReport {
        start_block: args.range.start_block,
        end_block: args.range.end_block,
        mapped_start_block: mapper.map_block_number(args.range.start_block)?,
        mapped_end_block: mapper.map_block_number(args.range.end_block)?,
        blocks_ingested,
        logs_ingested,
        elapsed_seconds,
        blocks_per_second,
        logs_per_second,
        scylla_keyspace: args.distributed.scylla_keyspace.clone(),
        minio_bucket: args.distributed.minio_bucket.clone(),
        minio_prefix: args.distributed.minio_prefix.clone(),
        ingest_mode: match args.ingest_mode {
            IngestModeArg::StrictCas => "strict_cas".to_string(),
            IngestModeArg::SingleWriterFast => "single_writer_fast".to_string(),
        },
        assume_empty_streams: args.assume_empty_streams,
        log_locator_write_concurrency: args.log_locator_write_concurrency.max(1),
        stream_append_concurrency: args.stream_append_concurrency.max(1),
    };

    println!(
        "ingest_done blocks_ingested={} logs_ingested={} elapsed_s={:.2} blocks_per_sec={:.2} logs_per_sec={:.2}",
        report.blocks_ingested,
        report.logs_ingested,
        report.elapsed_seconds,
        report.blocks_per_second,
        report.logs_per_second
    );

    if let Some(path) = &args.output_json {
        write_json(path, &report)?;
    }

    Ok(())
}

async fn cmd_benchmark(args: BenchmarkArgs) -> Result<()> {
    let mut traces = load_traces(&args.dataset_dir, &args.profiles)?;
    if let Some(limit) = args.trace_limit
        && traces.len() > limit
    {
        traces.truncate(limit);
    }
    if traces.is_empty() {
        bail!("no traces loaded from {}", args.dataset_dir.display());
    }

    let config = IndexConfig::default();
    let svc = connect_service(&args.distributed, config).await?;

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(traces.len());
    let mut total_results = 0usize;
    let mut profile_counts: BTreeMap<String, usize> = BTreeMap::new();

    for (idx, trace) in traces.iter().enumerate() {
        *profile_counts
            .entry(trace_profile_name(trace.profile.clone()).to_string())
            .or_default() += 1;

        let request = trace_to_request(trace, args.max_results)?;
        let q_start = Instant::now();
        let out = svc.query_logs(request, ExecutionBudget::default()).await?;
        latencies.push(q_start.elapsed().as_micros());
        total_results = total_results.saturating_add(out.items.len());

        let done = idx + 1;
        if args.log_every > 0 && (done == traces.len() || done % args.log_every == 0) {
            let elapsed = started.elapsed().as_secs_f64();
            let qps_so_far = if elapsed > 0.0 {
                done as f64 / elapsed
            } else {
                0.0
            };
            println!(
                "benchmark_progress traces_done={} traces_total={} elapsed_s={elapsed:.2} qps_so_far={qps_so_far:.2}",
                done,
                traces.len()
            );
        }
    }

    latencies.sort_unstable();
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let qps = if elapsed_seconds > 0.0 {
        traces.len() as f64 / elapsed_seconds
    } else {
        0.0
    };

    let report = BenchmarkReport {
        traces_total: traces.len(),
        traces_by_profile: profile_counts,
        total_results,
        elapsed_seconds,
        qps,
        latency_us: LatencyStats {
            p50_us: percentile(&latencies, 0.50),
            p95_us: percentile(&latencies, 0.95),
            p99_us: percentile(&latencies, 0.99),
            max_us: latencies.last().copied().unwrap_or(0),
            mean_us: mean(&latencies),
        },
        scylla_keyspace: args.distributed.scylla_keyspace.clone(),
        minio_bucket: args.distributed.minio_bucket.clone(),
        minio_prefix: args.distributed.minio_prefix.clone(),
    };

    println!(
        "benchmark_done traces_total={} total_results={} elapsed_s={:.2} qps={:.2} p50_us={} p95_us={} p99_us={} max_us={}",
        report.traces_total,
        report.total_results,
        report.elapsed_seconds,
        report.qps,
        report.latency_us.p50_us,
        report.latency_us.p95_us,
        report.latency_us.p99_us,
        report.latency_us.max_us
    );

    if let Some(path) = &args.output_json {
        write_json(path, &report)?;
    }

    Ok(())
}

async fn cmd_run_all(args: RunAllArgs) -> Result<()> {
    if !args.skip_mirror {
        cmd_mirror(MirrorArgs {
            archive: args.archive.clone(),
            range: args.range.clone(),
            source: args.source.clone(),
            log_every: 250,
        })
        .await?;
    }

    cmd_collect_generate(CollectGenerateArgs {
        archive: args.archive.clone(),
        range: args.range.clone(),
        mapping: args.mapping.clone(),
        dataset_dir: args.dataset_dir.clone(),
        clean_dataset_dir: true,
        seed: args.seed,
        trace_size_per_profile: args.trace_size_per_profile,
        channel_capacity: 4096,
        log_every: 1000,
    })
    .await?;

    cmd_ingest_distributed(IngestDistributedArgs {
        archive: args.archive.clone(),
        range: args.range.clone(),
        mapping: args.mapping.clone(),
        distributed: args.distributed.clone(),
        ingest_mode: IngestModeArg::StrictCas,
        assume_empty_streams: false,
        log_locator_write_concurrency: 256,
        stream_append_concurrency: 96,
        log_every: 250,
        output_json: None,
    })
    .await?;

    cmd_benchmark(BenchmarkArgs {
        dataset_dir: args.dataset_dir,
        distributed: args.distributed,
        profiles: vec![TraceProfileArg::All],
        trace_limit: None,
        max_results: args.max_results,
        log_every: 1000,
        output_json: None,
    })
    .await
}

async fn cmd_scan_density(args: ScanDensityArgs) -> Result<()> {
    validate_range(&args.range)?;
    if args.step == 0 {
        bail!("step must be > 0");
    }
    if args.top == 0 {
        bail!("top must be > 0");
    }

    let metrics = Metrics::none();
    let source_args = BlockDataReaderArgs::from_str(args.source.trim())
        .map_err(|e| anyhow::anyhow!("parse block data source '{}': {e}", args.source))?;
    let source = source_args
        .build(&metrics)
        .await
        .map_err(|e| anyhow::anyhow!("build source '{}': {e}", args.source))?;

    let mut sampled_blocks = 0u64;
    let mut total_logs = 0u64;
    let mut max_logs_in_sample = 0u64;
    let mut top_blocks = Vec::<DensityEntry>::new();

    let mut block_num = args.range.start_block;
    while block_num <= args.range.end_block {
        let receipts = source
            .get_block_receipts(block_num)
            .await
            .map_err(|e| anyhow::anyhow!("scan read receipts {block_num}: {e}"))?;

        let logs = receipts
            .iter()
            .map(|r| r.receipt.logs().len() as u64)
            .sum::<u64>();
        let txs = receipts.len() as u64;

        sampled_blocks = sampled_blocks.saturating_add(1);
        total_logs = total_logs.saturating_add(logs);
        max_logs_in_sample = max_logs_in_sample.max(logs);

        top_blocks.push(DensityEntry {
            block_num,
            txs,
            logs,
        });
        top_blocks.sort_by(|a, b| b.logs.cmp(&a.logs).then_with(|| b.txs.cmp(&a.txs)));
        if top_blocks.len() > args.top {
            top_blocks.truncate(args.top);
        }

        if block_num == args.range.start_block
            || block_num == args.range.end_block
            || (args.log_every > 0 && block_num.is_multiple_of(args.log_every))
        {
            let avg_logs = if sampled_blocks > 0 {
                total_logs as f64 / sampled_blocks as f64
            } else {
                0.0
            };
            println!(
                "scan_density_progress block={block_num} sampled_blocks={sampled_blocks} avg_logs={avg_logs:.2} max_logs={max_logs_in_sample}"
            );
        }

        if args.range.end_block - block_num < args.step {
            break;
        }
        block_num += args.step;
    }

    let report = ScanDensityReport {
        start_block: args.range.start_block,
        end_block: args.range.end_block,
        step: args.step,
        sampled_blocks,
        avg_logs_per_sample: if sampled_blocks > 0 {
            total_logs as f64 / sampled_blocks as f64
        } else {
            0.0
        },
        max_logs_in_sample,
        top_blocks,
    };

    println!(
        "scan_density_done sampled_blocks={} avg_logs={:.2} max_logs={} top_count={}",
        report.sampled_blocks,
        report.avg_logs_per_sample,
        report.max_logs_in_sample,
        report.top_blocks.len()
    );
    for entry in &report.top_blocks {
        println!(
            "scan_density_top block={} txs={} logs={}",
            entry.block_num, entry.txs, entry.logs
        );
    }

    if let Some(path) = &args.output_json {
        write_json(path, &report)?;
    }
    Ok(())
}

fn validate_range(range: &RangeArgs) -> Result<()> {
    if range.start_block > range.end_block {
        bail!(
            "invalid block range: start_block={} > end_block={}",
            range.start_block,
            range.end_block
        );
    }
    Ok(())
}

fn open_fs_archive(root: &Path) -> Result<BlockDataArchive> {
    let blocks_root = root.join("blocks");
    let storage = FsStorage::new(blocks_root.clone(), Metrics::none())
        .map_err(|e| anyhow::anyhow!("open fs archive store {}: {e}", blocks_root.display()))?;
    Ok(BlockDataArchive::new(storage))
}

async fn connect_service(
    args: &DistributedArgs,
    config: IndexConfig,
) -> Result<
    FinalizedHistoryService<
        LeaseAuthority<MetaPublicationStore<ScyllaMetaStore>>,
        ScyllaMetaStore,
        MinioBlobStore,
    >,
> {
    let meta = ScyllaMetaStore::new(
        std::slice::from_ref(&args.scylla_node),
        &args.scylla_keyspace,
    )
    .await
    .with_context(|| {
        format!(
            "connect scylla node={} keyspace={}",
            args.scylla_node, args.scylla_keyspace
        )
    })?;

    let blob = MinioBlobStore::new(
        &args.minio_endpoint,
        &args.minio_region,
        &args.minio_access_key,
        &args.minio_secret_key,
        &args.minio_bucket,
        &args.minio_prefix,
    )
    .await
    .with_context(|| {
        format!(
            "connect minio endpoint={} bucket={} prefix={}",
            args.minio_endpoint, args.minio_bucket, args.minio_prefix
        )
    })?;

    Ok(FinalizedHistoryService::new_reader_writer(
        IndexConfig {
            observe_upstream_finalized_block: Arc::new(|| Some(u64::MAX / 4)),
            ..config
        },
        meta,
        blob,
        args.writer_epoch,
    ))
}

#[derive(Clone, Copy)]
struct BlockMapper {
    start_block: u64,
    rebase_from_start: bool,
}

impl BlockMapper {
    fn new(start_block: u64, rebase_from_start: bool) -> Self {
        Self {
            start_block,
            rebase_from_start,
        }
    }

    fn map_block_number(&self, raw_block_num: u64) -> Result<u64> {
        if self.rebase_from_start {
            if raw_block_num < self.start_block {
                bail!(
                    "raw block {} is below mapping start {}",
                    raw_block_num,
                    self.start_block
                );
            }
            Ok(raw_block_num - self.start_block + 1)
        } else {
            Ok(raw_block_num)
        }
    }

    fn map_parent_hash(&self, raw_block_num: u64, parent_hash: [u8; 32]) -> [u8; 32] {
        if self.rebase_from_start && raw_block_num == self.start_block {
            [0u8; 32]
        } else {
            parent_hash
        }
    }
}

fn to_chain_event(
    chain_id: u64,
    mapped_block_num: u64,
    block: &ArchiveBlock,
    receipts: &BlockReceipts,
) -> Result<ChainEvent> {
    let block_hash = fixed32(block.header.hash_slow().as_slice())?;

    let mut logs = Vec::<LogEntry>::new();
    for (tx_idx, receipt) in receipts.iter().enumerate() {
        for (inner_log_idx, log) in receipt.receipt.logs().iter().enumerate() {
            let log_index_u64 = receipt
                .starting_log_index
                .saturating_add(inner_log_idx as u64);
            let log_index = u32::try_from(log_index_u64)
                .with_context(|| format!("log index overflow: {log_index_u64}"))?;
            let tx_index =
                u32::try_from(tx_idx).with_context(|| format!("tx index overflow: {tx_idx}"))?;

            logs.push(LogEntry {
                tx_index,
                log_index,
                address: fixed20(log.address.as_slice())?,
                topics: log
                    .topics()
                    .iter()
                    .map(|topic| fixed32(topic.as_slice()))
                    .collect::<Result<Vec<_>>>()?,
            });
        }
    }

    Ok(ChainEvent {
        chain_id,
        block_number: mapped_block_num,
        block_hash,
        timestamp: block.header.timestamp,
        logs,
    })
}

fn to_index_block(
    raw_block_num: u64,
    block: &ArchiveBlock,
    receipts: &BlockReceipts,
    mapper: &BlockMapper,
) -> Result<FinalizedBlock> {
    let mapped_block_num = mapper.map_block_number(raw_block_num)?;
    let block_hash = fixed32(block.header.hash_slow().as_slice())?;
    let parent_hash =
        mapper.map_parent_hash(raw_block_num, fixed32(block.header.parent_hash.as_slice())?);

    let mut logs = Vec::<Log>::new();
    for (tx_idx, receipt) in receipts.iter().enumerate() {
        for (inner_log_idx, log) in receipt.receipt.logs().iter().enumerate() {
            let log_idx_u64 = receipt
                .starting_log_index
                .saturating_add(inner_log_idx as u64);
            let log_idx = u32::try_from(log_idx_u64)
                .with_context(|| format!("log index overflow: {log_idx_u64}"))?;
            let tx_idx_u32 =
                u32::try_from(tx_idx).with_context(|| format!("tx index overflow: {tx_idx}"))?;

            logs.push(Log {
                address: fixed20(log.address.as_slice())?,
                topics: log
                    .topics()
                    .iter()
                    .map(|topic| fixed32(topic.as_slice()))
                    .collect::<Result<Vec<_>>>()?,
                data: log.data.data.to_vec(),
                block_num: mapped_block_num,
                tx_idx: tx_idx_u32,
                log_idx,
                block_hash,
            });
        }
    }

    Ok(FinalizedBlock {
        block_num: mapped_block_num,
        block_hash,
        parent_hash,
        header: EvmBlockHeader::minimal(mapped_block_num, block_hash, parent_hash),
        logs,
        txs: Vec::new(),
        trace_rlp: Vec::new(),
    })
}

fn fixed20(input: &[u8]) -> Result<[u8; 20]> {
    if input.len() != 20 {
        bail!("address len mismatch: expected 20, got {}", input.len());
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(input);
    Ok(out)
}

fn fixed32(input: &[u8]) -> Result<[u8; 32]> {
    if input.len() != 32 {
        bail!("hash/topic len mismatch: expected 32, got {}", input.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(input);
    Ok(out)
}

fn load_traces(dataset_dir: &Path, profiles: &[TraceProfileArg]) -> Result<Vec<TraceEntry>> {
    let mut selected = Vec::<TraceProfileArg>::new();
    if profiles.contains(&TraceProfileArg::All) {
        selected.push(TraceProfileArg::Expected);
        selected.push(TraceProfileArg::Stress);
        selected.push(TraceProfileArg::Adversarial);
    } else {
        selected.extend_from_slice(profiles);
    }

    let mut traces = Vec::new();
    for profile in selected {
        let file = match profile {
            TraceProfileArg::Expected => "trace_expected.jsonl",
            TraceProfileArg::Stress => "trace_stress.jsonl",
            TraceProfileArg::Adversarial => "trace_adversarial.jsonl",
            TraceProfileArg::All => continue,
        };

        let path = dataset_dir.join(file);
        let bytes =
            fs::read(&path).with_context(|| format!("read trace file {}", path.display()))?;
        for line in String::from_utf8(bytes)
            .with_context(|| format!("trace file is not valid utf8: {}", path.display()))?
            .lines()
        {
            if line.trim().is_empty() {
                continue;
            }
            let trace: TraceEntry =
                serde_json::from_str(line).with_context(|| format!("parse trace json: {line}"))?;
            traces.push(trace);
        }
    }

    Ok(traces)
}

fn trace_to_request(trace: &TraceEntry, max_results: usize) -> Result<QueryLogsRequest> {
    Ok(QueryLogsRequest {
        from_block: Some(trace.from_block),
        to_block: Some(trace.to_block),
        from_block_hash: None,
        to_block_hash: None,
        order: QueryOrder::Ascending,
        resume_id: None,
        limit: max_results,
        filter: LogFilter {
            address: decode_clause20(&trace.address_or)?,
            topic0: decode_clause32(&trace.topic0_or)?,
            topic1: decode_clause32(&trace.topic1_or)?,
            topic2: decode_clause32(&trace.topic2_or)?,
            topic3: decode_clause32(&trace.topic3_or)?,
        },
    })
}

fn decode_clause20(values: &[String]) -> Result<Option<Clause<[u8; 20]>>> {
    if values.is_empty() {
        return Ok(None);
    }
    let decoded = values
        .iter()
        .map(|v| decode_hex_fixed::<20>(v))
        .collect::<Result<Vec<_>>>()?;
    if decoded.len() == 1 {
        Ok(Some(Clause::One(decoded[0])))
    } else {
        Ok(Some(Clause::Or(decoded)))
    }
}

fn decode_clause32(values: &[String]) -> Result<Option<Clause<[u8; 32]>>> {
    if values.is_empty() {
        return Ok(None);
    }
    let decoded = values
        .iter()
        .map(|v| decode_hex_fixed::<32>(v))
        .collect::<Result<Vec<_>>>()?;
    if decoded.len() == 1 {
        Ok(Some(Clause::One(decoded[0])))
    } else {
        Ok(Some(Clause::Or(decoded)))
    }
}

fn decode_hex_fixed<const N: usize>(value: &str) -> Result<[u8; N]> {
    let trimmed = value.trim_start_matches("0x");
    let bytes = hex::decode(trimmed).with_context(|| format!("decode hex value: {value}"))?;
    if bytes.len() != N {
        bail!(
            "hex value {} decoded to {} bytes, expected {}",
            value,
            bytes.len(),
            N
        );
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn percentile(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx]
}

fn mean(values: &[u128]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let total: u128 = values.iter().copied().sum();
    total as f64 / values.len() as f64
}

fn trace_profile_name(profile: WorkloadTraceProfile) -> &'static str {
    match profile {
        WorkloadTraceProfile::Expected => "expected",
        WorkloadTraceProfile::Stress => "stress",
        WorkloadTraceProfile::Adversarial => "adversarial",
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output parent {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}
