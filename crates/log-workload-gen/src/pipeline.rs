use crate::artifact::{
    read_dataset_manifest, read_parquet_stats, write_dataset_artifacts, write_trace_jsonl,
};
use crate::config::GeneratorConfig;
use crate::error::Error;
use crate::generate::generate_traces;
use crate::ingest::consume_messages_with_events;
use crate::stats::{CooccurrenceAccumulator, KeyStatsAccumulator, RangeStatsAccumulator};
use crate::types::{DatasetManifest, DatasetSummary, TraceSummary};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;

pub async fn run_collect(
    config: GeneratorConfig,
    mut receiver: Receiver<crate::types::Message>,
    dataset_path: &Path,
) -> Result<DatasetSummary, Error> {
    config.validate()?;

    let mut messages = Vec::new();
    while let Some(msg) = receiver.recv().await {
        messages.push(msg);
    }

    let (summary, events) = consume_messages_with_events(messages);

    let mut key_stats = KeyStatsAccumulator::new();
    let mut cooccurrence = CooccurrenceAccumulator::new(config.cooccurrence_top_k_per_type);
    let mut range_stats = RangeStatsAccumulator::new(config.logs_per_window_size_blocks);

    for event in &events {
        for log in &event.logs {
            key_stats.observe_log(event.block_number, log);
            cooccurrence.observe_log(event.block_number, log);
        }
        range_stats.observe_block(event.block_number, event.logs.len() as u64, event.timestamp);
    }

    let key_rows = key_stats.finalize();
    let co_rows = cooccurrence.finalize();
    let range_rows = if let (Some(start), Some(end)) = (summary.start_block, summary.end_block) {
        range_stats.finalize(start, end)
    } else {
        Vec::new()
    };

    let manifest = build_manifest(&config, &summary, None)?;
    write_dataset_artifacts(dataset_path, &manifest, &key_rows, &co_rows, &range_rows)?;
    Ok(summary)
}

pub async fn run_collect_and_generate(
    config: GeneratorConfig,
    receiver: Receiver<crate::types::Message>,
    dataset_path: &Path,
    seed: u64,
) -> Result<TraceSummary, Error> {
    let summary = run_collect(config.clone(), receiver, dataset_path).await?;
    if !summary.valid {
        return Err(Error::InputInvalid(
            "cannot generate traces from invalid dataset".to_string(),
        ));
    }
    run_offline_generate(config, dataset_path, seed).await
}

pub async fn run_offline_generate(
    config: GeneratorConfig,
    dataset_path: &Path,
    seed: u64,
) -> Result<TraceSummary, Error> {
    config.validate()?;
    let mut manifest = read_dataset_manifest(dataset_path)?;
    let stats = read_parquet_stats(dataset_path)?;
    manifest.seed = Some(seed);

    let generated = generate_traces(&config, &manifest, &stats, seed)?;

    write_trace_jsonl(
        &dataset_path.join("trace_expected.jsonl"),
        &generated.expected,
    )?;
    write_trace_jsonl(&dataset_path.join("trace_stress.jsonl"), &generated.stress)?;
    write_trace_jsonl(
        &dataset_path.join("trace_adversarial.jsonl"),
        &generated.adversarial,
    )?;

    Ok(TraceSummary {
        expected: generated.expected.len() as u64,
        stress: generated.stress.len() as u64,
        adversarial: generated.adversarial.len() as u64,
    })
}

fn build_manifest(
    config: &GeneratorConfig,
    summary: &DatasetSummary,
    seed: Option<u64>,
) -> Result<DatasetManifest, Error> {
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::InternalInvariant(format!("system time before epoch: {e}")))?
        .as_secs()
        .to_string();

    Ok(DatasetManifest {
        schema_version: "1.0.0".to_string(),
        crate_version: env!("CARGO_PKG_VERSION").to_string(),
        chain_id: 1,
        start_block: summary.start_block.unwrap_or(0),
        end_block: summary.end_block.unwrap_or(0),
        blocks_observed: summary.blocks_observed,
        gap_count: summary.gap_count,
        missing_block_ranges: summary.missing_block_ranges.clone(),
        event_count: summary.event_count,
        log_count: summary.log_count,
        created_at,
        config_hash: config.config_hash()?,
        seed,
        valid: summary.valid,
        invalid_reason: summary.invalid_reason.clone(),
    })
}
