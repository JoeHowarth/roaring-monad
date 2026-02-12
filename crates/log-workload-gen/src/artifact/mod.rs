mod manifest;
mod parquet;
mod trace;

use crate::error::Error;
use crate::stats::{CooccurrenceRow, KeyStatsRow, RangeStatsRow};
use crate::types::DatasetManifest;
use std::fs;
use std::path::Path;

pub use parquet::ParquetStats;
pub use trace::write_trace_jsonl;

pub fn write_dataset_artifacts(
    dataset_dir: &Path,
    manifest: &DatasetManifest,
    key_stats: &[KeyStatsRow],
    cooccurrence: &[CooccurrenceRow],
    range_stats: &[RangeStatsRow],
) -> Result<(), Error> {
    let tmp_dir = dataset_dir.with_extension(format!("tmp.{}", std::process::id()));
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir).map_err(|e| Error::Io(format!("remove tmp dir: {e}")))?;
    }
    fs::create_dir_all(&tmp_dir).map_err(|e| Error::Io(format!("create tmp dir: {e}")))?;

    manifest::write_manifest(&tmp_dir.join("dataset_manifest.json"), manifest)?;
    parquet::write_key_stats_parquet(&tmp_dir.join("key_stats.parquet"), key_stats)?;
    parquet::write_cooccurrence_parquet(&tmp_dir.join("cooccurrence.parquet"), cooccurrence)?;
    parquet::write_range_stats_parquet(&tmp_dir.join("range_stats.parquet"), range_stats)?;

    fs::rename(&tmp_dir, dataset_dir).map_err(|e| Error::Io(format!("rename dataset dir: {e}")))?;
    Ok(())
}

pub fn read_dataset_manifest(dataset_dir: &Path) -> Result<DatasetManifest, Error> {
    manifest::read_manifest(&dataset_dir.join("dataset_manifest.json"))
}

pub fn read_parquet_stats(dataset_dir: &Path) -> Result<ParquetStats, Error> {
    Ok(ParquetStats {
        key_stats: parquet::read_key_stats_parquet(&dataset_dir.join("key_stats.parquet"))?,
        cooccurrence: parquet::read_cooccurrence_parquet(
            &dataset_dir.join("cooccurrence.parquet"),
        )?,
        range_stats: parquet::read_range_stats_parquet(&dataset_dir.join("range_stats.parquet"))?,
    })
}
