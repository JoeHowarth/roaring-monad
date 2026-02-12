use crate::error::Error;
use crate::stats::{CooccurrenceRow, KeyStatsRow, KeyType, PairType, RangeMetric, RangeStatsRow};
use crate::types::{DatasetManifest, TraceEntry};
use arrow::array::{Array, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub struct ParquetStats {
    pub key_stats: Vec<KeyStatsRow>,
    pub cooccurrence: Vec<CooccurrenceRow>,
    pub range_stats: Vec<RangeStatsRow>,
}

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

    write_manifest(&tmp_dir.join("dataset_manifest.json"), manifest)?;
    write_key_stats_parquet(&tmp_dir.join("key_stats.parquet"), key_stats)?;
    write_cooccurrence_parquet(&tmp_dir.join("cooccurrence.parquet"), cooccurrence)?;
    write_range_stats_parquet(&tmp_dir.join("range_stats.parquet"), range_stats)?;

    fs::rename(&tmp_dir, dataset_dir).map_err(|e| Error::Io(format!("rename dataset dir: {e}")))?;
    Ok(())
}

pub fn read_dataset_manifest(dataset_dir: &Path) -> Result<DatasetManifest, Error> {
    let bytes = fs::read(dataset_dir.join("dataset_manifest.json"))
        .map_err(|e| Error::Io(format!("read dataset manifest: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::Serialization(format!("deserialize dataset manifest: {e}")))
}

pub fn read_parquet_stats(dataset_dir: &Path) -> Result<ParquetStats, Error> {
    Ok(ParquetStats {
        key_stats: read_key_stats_parquet(&dataset_dir.join("key_stats.parquet"))?,
        cooccurrence: read_cooccurrence_parquet(&dataset_dir.join("cooccurrence.parquet"))?,
        range_stats: read_range_stats_parquet(&dataset_dir.join("range_stats.parquet"))?,
    })
}

pub fn write_trace_jsonl(path: &Path, entries: &[TraceEntry]) -> Result<(), Error> {
    let mut file = File::create(path).map_err(|e| Error::Io(format!("create trace file: {e}")))?;
    for entry in entries {
        let line = serde_json::to_string(entry)
            .map_err(|e| Error::Serialization(format!("serialize trace entry: {e}")))?;
        file.write_all(line.as_bytes())
            .map_err(|e| Error::Io(format!("write trace line: {e}")))?;
        file.write_all(b"\n")
            .map_err(|e| Error::Io(format!("write trace newline: {e}")))?;
    }
    Ok(())
}

fn write_manifest(path: &Path, manifest: &DatasetManifest) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| Error::Serialization(format!("serialize dataset manifest: {e}")))?;
    fs::write(path, bytes).map_err(|e| Error::Io(format!("write dataset manifest: {e}")))
}

fn write_key_stats_parquet(path: &Path, rows: &[KeyStatsRow]) -> Result<(), Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key_type", DataType::Utf8, false),
        Field::new("key_value", DataType::Utf8, false),
        Field::new("count_total", DataType::UInt64, false),
        Field::new("first_block", DataType::UInt64, false),
        Field::new("last_block", DataType::UInt64, false),
        Field::new("active_block_count", DataType::UInt64, false),
        Field::new("distinct_partner_estimate", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| key_type_to_str(r.key_type))
                    .collect::<Vec<_>>(),
            )) as Arc<dyn Array>,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| hex::encode(&r.key_value))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.count_total).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.first_block).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.last_block).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|r| r.active_block_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.distinct_partner_estimate)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| Error::Serialization(format!("build key_stats batch: {e}")))?;

    write_batch(path, schema, batch)
}

fn write_cooccurrence_parquet(path: &Path, rows: &[CooccurrenceRow]) -> Result<(), Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pair_type", DataType::Utf8, false),
        Field::new("left_key", DataType::Utf8, false),
        Field::new("right_key", DataType::Utf8, false),
        Field::new("count_total", DataType::UInt64, false),
        Field::new("first_block", DataType::UInt64, false),
        Field::new("last_block", DataType::UInt64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| pair_type_to_str(r.pair_type))
                    .collect::<Vec<_>>(),
            )) as Arc<dyn Array>,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| hex::encode(&r.left_key))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| hex::encode(&r.right_key))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.count_total).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.first_block).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.last_block).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| Error::Serialization(format!("build cooccurrence batch: {e}")))?;

    write_batch(path, schema, batch)
}

fn write_range_stats_parquet(path: &Path, rows: &[RangeStatsRow]) -> Result<(), Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric", DataType::Utf8, false),
        Field::new("bucket_lower", DataType::UInt64, false),
        Field::new("bucket_upper", DataType::UInt64, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("window_size_blocks", DataType::UInt64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| metric_to_str(r.metric))
                    .collect::<Vec<_>>(),
            )) as Arc<dyn Array>,
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.bucket_lower).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.bucket_upper).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.count).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|r| r.window_size_blocks)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| Error::Serialization(format!("build range_stats batch: {e}")))?;

    write_batch(path, schema, batch)
}

fn write_batch(path: &Path, schema: Arc<Schema>, batch: RecordBatch) -> Result<(), Error> {
    let file = File::create(path).map_err(|e| Error::Io(format!("create parquet file: {e}")))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|e| Error::Serialization(format!("create parquet writer: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| Error::Serialization(format!("write parquet batch: {e}")))?;
    writer
        .close()
        .map_err(|e| Error::Serialization(format!("close parquet writer: {e}")))?;
    Ok(())
}

fn read_key_stats_parquet(path: &Path) -> Result<Vec<KeyStatsRow>, Error> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|e| Error::Io(format!("open key_stats parquet: {e}")))?,
    )
    .map_err(|e| Error::Serialization(format!("build key_stats reader: {e}")))?
    .build()
    .map_err(|e| Error::Serialization(format!("open key_stats batch reader: {e}")))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch =
            batch.map_err(|e| Error::Serialization(format!("read key_stats batch: {e}")))?;
        let key_type = str_col(&batch, 0)?;
        let key_value = str_col(&batch, 1)?;
        let count_total = u64_col(&batch, 2)?;
        let first_block = u64_col(&batch, 3)?;
        let last_block = u64_col(&batch, 4)?;
        let active_block_count = u64_col(&batch, 5)?;
        let distinct = f64_opt_col(&batch, 6)?;

        for i in 0..batch.num_rows() {
            out.push(KeyStatsRow {
                key_type: str_to_key_type(key_type.value(i))?,
                key_value: hex::decode(key_value.value(i))
                    .map_err(|e| Error::Serialization(format!("decode key_value hex: {e}")))?,
                count_total: count_total.value(i),
                first_block: first_block.value(i),
                last_block: last_block.value(i),
                active_block_count: active_block_count.value(i),
                distinct_partner_estimate: if distinct.is_null(i) {
                    None
                } else {
                    Some(distinct.value(i))
                },
            });
        }
    }
    Ok(out)
}

fn read_cooccurrence_parquet(path: &Path) -> Result<Vec<CooccurrenceRow>, Error> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|e| Error::Io(format!("open cooccurrence parquet: {e}")))?,
    )
    .map_err(|e| Error::Serialization(format!("build cooccurrence reader: {e}")))?
    .build()
    .map_err(|e| Error::Serialization(format!("open cooccurrence batch reader: {e}")))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch =
            batch.map_err(|e| Error::Serialization(format!("read cooccurrence batch: {e}")))?;
        let pair_type = str_col(&batch, 0)?;
        let left_key = str_col(&batch, 1)?;
        let right_key = str_col(&batch, 2)?;
        let count_total = u64_col(&batch, 3)?;
        let first_block = u64_col(&batch, 4)?;
        let last_block = u64_col(&batch, 5)?;

        for i in 0..batch.num_rows() {
            out.push(CooccurrenceRow {
                pair_type: str_to_pair_type(pair_type.value(i))?,
                left_key: hex::decode(left_key.value(i))
                    .map_err(|e| Error::Serialization(format!("decode left_key hex: {e}")))?,
                right_key: hex::decode(right_key.value(i))
                    .map_err(|e| Error::Serialization(format!("decode right_key hex: {e}")))?,
                count_total: count_total.value(i),
                first_block: first_block.value(i),
                last_block: last_block.value(i),
            });
        }
    }
    Ok(out)
}

fn read_range_stats_parquet(path: &Path) -> Result<Vec<RangeStatsRow>, Error> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|e| Error::Io(format!("open range_stats parquet: {e}")))?,
    )
    .map_err(|e| Error::Serialization(format!("build range_stats reader: {e}")))?
    .build()
    .map_err(|e| Error::Serialization(format!("open range_stats batch reader: {e}")))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch =
            batch.map_err(|e| Error::Serialization(format!("read range_stats batch: {e}")))?;
        let metric = str_col(&batch, 0)?;
        let bucket_lower = u64_col(&batch, 1)?;
        let bucket_upper = u64_col(&batch, 2)?;
        let count = u64_col(&batch, 3)?;
        let window_size_blocks = u64_opt_col(&batch, 4)?;

        for i in 0..batch.num_rows() {
            out.push(RangeStatsRow {
                metric: str_to_metric(metric.value(i))?,
                bucket_lower: bucket_lower.value(i),
                bucket_upper: bucket_upper.value(i),
                count: count.value(i),
                window_size_blocks: if window_size_blocks.is_null(i) {
                    None
                } else {
                    Some(window_size_blocks.value(i))
                },
            });
        }
    }
    Ok(out)
}

fn str_col(batch: &RecordBatch, idx: usize) -> Result<&StringArray, Error> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::InternalInvariant(format!("expected utf8 at col {idx}")))
}

fn u64_col(batch: &RecordBatch, idx: usize) -> Result<&UInt64Array, Error> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::InternalInvariant(format!("expected u64 at col {idx}")))
}

fn u64_opt_col(batch: &RecordBatch, idx: usize) -> Result<&UInt64Array, Error> {
    u64_col(batch, idx)
}

fn f64_opt_col(batch: &RecordBatch, idx: usize) -> Result<&Float64Array, Error> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::InternalInvariant(format!("expected f64 at col {idx}")))
}

fn key_type_to_str(v: KeyType) -> &'static str {
    match v {
        KeyType::Address => "address",
        KeyType::Topic0 => "topic0",
        KeyType::Topic1 => "topic1",
        KeyType::Topic2 => "topic2",
        KeyType::Topic3 => "topic3",
        KeyType::AddressTopic0 => "address_topic0",
    }
}

fn str_to_key_type(v: &str) -> Result<KeyType, Error> {
    match v {
        "address" => Ok(KeyType::Address),
        "topic0" => Ok(KeyType::Topic0),
        "topic1" => Ok(KeyType::Topic1),
        "topic2" => Ok(KeyType::Topic2),
        "topic3" => Ok(KeyType::Topic3),
        "address_topic0" => Ok(KeyType::AddressTopic0),
        _ => Err(Error::Serialization(format!("unknown key_type: {v}"))),
    }
}

fn pair_type_to_str(v: PairType) -> &'static str {
    match v {
        PairType::AddressTopic0 => "address_topic0",
        PairType::Topic0Topic1 => "topic0_topic1",
    }
}

fn str_to_pair_type(v: &str) -> Result<PairType, Error> {
    match v {
        "address_topic0" => Ok(PairType::AddressTopic0),
        "topic0_topic1" => Ok(PairType::Topic0Topic1),
        _ => Err(Error::Serialization(format!("unknown pair_type: {v}"))),
    }
}

fn metric_to_str(v: RangeMetric) -> &'static str {
    match v {
        RangeMetric::LogsPerBlock => "logs_per_block",
        RangeMetric::LogsPerWindow => "logs_per_window",
        RangeMetric::InterarrivalSeconds => "interarrival_seconds",
    }
}

fn str_to_metric(v: &str) -> Result<RangeMetric, Error> {
    match v {
        "logs_per_block" => Ok(RangeMetric::LogsPerBlock),
        "logs_per_window" => Ok(RangeMetric::LogsPerWindow),
        "interarrival_seconds" => Ok(RangeMetric::InterarrivalSeconds),
        _ => Err(Error::Serialization(format!("unknown range metric: {v}"))),
    }
}
