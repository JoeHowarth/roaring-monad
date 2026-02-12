use crate::error::Error;
use crate::types::TraceEntry;
use std::fs::File;
use std::io::Write;
use std::path::Path;

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
