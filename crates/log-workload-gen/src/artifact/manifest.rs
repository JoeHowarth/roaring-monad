use crate::error::Error;
use crate::types::DatasetManifest;
use std::fs;
use std::path::Path;

pub fn write_manifest(path: &Path, manifest: &DatasetManifest) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| Error::Serialization(format!("serialize dataset manifest: {e}")))?;
    fs::write(path, bytes).map_err(|e| Error::Io(format!("write dataset manifest: {e}")))
}

pub fn read_manifest(path: &Path) -> Result<DatasetManifest, Error> {
    let bytes = fs::read(path).map_err(|e| Error::Io(format!("read dataset manifest: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::Serialization(format!("deserialize dataset manifest: {e}")))
}
