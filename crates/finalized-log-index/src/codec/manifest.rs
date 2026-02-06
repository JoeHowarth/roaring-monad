use bytes::Bytes;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct ChunkRef {
    pub chunk_seq: u64,
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Manifest {
    pub version: u64,
    pub last_chunk_seq: u64,
    pub chunk_refs: Vec<ChunkRef>,
    pub approx_count: u64,
}

pub fn encode_manifest(manifest: &Manifest) -> Bytes {
    let mut out = Vec::with_capacity(32 + manifest.chunk_refs.len() * 20);
    out.push(1); // codec version
    out.extend_from_slice(&manifest.version.to_be_bytes());
    out.extend_from_slice(&manifest.last_chunk_seq.to_be_bytes());
    out.extend_from_slice(&manifest.approx_count.to_be_bytes());
    out.extend_from_slice(&(manifest.chunk_refs.len() as u32).to_be_bytes());
    for c in &manifest.chunk_refs {
        out.extend_from_slice(&c.chunk_seq.to_be_bytes());
        out.extend_from_slice(&c.min_local.to_be_bytes());
        out.extend_from_slice(&c.max_local.to_be_bytes());
        out.extend_from_slice(&c.count.to_be_bytes());
    }
    Bytes::from(out)
}

pub fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    if bytes.len() < 1 + 8 + 8 + 8 + 4 {
        return Err(Error::Decode("manifest too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported manifest version"));
    }

    let version = u64::from_be_bytes(
        bytes[1..9]
            .try_into()
            .map_err(|_| Error::Decode("version"))?,
    );
    let last_chunk_seq = u64::from_be_bytes(
        bytes[9..17]
            .try_into()
            .map_err(|_| Error::Decode("last_chunk_seq"))?,
    );
    let approx_count = u64::from_be_bytes(
        bytes[17..25]
            .try_into()
            .map_err(|_| Error::Decode("approx_count"))?,
    );
    let n =
        u32::from_be_bytes(bytes[25..29].try_into().map_err(|_| Error::Decode("len"))?) as usize;

    let expected = 29 + n * 20;
    if bytes.len() != expected {
        return Err(Error::Decode("manifest length mismatch"));
    }

    let mut chunk_refs = Vec::with_capacity(n);
    let mut pos = 29usize;
    for _ in 0..n {
        let chunk_seq = u64::from_be_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .map_err(|_| Error::Decode("chunk_seq"))?,
        );
        pos += 8;
        let min_local = u32::from_be_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Decode("min_local"))?,
        );
        pos += 4;
        let max_local = u32::from_be_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Decode("max_local"))?,
        );
        pos += 4;
        let count = u32::from_be_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Decode("count"))?,
        );
        pos += 4;

        chunk_refs.push(ChunkRef {
            chunk_seq,
            min_local,
            max_local,
            count,
        });
    }

    Ok(Manifest {
        version,
        last_chunk_seq,
        chunk_refs,
        approx_count,
    })
}

pub fn encode_tail(entries: &roaring::RoaringBitmap) -> Result<Bytes> {
    let mut out = Vec::new();
    out.push(1);
    entries
        .serialize_into(&mut out)
        .map_err(|e| Error::Backend(format!("serialize tail: {e}")))?;
    Ok(Bytes::from(out))
}

pub fn decode_tail(bytes: &[u8]) -> Result<roaring::RoaringBitmap> {
    if bytes.is_empty() {
        return Err(Error::Decode("tail too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported tail version"));
    }
    roaring::RoaringBitmap::deserialize_from(&bytes[1..])
        .map_err(|e| Error::Backend(format!("deserialize tail: {e}")))
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use super::*;

    #[test]
    fn roundtrip_manifest() {
        let m = Manifest {
            version: 7,
            last_chunk_seq: 11,
            approx_count: 100,
            chunk_refs: vec![ChunkRef {
                chunk_seq: 11,
                min_local: 2,
                max_local: 3,
                count: 2,
            }],
        };
        let enc = encode_manifest(&m);
        let dec = decode_manifest(&enc).expect("decode");
        assert_eq!(dec.version, m.version);
        assert_eq!(dec.last_chunk_seq, m.last_chunk_seq);
        assert_eq!(dec.approx_count, m.approx_count);
        assert_eq!(dec.chunk_refs.len(), 1);
    }

    #[test]
    fn roundtrip_tail() {
        let mut bm = RoaringBitmap::new();
        bm.insert(10);
        bm.insert(42);

        let enc = encode_tail(&bm).expect("encode tail");
        let dec = decode_tail(&enc).expect("decode tail");
        assert!(dec.contains(10));
        assert!(dec.contains(42));
    }
}
