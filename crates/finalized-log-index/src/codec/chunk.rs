use std::hash::{Hash, Hasher};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct ChunkBlob {
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
    pub crc32: u32,
    pub bitmap: RoaringBitmap,
}

pub fn encode_chunk(blob: &ChunkBlob) -> Result<Bytes> {
    let mut payload = Vec::new();
    blob.bitmap
        .serialize_into(&mut payload)
        .map_err(|e| Error::Backend(format!("serialize chunk bitmap: {e}")))?;

    let mut out = Vec::with_capacity(1 + 4 * 4 + payload.len());
    out.push(1); // codec version
    out.extend_from_slice(&blob.min_local.to_be_bytes());
    out.extend_from_slice(&blob.max_local.to_be_bytes());
    out.extend_from_slice(&blob.count.to_be_bytes());
    let crc32 = crc32_like(&payload);
    out.extend_from_slice(&crc32.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(Bytes::from(out))
}

pub fn decode_chunk(bytes: &[u8]) -> Result<ChunkBlob> {
    if bytes.len() < 17 {
        return Err(Error::Decode("chunk too short"));
    }
    if bytes[0] != 1 {
        return Err(Error::Decode("unsupported chunk version"));
    }

    let min_local = u32::from_be_bytes(bytes[1..5].try_into().map_err(|_| Error::Decode("min"))?);
    let max_local = u32::from_be_bytes(bytes[5..9].try_into().map_err(|_| Error::Decode("max"))?);
    let count = u32::from_be_bytes(
        bytes[9..13]
            .try_into()
            .map_err(|_| Error::Decode("count"))?,
    );
    let crc32 = u32::from_be_bytes(bytes[13..17].try_into().map_err(|_| Error::Decode("crc"))?);
    let payload = &bytes[17..];

    if crc32 != crc32_like(payload) {
        return Err(Error::Decode("chunk checksum mismatch"));
    }

    let bitmap = RoaringBitmap::deserialize_from(payload)
        .map_err(|e| Error::Backend(format!("deserialize chunk bitmap: {e}")))?;

    Ok(ChunkBlob {
        min_local,
        max_local,
        count,
        crc32,
        bitmap,
    })
}

fn crc32_like(bytes: &[u8]) -> u32 {
    // Non-cryptographic checksum for corruption detection in this scaffold.
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    (h.finish() & 0xffff_ffff) as u32
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use super::*;

    #[test]
    fn roundtrip_chunk() {
        let mut bm = RoaringBitmap::new();
        bm.insert(2);
        bm.insert(1000);
        let blob = ChunkBlob {
            min_local: 2,
            max_local: 1000,
            count: 2,
            crc32: 0,
            bitmap: bm,
        };

        let enc = encode_chunk(&blob).expect("encode");
        let dec = decode_chunk(&enc).expect("decode");
        assert_eq!(dec.min_local, 2);
        assert_eq!(dec.max_local, 1000);
        assert_eq!(dec.count, 2);
        assert!(dec.bitmap.contains(2));
        assert!(dec.bitmap.contains(1000));
    }
}
