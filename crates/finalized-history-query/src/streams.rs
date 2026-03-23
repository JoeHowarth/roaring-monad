use std::hash::{Hash, Hasher};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::codec::StorageCodec;
use crate::codec::fixed_codec;
use crate::error::{Error, Result};

const BITMAP_BLOB_HEADER_LEN: usize = 1 + 4 * 4;

#[derive(Debug, Clone)]
pub struct BitmapBlob {
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
    pub crc32: u32,
    pub bitmap: RoaringBitmap,
}

struct BitmapBlobHeader {
    min_local: u32,
    max_local: u32,
    count: u32,
    crc32: u32,
}

fixed_codec! {
    impl BitmapBlobHeader {
        length_error = "bitmap blob too short";
        version = 1;
        version_error = "unsupported bitmap blob version";
        fields {
            min_local: u32,
            max_local: u32,
            count: u32,
            crc32: u32,
        }
    }
}

pub fn encode_bitmap_blob(blob: &BitmapBlob) -> Result<Bytes> {
    let mut payload = Vec::new();
    blob.bitmap
        .serialize_into(&mut payload)
        .map_err(|e| Error::Backend(format!("serialize bitmap blob: {e}")))?;

    let crc32 = crc32_like(&payload);
    let header = BitmapBlobHeader {
        min_local: blob.min_local,
        max_local: blob.max_local,
        count: blob.count,
        crc32,
    };
    let mut out = Vec::with_capacity(BITMAP_BLOB_HEADER_LEN + payload.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&payload);
    Ok(Bytes::from(out))
}

pub fn decode_bitmap_blob(bytes: &[u8]) -> Result<BitmapBlob> {
    let header = BitmapBlobHeader::decode(
        bytes
            .get(..BITMAP_BLOB_HEADER_LEN)
            .ok_or(Error::Decode("bitmap blob too short"))?,
    )?;
    let payload = &bytes[BITMAP_BLOB_HEADER_LEN..];

    if header.crc32 != crc32_like(payload) {
        return Err(Error::Decode("bitmap blob checksum mismatch"));
    }

    let bitmap = RoaringBitmap::deserialize_from(payload)
        .map_err(|e| Error::Backend(format!("deserialize bitmap blob: {e}")))?;

    Ok(BitmapBlob {
        min_local: header.min_local,
        max_local: header.max_local,
        count: header.count,
        crc32: header.crc32,
        bitmap,
    })
}

fn crc32_like(bytes: &[u8]) -> u32 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    (h.finish() & 0xffff_ffff) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rejects_corrupted_payload() {
        let mut bm = RoaringBitmap::new();
        bm.insert(5);
        let blob = BitmapBlob {
            min_local: 5,
            max_local: 5,
            count: 1,
            crc32: 0,
            bitmap: bm,
        };
        let mut encoded = encode_bitmap_blob(&blob).expect("encode").to_vec();
        // Corrupt one byte in the bitmap payload (after the header).
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        let err = decode_bitmap_blob(&encoded).unwrap_err();
        assert!(
            err.to_string().contains("checksum"),
            "expected checksum error, got: {err}"
        );
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let err = decode_bitmap_blob(&[0u8; 4]).unwrap_err();
        assert!(
            err.to_string().contains("too short"),
            "expected too-short error, got: {err}"
        );
    }
}
