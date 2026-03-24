use bytes::Bytes;

use crate::error::{Error, Result};
use crate::kernel::codec::StorageCodec;
use crate::kernel::codec::fixed_codec;
use crate::traces::types::{BlockTraceHeader, StreamBitmapMeta};

impl StorageCodec for BlockTraceHeader {
    fn encode(&self) -> Bytes {
        assert!(u32::try_from(self.tx_starts.len()).is_ok());
        let offsets = self.offsets.encode();
        assert!(u32::try_from(offsets.len()).is_ok());

        let mut out = Vec::new();
        out.push(1);
        out.extend_from_slice(&self.encoding_version.to_be_bytes());
        out.extend_from_slice(&(offsets.len() as u32).to_be_bytes());
        out.extend_from_slice(&offsets);
        out.extend_from_slice(&(self.tx_starts.len() as u32).to_be_bytes());
        for tx_start in &self.tx_starts {
            out.extend_from_slice(&tx_start.to_be_bytes());
        }
        Bytes::from(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 4 + 4 + 1 + 4 {
            return Err(Error::Decode("block trace header too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported block trace header version"));
        }
        let encoding_version = u32::from_be_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| Error::Decode("block trace header encoding version"))?,
        );
        let offsets_len = u32::from_be_bytes(
            bytes[5..9]
                .try_into()
                .map_err(|_| Error::Decode("block trace header offsets length"))?,
        ) as usize;
        let offsets_end = 9usize
            .checked_add(offsets_len)
            .ok_or(Error::Decode("block trace header offsets overflow"))?;
        if bytes.len() < offsets_end + 4 {
            return Err(Error::Decode("block trace header offsets bytes"));
        }
        let offsets = crate::core::offsets::BucketedOffsets::decode(&bytes[9..offsets_end])?;
        let tx_count = u32::from_be_bytes(
            bytes[offsets_end..offsets_end + 4]
                .try_into()
                .map_err(|_| Error::Decode("block trace header tx count"))?,
        ) as usize;
        let expected_len = offsets_end
            .checked_add(4)
            .and_then(|len| len.checked_add(tx_count.checked_mul(4)?))
            .ok_or(Error::Decode("block trace header length overflow"))?;
        if bytes.len() != expected_len {
            return Err(Error::Decode("invalid block trace header length"));
        }

        let mut tx_starts = Vec::with_capacity(tx_count);
        let mut pos = offsets_end + 4;
        for _ in 0..tx_count {
            tx_starts.push(u32::from_be_bytes(
                bytes[pos..pos + 4]
                    .try_into()
                    .map_err(|_| Error::Decode("block trace header tx_start"))?,
            ));
            pos += 4;
        }

        Ok(Self {
            encoding_version,
            offsets,
            tx_starts,
        })
    }
}

fixed_codec! {
    impl StreamBitmapMeta {
        length_error = "invalid trace stream bitmap meta length";
        version = 1;
        version_error = "unsupported trace stream bitmap meta version";
        fields {
            count: u32,
            min_local: u32,
            max_local: u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::offsets::BucketedOffsets;
    use crate::traces::types::DirBucket;

    #[test]
    fn roundtrip_large_trace_dir_bucket() {
        let count = (u16::MAX as usize) + 2;
        let mut first_primary_ids = Vec::with_capacity(count);
        for i in 0..count {
            first_primary_ids.push(i as u64);
        }
        let bucket = DirBucket {
            start_block: 123,
            first_primary_ids,
        };

        let enc = bucket.encode();
        let dec = DirBucket::decode(&enc).expect("decode large bucket");
        assert_eq!(dec, bucket);
    }

    #[test]
    fn roundtrip_block_trace_header() {
        let mut offsets = BucketedOffsets::new();
        offsets.push(0).expect("push offset");
        offsets.push(8).expect("push offset");
        offsets.push((1u64 << 32) + 16).expect("push offset");
        let header = BlockTraceHeader {
            encoding_version: 7,
            offsets,
            tx_starts: vec![0, 2],
        };

        let enc = header.encode();
        let dec = BlockTraceHeader::decode(&enc).expect("decode header");
        assert_eq!(dec, header);
    }
}
