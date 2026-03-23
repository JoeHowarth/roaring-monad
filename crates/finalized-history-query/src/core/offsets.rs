use bytes::Bytes;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct BucketedOffsets {
    bucket_high_bits: Vec<u32>,
    bucket_starts: Vec<u32>,
    buckets: Vec<Vec<u32>>,
}

impl BucketedOffsets {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, offset: u64) -> Result<()> {
        let high_bits = u32::try_from(offset >> 32)
            .map_err(|_| Error::Decode("bucketed offset high bits overflow"))?;
        let low_bits = (offset & u64::from(u32::MAX)) as u32;

        match self.bucket_high_bits.last().copied() {
            Some(existing_high_bits) if existing_high_bits == high_bits => {
                self.buckets
                    .last_mut()
                    .expect("bucket exists for matching high bits")
                    .push(low_bits);
            }
            _ => {
                let bucket_start = u32::try_from(self.len())
                    .map_err(|_| Error::Decode("bucketed offset entity count overflow"))?;
                self.bucket_high_bits.push(high_bits);
                self.bucket_starts.push(bucket_start);
                self.buckets.push(vec![low_bits]);
            }
        }

        Ok(())
    }

    pub fn get(&self, index: usize) -> Option<u64> {
        let bucket_index = self.bucket_index_for(index)?;
        let local_index = index.checked_sub(self.bucket_starts[bucket_index] as usize)?;
        let low_bits = *self.buckets.get(bucket_index)?.get(local_index)?;
        Some((u64::from(self.bucket_high_bits[bucket_index]) << 32) | u64::from(low_bits))
    }

    pub fn encode(&self) -> Bytes {
        assert!(u32::try_from(self.bucket_high_bits.len()).is_ok());
        let total_entries = self.buckets.iter().map(Vec::len).sum::<usize>();
        assert!(u32::try_from(total_entries).is_ok());

        let mut out = Vec::new();
        out.push(1);
        out.extend_from_slice(&(self.bucket_high_bits.len() as u32).to_be_bytes());
        for ((high_bits, bucket_start), bucket) in self
            .bucket_high_bits
            .iter()
            .zip(self.bucket_starts.iter())
            .zip(self.buckets.iter())
        {
            out.extend_from_slice(&high_bits.to_be_bytes());
            out.extend_from_slice(&bucket_start.to_be_bytes());
            out.extend_from_slice(&(bucket.len() as u32).to_be_bytes());
            for low_bits in bucket {
                out.extend_from_slice(&low_bits.to_be_bytes());
            }
        }
        Bytes::from(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 1 + 4 {
            return Err(Error::Decode("bucketed offsets too short"));
        }
        if bytes[0] != 1 {
            return Err(Error::Decode("unsupported bucketed offsets version"));
        }

        let bucket_count = u32::from_be_bytes(
            bytes[1..5]
                .try_into()
                .map_err(|_| Error::Decode("bucketed offsets count"))?,
        ) as usize;
        let mut pos = 5usize;
        let mut bucket_high_bits = Vec::with_capacity(bucket_count);
        let mut bucket_starts = Vec::with_capacity(bucket_count);
        let mut buckets = Vec::with_capacity(bucket_count);

        for _ in 0..bucket_count {
            if bytes.len() < pos + 12 {
                return Err(Error::Decode("bucketed offsets bucket header"));
            }
            let high_bits = u32::from_be_bytes(
                bytes[pos..pos + 4]
                    .try_into()
                    .map_err(|_| Error::Decode("bucketed offsets high bits"))?,
            );
            pos += 4;
            let bucket_start = u32::from_be_bytes(
                bytes[pos..pos + 4]
                    .try_into()
                    .map_err(|_| Error::Decode("bucketed offsets bucket start"))?,
            );
            pos += 4;
            let entry_count = u32::from_be_bytes(
                bytes[pos..pos + 4]
                    .try_into()
                    .map_err(|_| Error::Decode("bucketed offsets entry count"))?,
            ) as usize;
            pos += 4;

            let entry_bytes = entry_count
                .checked_mul(4)
                .ok_or(Error::Decode("bucketed offsets entry bytes overflow"))?;
            if bytes.len() < pos + entry_bytes {
                return Err(Error::Decode("bucketed offsets entries"));
            }

            let mut bucket = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                bucket.push(u32::from_be_bytes(
                    bytes[pos..pos + 4]
                        .try_into()
                        .map_err(|_| Error::Decode("bucketed offsets entry"))?,
                ));
                pos += 4;
            }

            bucket_high_bits.push(high_bits);
            bucket_starts.push(bucket_start);
            buckets.push(bucket);
        }

        if pos != bytes.len() {
            return Err(Error::Decode("invalid bucketed offsets length"));
        }

        let offsets = Self {
            bucket_high_bits,
            bucket_starts,
            buckets,
        };
        offsets.validate()?;
        Ok(offsets)
    }

    fn bucket_index_for(&self, index: usize) -> Option<usize> {
        if self.bucket_starts.is_empty() {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.bucket_starts.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.bucket_starts[mid] as usize <= index {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.checked_sub(1)
    }

    fn validate(&self) -> Result<()> {
        if self.bucket_high_bits.len() != self.bucket_starts.len()
            || self.bucket_high_bits.len() != self.buckets.len()
        {
            return Err(Error::Decode(
                "bucketed offsets bucket vector length mismatch",
            ));
        }

        let mut expected_start = 0u32;
        let mut previous_high_bits = None::<u32>;
        for ((high_bits, bucket_start), bucket) in self
            .bucket_high_bits
            .iter()
            .zip(self.bucket_starts.iter())
            .zip(self.buckets.iter())
        {
            if *bucket_start != expected_start {
                return Err(Error::Decode("bucketed offsets invalid bucket start"));
            }
            if let Some(previous_high_bits) = previous_high_bits
                && *high_bits <= previous_high_bits
            {
                return Err(Error::Decode("bucketed offsets high bits not increasing"));
            }
            expected_start = expected_start
                .checked_add(
                    u32::try_from(bucket.len())
                        .map_err(|_| Error::Decode("bucketed offsets bucket length overflow"))?,
                )
                .ok_or(Error::Decode("bucketed offsets total length overflow"))?;
            previous_high_bits = Some(*high_bits);
        }

        Ok(())
    }
}

/// Compute the byte offset of `slice` within `root`.
///
/// Both must point into the same allocation (e.g. `slice` was obtained by
/// parsing `root`). Panics if `slice` does not live inside `root`.
pub fn byte_offset_in(root: &[u8], slice: &[u8]) -> u64 {
    let root_ptr = root.as_ptr();
    let slice_ptr = slice.as_ptr();
    // SAFETY: both pointers come from the same allocation (the caller must
    // guarantee this). `offset_from` is well-defined for pointers into the
    // same allocated object.
    let offset = unsafe { slice_ptr.offset_from(root_ptr) };
    u64::try_from(offset).expect("slice must live inside the root buffer")
}

#[cfg(test)]
mod tests {
    use super::BucketedOffsets;

    #[test]
    fn bucketed_offsets_roundtrip_empty() {
        let offsets = BucketedOffsets::new();
        let decoded = BucketedOffsets::decode(&offsets.encode()).expect("decode offsets");
        assert_eq!(decoded, offsets);
        assert_eq!(decoded.get(0), None);
    }

    #[test]
    fn bucketed_offsets_roundtrip_single_bucket() {
        let mut offsets = BucketedOffsets::new();
        offsets.push(0).expect("push");
        offsets.push(7).expect("push");
        offsets.push(1024).expect("push");

        let decoded = BucketedOffsets::decode(&offsets.encode()).expect("decode offsets");
        assert_eq!(decoded.get(0), Some(0));
        assert_eq!(decoded.get(1), Some(7));
        assert_eq!(decoded.get(2), Some(1024));
        assert_eq!(decoded.get(3), None);
    }

    #[test]
    fn bucketed_offsets_roundtrip_multiple_buckets() {
        let mut offsets = BucketedOffsets::new();
        offsets.push(1).expect("push");
        offsets.push((1u64 << 32) + 9).expect("push");
        offsets.push((1u64 << 32) + 33).expect("push");
        offsets.push((3u64 << 32) + 5).expect("push");

        let decoded = BucketedOffsets::decode(&offsets.encode()).expect("decode offsets");
        assert_eq!(decoded.get(0), Some(1));
        assert_eq!(decoded.get(1), Some((1u64 << 32) + 9));
        assert_eq!(decoded.get(2), Some((1u64 << 32) + 33));
        assert_eq!(decoded.get(3), Some((3u64 << 32) + 5));
    }
}
