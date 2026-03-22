use bytes::Bytes;

use crate::error::{Error, Result};
use crate::family::Hash32;
use crate::logs::types::{Address20, Log, Topic32};

/// Zero-copy view over an encoded log record.
///
/// Wire layout:
///   address:     [u8; 20]
///   topic_count: u8
///   topics:      topic_count * [u8; 32]
///   data_len:    u32 BE
///   data:        data_len bytes
///   block_num:   u64 BE
///   tx_idx:      u32 BE
///   log_idx:     u32 BE
///   block_hash:  [u8; 32]
#[derive(Clone)]
pub struct LogRef {
    buf: Bytes,
    topic_count: u8,
    data_offset: u32,
    data_len: u32,
    trailer_offset: u32,
}

impl LogRef {
    pub fn new(buf: Bytes) -> Result<Self> {
        const MIN_LEN: usize = 20 + 1 + 4 + 8 + 4 + 4 + 32; // 73
        if buf.len() < MIN_LEN {
            return Err(Error::Decode("log too short"));
        }

        let topic_count = buf[20];
        if topic_count > 4 {
            return Err(Error::Decode("topic count exceeds 4"));
        }

        let topics_end = 21 + (topic_count as usize) * 32;
        if buf.len() < topics_end + 4 + 8 + 4 + 4 + 32 {
            return Err(Error::Decode("log missing topic bytes"));
        }

        let data_len = u32::from_be_bytes(
            buf[topics_end..topics_end + 4]
                .try_into()
                .map_err(|_| Error::Decode("log data_len"))?,
        );
        let data_offset = (topics_end + 4) as u32;
        let trailer_offset = data_offset + data_len;

        let expected_end = trailer_offset as usize + 8 + 4 + 4 + 32;
        if buf.len() < expected_end {
            return Err(Error::Decode("log missing data/body bytes"));
        }

        Ok(Self {
            buf,
            topic_count,
            data_offset,
            data_len,
            trailer_offset,
        })
    }

    pub fn address(&self) -> &Address20 {
        self.buf[0..20].try_into().unwrap()
    }

    pub fn topic_count(&self) -> usize {
        self.topic_count as usize
    }

    pub fn topic(&self, i: usize) -> &Topic32 {
        assert!(i < self.topic_count as usize, "topic index out of bounds");
        let start = 21 + i * 32;
        self.buf[start..start + 32].try_into().unwrap()
    }

    pub fn topics(&self) -> impl Iterator<Item = &Topic32> {
        (0..self.topic_count as usize).map(move |i| self.topic(i))
    }

    pub fn data(&self) -> &[u8] {
        let start = self.data_offset as usize;
        let end = start + self.data_len as usize;
        &self.buf[start..end]
    }

    pub fn block_num(&self) -> u64 {
        let off = self.trailer_offset as usize;
        u64::from_be_bytes(self.buf[off..off + 8].try_into().unwrap())
    }

    pub fn tx_idx(&self) -> u32 {
        let off = self.trailer_offset as usize + 8;
        u32::from_be_bytes(self.buf[off..off + 4].try_into().unwrap())
    }

    pub fn log_idx(&self) -> u32 {
        let off = self.trailer_offset as usize + 12;
        u32::from_be_bytes(self.buf[off..off + 4].try_into().unwrap())
    }

    pub fn block_hash(&self) -> &Hash32 {
        let off = self.trailer_offset as usize + 16;
        self.buf[off..off + 32].try_into().unwrap()
    }

    pub fn to_owned_log(&self) -> Log {
        Log {
            address: *self.address(),
            topics: self.topics().copied().collect(),
            data: self.data().to_vec(),
            block_num: self.block_num(),
            tx_idx: self.tx_idx(),
            log_idx: self.log_idx(),
            block_hash: *self.block_hash(),
        }
    }
}

impl std::fmt::Debug for LogRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogRef")
            .field("block_num", &self.block_num())
            .field("tx_idx", &self.tx_idx())
            .field("log_idx", &self.log_idx())
            .field("topic_count", &self.topic_count())
            .field("data_len", &self.data_len)
            .finish()
    }
}

impl PartialEq for LogRef {
    fn eq(&self, other: &Self) -> bool {
        self.buf == other.buf
    }
}

impl Eq for LogRef {}

/// Zero-copy view over an encoded block log header.
///
/// Wire layout:
///   version: u8 (must be 1)
///   count:   u32 BE
///   offsets: count * u32 BE
#[derive(Clone)]
pub struct BlockLogHeaderRef {
    buf: Bytes,
    count: u32,
}

impl BlockLogHeaderRef {
    pub fn new(buf: Bytes) -> Result<Self> {
        if buf.len() < 1 + 4 + 4 {
            return Err(Error::Decode("block log header too short"));
        }
        if buf[0] != 1 {
            return Err(Error::Decode("unsupported block log header version"));
        }
        let count = u32::from_be_bytes(
            buf[1..5]
                .try_into()
                .map_err(|_| Error::Decode("block log header count"))?,
        );
        if count < 2 {
            return Err(Error::Decode("block log header missing sentinel"));
        }
        let expected_len = 1 + 4 + (count as usize) * 4;
        if buf.len() != expected_len {
            return Err(Error::Decode("invalid block log header length"));
        }
        Ok(Self { buf, count })
    }

    pub fn count(&self) -> usize {
        self.count as usize
    }

    pub fn offset(&self, i: usize) -> u32 {
        assert!(i < self.count as usize, "offset index out of bounds");
        let pos = 5 + i * 4;
        u32::from_be_bytes(self.buf[pos..pos + 4].try_into().unwrap())
    }
}

impl std::fmt::Debug for BlockLogHeaderRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockLogHeaderRef")
            .field("count", &self.count)
            .finish()
    }
}

/// Zero-copy view over an encoded log directory bucket.
///
/// Wire layout:
///   version:       u8 (must be 1)
///   start_block:   u64 BE
///   count:         u32 BE
///   first_log_ids: count * u64 BE
#[derive(Clone)]
pub struct DirBucketRef {
    buf: Bytes,
    start_block: u64,
    count: u32,
}

impl DirBucketRef {
    pub fn new(buf: Bytes) -> Result<Self> {
        if buf.len() < 1 + 8 + 4 + 8 {
            return Err(Error::Decode("log directory bucket too short"));
        }
        if buf[0] != 1 {
            return Err(Error::Decode("unsupported log directory bucket version"));
        }
        let start_block = u64::from_be_bytes(
            buf[1..9]
                .try_into()
                .map_err(|_| Error::Decode("log directory bucket start_block"))?,
        );
        let count = u32::from_be_bytes(
            buf[9..13]
                .try_into()
                .map_err(|_| Error::Decode("log directory bucket count"))?,
        );
        if count < 2 {
            return Err(Error::Decode("log directory bucket missing sentinel"));
        }
        let expected_len = 1 + 8 + 4 + (count as usize) * 8;
        if buf.len() != expected_len {
            return Err(Error::Decode("invalid log directory bucket length"));
        }
        Ok(Self {
            buf,
            start_block,
            count,
        })
    }

    pub fn start_block(&self) -> u64 {
        self.start_block
    }

    pub fn count(&self) -> usize {
        self.count as usize
    }

    pub fn first_log_id(&self, i: usize) -> u64 {
        assert!(i < self.count as usize, "first_log_id index out of bounds");
        let pos = 13 + i * 8;
        u64::from_be_bytes(self.buf[pos..pos + 8].try_into().unwrap())
    }

    /// Binary search over the first_log_ids array.
    pub fn partition_point(&self, f: impl Fn(u64) -> bool) -> usize {
        let mut lo = 0usize;
        let mut hi = self.count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if f(self.first_log_id(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

impl std::fmt::Debug for DirBucketRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirBucketRef")
            .field("start_block", &self.start_block)
            .field("count", &self.count)
            .finish()
    }
}

/// Trait providing a uniform view over log data, implemented by both `Log` (owned)
/// and `LogRef` (zero-copy). This enforces compile-time consistency between the two
/// representations — adding a field to one without the other causes a build error
/// in code generic over `LogView`.
pub trait LogView {
    fn address(&self) -> &Address20;
    fn topic_count(&self) -> usize;
    fn topic(&self, i: usize) -> &Topic32;
    fn data(&self) -> &[u8];
    fn block_num(&self) -> u64;
    fn tx_idx(&self) -> u32;
    fn log_idx(&self) -> u32;
    fn block_hash(&self) -> &Hash32;
}

impl LogView for LogRef {
    fn address(&self) -> &Address20 {
        self.address()
    }
    fn topic_count(&self) -> usize {
        self.topic_count()
    }
    fn topic(&self, i: usize) -> &Topic32 {
        self.topic(i)
    }
    fn data(&self) -> &[u8] {
        self.data()
    }
    fn block_num(&self) -> u64 {
        self.block_num()
    }
    fn tx_idx(&self) -> u32 {
        self.tx_idx()
    }
    fn log_idx(&self) -> u32 {
        self.log_idx()
    }
    fn block_hash(&self) -> &Hash32 {
        self.block_hash()
    }
}

impl LogView for Log {
    fn address(&self) -> &Address20 {
        &self.address
    }
    fn topic_count(&self) -> usize {
        self.topics.len()
    }
    fn topic(&self, i: usize) -> &Topic32 {
        &self.topics[i]
    }
    fn data(&self) -> &[u8] {
        &self.data
    }
    fn block_num(&self) -> u64 {
        self.block_num
    }
    fn tx_idx(&self) -> u32 {
        self.tx_idx
    }
    fn log_idx(&self) -> u32 {
        self.log_idx
    }
    fn block_hash(&self) -> &Hash32 {
        &self.block_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_log() -> Log {
        Log {
            address: [7u8; 20],
            topics: vec![[1u8; 32], [2u8; 32]],
            data: vec![9, 8, 7],
            block_num: 12,
            tx_idx: 3,
            log_idx: 2,
            block_hash: [5u8; 32],
        }
    }

    #[test]
    fn log_ref_accessors() {
        let log = test_log();
        let encoded = log.encode();
        let log_ref = LogRef::new(encoded).expect("construct LogRef");

        assert_eq!(log_ref.address(), &log.address);
        assert_eq!(log_ref.topic_count(), 2);
        assert_eq!(log_ref.topic(0), &log.topics[0]);
        assert_eq!(log_ref.topic(1), &log.topics[1]);
        assert_eq!(log_ref.data(), &log.data[..]);
        assert_eq!(log_ref.block_num(), log.block_num);
        assert_eq!(log_ref.tx_idx(), log.tx_idx);
        assert_eq!(log_ref.log_idx(), log.log_idx);
        assert_eq!(log_ref.block_hash(), &log.block_hash);
    }

    #[test]
    fn log_view_trait_consistency() {
        let log = test_log();
        let encoded = log.encode();
        let log_ref = LogRef::new(encoded).expect("construct LogRef");

        fn check_view(view: &impl LogView, expected: &Log) {
            assert_eq!(view.address(), &expected.address);
            assert_eq!(view.topic_count(), expected.topics.len());
            for i in 0..expected.topics.len() {
                assert_eq!(view.topic(i), &expected.topics[i]);
            }
            assert_eq!(view.data(), &expected.data[..]);
            assert_eq!(view.block_num(), expected.block_num);
            assert_eq!(view.tx_idx(), expected.tx_idx);
            assert_eq!(view.log_idx(), expected.log_idx);
            assert_eq!(view.block_hash(), &expected.block_hash);
        }

        check_view(&log, &log);
        check_view(&log_ref, &log);
    }

    #[test]
    fn log_ref_zero_topics() {
        let log = Log {
            address: [1u8; 20],
            topics: vec![],
            data: vec![42],
            block_num: 1,
            tx_idx: 0,
            log_idx: 0,
            block_hash: [0u8; 32],
        };
        let encoded = log.encode();
        let log_ref = LogRef::new(encoded).expect("construct LogRef");
        assert_eq!(log_ref.to_owned_log(), log);
    }

    #[test]
    fn log_dir_bucket_ref_partition_point() {
        use crate::logs::types::DirBucket;

        let bucket = DirBucket {
            start_block: 0,
            first_log_ids: vec![10, 20, 30, 40],
        };
        let encoded = bucket.encode();
        let bucket_ref = DirBucketRef::new(encoded).expect("construct DirBucketRef");

        // partition_point with <= 25 should return 2 (ids 10, 20 satisfy <= 25)
        assert_eq!(bucket_ref.partition_point(|id| id <= 25), 2);
        // partition_point with <= 10 should return 1
        assert_eq!(bucket_ref.partition_point(|id| id <= 10), 1);
        // partition_point with <= 5 should return 0
        assert_eq!(bucket_ref.partition_point(|id| id <= 5), 0);
        // partition_point with <= 40 should return 4 (all satisfy)
        assert_eq!(bucket_ref.partition_point(|id| id <= 40), 4);
    }
}
