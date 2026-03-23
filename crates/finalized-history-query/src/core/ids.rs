use crate::logs::keys::{LOCAL_ID_BITS, LOCAL_ID_MASK, MAX_LOCAL_ID};

const MAX_LOG_SHARD: u64 = u64::MAX >> LOCAL_ID_BITS;
const MAX_TRACE_SHARD: u64 = u64::MAX >> LOCAL_ID_BITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId(u64);

impl LogId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn shard(self) -> LogShard {
        LogShard::new_masked(self.0 >> LOCAL_ID_BITS)
    }

    pub const fn local(self) -> LogLocalId {
        LogLocalId::new_masked((self.0 & LOCAL_ID_MASK) as u32)
    }

    pub const fn split(self) -> (LogShard, LogLocalId) {
        (self.shard(), self.local())
    }
}

impl From<u64> for LogId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<LogId> for u64 {
    fn from(value: LogId) -> Self {
        value.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogShard(u64);

impl LogShard {
    pub fn new(raw: u64) -> Result<Self, InvalidLogShard> {
        if raw <= MAX_LOG_SHARD {
            Ok(Self(raw))
        } else {
            Err(InvalidLogShard { raw })
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn new_masked(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidLogShard {
    raw: u64,
}

impl InvalidLogShard {
    pub const fn raw(self) -> u64 {
        self.raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidLogLocalId {
    raw: u32,
}

impl InvalidLogLocalId {
    pub const fn raw(self) -> u32 {
        self.raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogLocalId(u32);

impl LogLocalId {
    pub fn new(raw: u32) -> Result<Self, InvalidLogLocalId> {
        if raw <= MAX_LOCAL_ID {
            Ok(Self(raw))
        } else {
            Err(InvalidLogLocalId { raw })
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub(crate) const fn new_masked(raw: u32) -> Self {
        Self(raw)
    }
}

pub const fn compose_log_id(shard: LogShard, local: LogLocalId) -> LogId {
    LogId::new((shard.get() << LOCAL_ID_BITS) | (local.get() as u64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TraceId(u64);

impl TraceId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn shard(self) -> TraceShard {
        TraceShard::new_masked(self.0 >> LOCAL_ID_BITS)
    }

    pub const fn local(self) -> TraceLocalId {
        TraceLocalId::new_masked((self.0 & LOCAL_ID_MASK) as u32)
    }

    pub const fn split(self) -> (TraceShard, TraceLocalId) {
        (self.shard(), self.local())
    }
}

impl From<u64> for TraceId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<TraceId> for u64 {
    fn from(value: TraceId) -> Self {
        value.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TraceShard(u64);

impl TraceShard {
    pub fn new(raw: u64) -> Result<Self, InvalidTraceShard> {
        if raw <= MAX_TRACE_SHARD {
            Ok(Self(raw))
        } else {
            Err(InvalidTraceShard { raw })
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn new_masked(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTraceShard {
    raw: u64,
}

impl InvalidTraceShard {
    pub const fn raw(self) -> u64 {
        self.raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTraceLocalId {
    raw: u32,
}

impl InvalidTraceLocalId {
    pub const fn raw(self) -> u32 {
        self.raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TraceLocalId(u32);

impl TraceLocalId {
    pub fn new(raw: u32) -> Result<Self, InvalidTraceLocalId> {
        if raw <= MAX_LOCAL_ID {
            Ok(Self(raw))
        } else {
            Err(InvalidTraceLocalId { raw })
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub(crate) const fn new_masked(raw: u32) -> Self {
        Self(raw)
    }
}

pub const fn compose_trace_id(shard: TraceShard, local: TraceLocalId) -> TraceId {
    TraceId::new((shard.get() << LOCAL_ID_BITS) | (local.get() as u64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryIdRange {
    pub start: LogId,
    pub end_inclusive: LogId,
}

impl PrimaryIdRange {
    pub fn new(start: LogId, end_inclusive: LogId) -> Option<Self> {
        (start <= end_inclusive).then_some(Self {
            start,
            end_inclusive,
        })
    }

    pub fn contains(&self, id: LogId) -> bool {
        self.start <= id && id <= self.end_inclusive
    }

    pub fn resume_strictly_after(&self, id: LogId) -> Option<Self> {
        let next_start = id.get().checked_add(1).map(LogId::new)?;
        Self::new(next_start, self.end_inclusive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceIdRange {
    pub start: TraceId,
    pub end_inclusive: TraceId,
}

impl TraceIdRange {
    pub fn new(start: TraceId, end_inclusive: TraceId) -> Option<Self> {
        (start <= end_inclusive).then_some(Self {
            start,
            end_inclusive,
        })
    }

    pub fn contains(&self, id: TraceId) -> bool {
        self.start <= id && id <= self.end_inclusive
    }

    pub fn resume_strictly_after(&self, id: TraceId) -> Option<Self> {
        let next_start = id.get().checked_add(1).map(TraceId::new)?;
        Self::new(next_start, self.end_inclusive)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LogId, LogLocalId, LogShard, PrimaryIdRange, TraceId, TraceIdRange, TraceLocalId,
        TraceShard, compose_log_id, compose_trace_id,
    };
    use crate::logs::keys::MAX_LOCAL_ID;

    #[test]
    fn log_id_roundtrips_at_boundaries() {
        let values = [
            LogId::new(0),
            LogId::new(1),
            LogId::new(u64::from(MAX_LOCAL_ID)),
            LogId::new(u64::from(MAX_LOCAL_ID) + 1),
            compose_log_id(
                LogShard::new(u64::from(u32::MAX) + 1).unwrap(),
                LogLocalId::new(7).unwrap(),
            ),
        ];

        for value in values {
            let (shard, local) = value.split();
            assert_eq!(compose_log_id(shard, local), value);
        }
    }

    #[test]
    fn primary_id_range_uses_typed_log_ids() {
        let range = PrimaryIdRange::new(LogId::new(10), LogId::new(12)).expect("valid range");
        assert!(range.contains(LogId::new(10)));
        assert!(range.contains(LogId::new(12)));
        assert!(!range.contains(LogId::new(13)));
        assert_eq!(
            range.resume_strictly_after(LogId::new(10)),
            Some(PrimaryIdRange {
                start: LogId::new(11),
                end_inclusive: LogId::new(12),
            })
        );
    }

    #[test]
    fn resume_strictly_after_returns_none_at_u64_max() {
        let range =
            PrimaryIdRange::new(LogId::new(u64::MAX), LogId::new(u64::MAX)).expect("valid range");
        assert_eq!(range.resume_strictly_after(LogId::new(u64::MAX)), None);
    }

    #[test]
    fn trace_id_roundtrips_at_boundaries() {
        let values = [
            TraceId::new(0),
            TraceId::new(1),
            TraceId::new(u64::from(MAX_LOCAL_ID)),
            TraceId::new(u64::from(MAX_LOCAL_ID) + 1),
            compose_trace_id(
                TraceShard::new(u64::from(u32::MAX) + 1).unwrap(),
                TraceLocalId::new(7).unwrap(),
            ),
        ];

        for value in values {
            let (shard, local) = value.split();
            assert_eq!(compose_trace_id(shard, local), value);
        }
    }

    #[test]
    fn trace_id_range_uses_typed_trace_ids() {
        let range = TraceIdRange::new(TraceId::new(10), TraceId::new(12)).expect("valid range");
        assert!(range.contains(TraceId::new(10)));
        assert!(range.contains(TraceId::new(12)));
        assert!(!range.contains(TraceId::new(13)));
        assert_eq!(
            range.resume_strictly_after(TraceId::new(10)),
            Some(TraceIdRange {
                start: TraceId::new(11),
                end_inclusive: TraceId::new(12),
            })
        );
    }
}
