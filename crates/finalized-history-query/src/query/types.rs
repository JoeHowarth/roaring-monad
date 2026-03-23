use crate::core::ids::{LogId, PrimaryIdRange, TraceId, TraceIdRange};

pub(crate) trait PrimaryId: Copy + Ord {
    fn new(raw: u64) -> Self;
    fn get(self) -> u64;
}

impl PrimaryId for LogId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }
}

impl PrimaryId for TraceId {
    fn new(raw: u64) -> Self {
        Self::new(raw)
    }

    fn get(self) -> u64 {
        self.get()
    }
}

pub(crate) trait PrimaryRange: Copy {
    type Id: PrimaryId;

    fn new(start: Self::Id, end_inclusive: Self::Id) -> Option<Self>;
    fn contains(self, id: Self::Id) -> bool;
    fn resume_strictly_after(self, id: Self::Id) -> Option<Self>;
}

impl PrimaryRange for PrimaryIdRange {
    type Id = LogId;

    fn new(start: Self::Id, end_inclusive: Self::Id) -> Option<Self> {
        Self::new(start, end_inclusive)
    }

    fn contains(self, id: Self::Id) -> bool {
        PrimaryIdRange::contains(&self, id)
    }

    fn resume_strictly_after(self, id: Self::Id) -> Option<Self> {
        PrimaryIdRange::resume_strictly_after(&self, id)
    }
}

impl PrimaryRange for TraceIdRange {
    type Id = TraceId;

    fn new(start: Self::Id, end_inclusive: Self::Id) -> Option<Self> {
        Self::new(start, end_inclusive)
    }

    fn contains(self, id: Self::Id) -> bool {
        TraceIdRange::contains(&self, id)
    }

    fn resume_strictly_after(self, id: Self::Id) -> Option<Self> {
        TraceIdRange::resume_strictly_after(&self, id)
    }
}

pub(crate) trait BlockWindow<I: PrimaryId>: Copy {
    fn first_id(self) -> I;
    fn count(self) -> u32;
}
