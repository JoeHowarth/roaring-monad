#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryIdRange {
    pub start: u64,
    pub end_inclusive: u64,
}

impl PrimaryIdRange {
    pub fn new(start: u64, end_inclusive: u64) -> Option<Self> {
        (start <= end_inclusive).then_some(Self {
            start,
            end_inclusive,
        })
    }

    pub fn contains(&self, id: u64) -> bool {
        self.start <= id && id <= self.end_inclusive
    }

    pub fn resume_strictly_after(&self, id: u64) -> Option<Self> {
        let next_start = id.saturating_add(1);
        Self::new(next_start, self.end_inclusive)
    }
}
