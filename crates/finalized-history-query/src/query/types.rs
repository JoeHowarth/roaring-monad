use crate::core::ids::{FamilyIdRange, FamilyIdValue};

pub(crate) trait PrimaryId: Copy + Ord {
    fn new(raw: u64) -> Self;
    fn get(self) -> u64;
}

impl<T: FamilyIdValue> PrimaryId for T {
    fn new(raw: u64) -> Self {
        FamilyIdValue::new(raw)
    }

    fn get(self) -> u64 {
        FamilyIdValue::get(self)
    }
}

pub(crate) trait PrimaryRange: Copy {
    type Id: PrimaryId;

    fn new(start: Self::Id, end_inclusive: Self::Id) -> Option<Self>;
    fn contains(self, id: Self::Id) -> bool;
    fn resume_strictly_after(self, id: Self::Id) -> Option<Self>;
}

impl<T: FamilyIdValue> PrimaryRange for FamilyIdRange<T> {
    type Id = T;

    fn new(start: Self::Id, end_inclusive: Self::Id) -> Option<Self> {
        FamilyIdRange::new(start, end_inclusive)
    }

    fn contains(self, id: Self::Id) -> bool {
        FamilyIdRange::contains(&self, id)
    }

    fn resume_strictly_after(self, id: Self::Id) -> Option<Self> {
        FamilyIdRange::resume_strictly_after(&self, id)
    }
}

pub(crate) trait BlockWindow<I: PrimaryId>: Copy {
    fn first_id(self) -> I;
    fn count(self) -> u32;
}
