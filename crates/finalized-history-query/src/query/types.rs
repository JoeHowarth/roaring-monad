use crate::core::ids::FamilyIdValue;

pub(crate) trait PrimaryId: FamilyIdValue + Copy + Ord {}

impl<T: FamilyIdValue> PrimaryId for T {}
