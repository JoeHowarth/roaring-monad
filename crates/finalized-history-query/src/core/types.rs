// --- clause ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Clause<T> {
    Any,
    One(T),
    Or(Vec<T>),
}

impl<T> Clause<T> {
    pub fn or_terms(&self) -> usize {
        match self {
            Self::Any => 0,
            Self::One(_) => 1,
            Self::Or(values) => values.len(),
        }
    }
}

// --- refs ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

impl BlockRef {
    pub const fn zero(number: u64) -> Self {
        Self {
            number,
            hash: [0; 32],
            parent_hash: [0; 32],
        }
    }
}

// --- page ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOrder {
    Ascending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPageMeta {
    pub resolved_from_block: BlockRef,
    pub resolved_to_block: BlockRef,
    pub cursor_block: BlockRef,
    pub has_more: bool,
    pub next_resume_log_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPage<T> {
    pub items: Vec<T>,
    pub meta: QueryPageMeta,
}
