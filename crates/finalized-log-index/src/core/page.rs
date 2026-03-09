use crate::core::refs::BlockRef;

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
