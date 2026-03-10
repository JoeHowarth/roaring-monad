#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("not found")]
    NotFound,
    #[error("cas conflict")]
    CasConflict,
    #[error("lease lost")]
    LeaseLost,
    #[error("invalid finalized sequence: expected {expected}, got {got}")]
    InvalidSequence { expected: u64, got: u64 },
    #[error("invalid parent linkage")]
    InvalidParent,
    #[error("finality violation")]
    FinalityViolation,
    #[error("service degraded: {0}")]
    Degraded(String),
    #[error("service throttled: {0}")]
    Throttled(String),
    #[error("invalid params: {0}")]
    InvalidParams(&'static str),
    #[error("decode error: {0}")]
    Decode(&'static str),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    #[error("query too broad: clause has {actual} OR terms, max allowed is {max}")]
    QueryTooBroad { actual: usize, max: usize },
}

pub type Result<T> = core::result::Result<T, Error>;
