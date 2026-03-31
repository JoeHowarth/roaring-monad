use thiserror::Error;

pub type Result<T> = std::result::Result<T, MonadChainDataError>;

#[derive(Debug, Error)]
pub enum MonadChainDataError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("decode error: {0}")]
    Decode(&'static str),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("missing data: {0}")]
    MissingData(&'static str),
}
