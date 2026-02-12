use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("input invalid: {0}")]
    InputInvalid(String),
    #[error("config invalid: {0}")]
    ConfigInvalid(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}
