pub mod write_utils;

#[derive(thiserror::Error, Debug)]
pub enum PathError {
    #[error("Fatal error: {0}")]
    Generic(String),

    #[error("Cannot infer storage location from: {0}")]
    InvalidTableLocation(String),

    #[error("File already exists at the given location: {0}")]
    FileAlreadyExists(String),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum WriteModes {
    ErrorIfExists,
    Overwrite,
}
