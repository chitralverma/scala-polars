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

impl From<arrow2::error::Error> for PathError {
    fn from(error: arrow2::error::Error) -> Self {
        Self::Generic(error.to_string())
    }
}

impl From<arrow2::io::avro::avro_schema::error::Error> for PathError {
    fn from(error: arrow2::io::avro::avro_schema::error::Error) -> Self {
        Self::Generic(error.to_string())
    }
}

impl From<object_store::Error> for PathError {
    fn from(error: object_store::Error) -> Self {
        Self::Generic(error.to_string())
    }
}

impl From<std::io::Error> for PathError {
    fn from(error: std::io::Error) -> Self {
        Self::Generic(error.to_string())
    }
}

impl From<polars::error::PolarsError> for PathError {
    fn from(error: polars::error::PolarsError) -> Self {
        Self::Generic(error.to_string())
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum WriteModes {
    ErrorIfExists,
    Overwrite,
}
