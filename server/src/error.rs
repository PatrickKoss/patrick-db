use std::fmt::Display;
use prost::DecodeError;

use tonic::Status;
use indexengine::index::IndexError;

use indexengine::index::IndexError::AlreadyExists;
use indexengine::index::IndexError::NotFound;

#[derive(Debug)]
pub enum ServerError {
    IndexError(IndexError),
    InternalError(anyhow::Error),
    InvalidArgument(String),
    Internal(String),
    DecodeError(DecodeError),
}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::IndexError(e) => write!(f, "IndexError: {:?}", e),
            ServerError::InternalError(e) => write!(f, "InternalError: {}", e),
            ServerError::InvalidArgument(e) => write!(f, "InvalidArgument: {}", e),
            ServerError::Internal(e) => write!(f, "Internal: {}", e),
            ServerError::DecodeError(e) => write!(f, "DecodeError: {}", e),
        }
    }
}

impl From<ServerError> for Status {
    fn from(err: ServerError) -> Self {
        match err {
            ServerError::IndexError(e) => match e {
                AlreadyExists => Status::already_exists("document already exists"),
                NotFound => Status::not_found("document not found"),
            },
            ServerError::InternalError(e) => Status::internal(e.to_string()),
            ServerError::InvalidArgument(e) => Status::invalid_argument(e.to_string()),
            ServerError::Internal(e) => Status::internal(e.to_string()),
            ServerError::DecodeError(e) => Status::invalid_argument(e.to_string()),
        }
    }
}

impl From<IndexError> for ServerError {
    fn from(err: IndexError) -> Self {
        ServerError::IndexError(err)
    }
}

impl From<anyhow::Error> for ServerError {
    fn from(e: anyhow::Error) -> Self {
        e.downcast::<IndexError>()
            .map(ServerError::IndexError)
            .unwrap_or_else(ServerError::InternalError)
    }
}

impl From<DecodeError> for ServerError {
    fn from(e: DecodeError) -> Self {
        ServerError::DecodeError(e)
    }
}
