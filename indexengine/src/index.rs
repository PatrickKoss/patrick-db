use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Document {
    pub id: String,
    pub value: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("NotFound")]
    NotFound,
    #[error("AlreadyExists")]
    AlreadyExists,
}

pub trait Index {
    fn insert(&mut self, document: Document) -> Result<()>;
    fn search(&mut self, id: &str) -> Result<Document>;
    fn delete(&mut self, id: &str) -> Result<()>;
    fn update(&mut self, id: &str, document: Document) -> Result<()>;
}
