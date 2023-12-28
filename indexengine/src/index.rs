use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Document<K, V> {
    pub id: K,
    pub value: V,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("NotFound")]
    NotFound,
    #[error("AlreadyExists")]
    AlreadyExists,
}

pub trait Index<K, V> {
    fn insert(&mut self, document: Document<K, V>) -> Result<()>;
    fn search(&mut self, id: &K) -> Result<Document<K, V>>;
    fn delete(&mut self, id: &K) -> Result<()>;
    fn update(&mut self, id: &K, document: Document<K, V>) -> Result<()>;
}
