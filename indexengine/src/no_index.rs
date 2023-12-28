use std::hash::Hash;
use crate::index::{Document, Index, IndexError};
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct NoIndex {
    db_operations: Box<dyn storageengine::operations::DbOperations>,
    transaction_id: u64,
}

impl NoIndex {
    pub fn new(db_operations: Box<dyn storageengine::operations::DbOperations>) -> Self {
        Self {
            db_operations,
            transaction_id: 0,
        }
    }
}

impl<K, V> Index<K, V> for NoIndex where K: Serialize + DeserializeOwned + Hash + Eq, V: Serialize + DeserializeOwned {
    fn insert(&mut self, document: Document<K, V>) -> Result<()> {
        let data = bincode::serialize(&document)?;
        self.db_operations.insert(data, self.transaction_id)?;
        self.transaction_id += 1;
        Ok(())
    }

    fn search(&mut self, id: &K) -> Result<Document<K, V>> {
        let rows = self.db_operations.read_all()?;
        for row in rows {
            let doc: Document<K, V> = bincode::deserialize(&row.data)?;
            if doc.id == *id {
                return Ok(doc);
            }
        }

        Err(IndexError::NotFound.into())
    }

    fn delete(&mut self, id: &K) -> Result<()> {
        let rows = self.db_operations.read_all()?;
        let mut offset_size = storageengine::operations::OffsetSize {
            offset: 0,
            size: 0,
        };

        for row in rows {
            offset_size.size = row.header.tuple_length;
            let doc: Document<K, V> = bincode::deserialize(&row.data)?;
            if doc.id == *id {
                match self.db_operations.delete_with_offset(&offset_size, self.transaction_id) {
                    Ok(_) => {
                        self.transaction_id += 1;
                        return Ok(());
                    },
                    Err(e) => return Err(e),
                }
            }
            offset_size.offset += row.header.tuple_length;
        }

        Err(IndexError::NotFound.into())
    }

    fn update(&mut self, id: &K, document: Document<K, V>) -> Result<()> {
        let rows = self.db_operations.read_all()?;
        let data = bincode::serialize(&document)?;
        let mut offset_size = storageengine::operations::OffsetSize {
            offset: 0,
            size: 0,
        };

        for row in rows {
            offset_size.size = row.header.tuple_length;
            let doc: Document<K, V> = bincode::deserialize(&row.data)?;
            if doc.id == *id {
                match self.db_operations.update_with_offset(&offset_size, data, self.transaction_id) {
                    Ok(_) => {
                        self.transaction_id += 1;
                        return Ok(());
                    },
                    Err(e) => return Err(e),
                }
            }
            offset_size.offset += row.header.tuple_length;
        }

        Err(IndexError::NotFound.into())
    }
}
