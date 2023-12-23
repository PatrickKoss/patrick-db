use std::collections::BTreeMap;
use storageengine::operations::{DbOperations, OffsetSize, NONE_SENTINEL};
use anyhow::Result;
use crate::index::{Document, Index, IndexError};

pub struct BTree {
    map: BTreeMap<String, OffsetSize>,
    db_operations: Box<dyn DbOperations>,
    transaction_id: u64,
}

impl BTree {
    pub fn new(mut db_operations: Box<dyn DbOperations>) -> Result<Self> {
        let mut map = BTreeMap::new();
        let mut offset = 0;
        let rows = db_operations.read_all()?;
        for row in rows {
            // deleted
            if row.header.cmax != NONE_SENTINEL {
                offset += row.header.tuple_length;
                continue;
            }

            let doc: Document = bincode::deserialize(&row.data)?;

            map.insert(doc.id, OffsetSize {
                offset,
                size: row.header.tuple_length,
            });

            offset += row.header.tuple_length;
        }

        let map_len = map.len();

        Ok(Self {
            map,
            db_operations,
            transaction_id: map_len as u64,
        })
    }
}

impl Index for BTree {
    fn insert(&mut self, document: Document) -> Result<()> {
        let data = bincode::serialize(&document)?;
        let offset_size = self.db_operations.insert(data, self.transaction_id)?;
        self.map.insert(document.id, offset_size);
        self.transaction_id += 1;
        Ok(())
    }

    fn search(&mut self, id: &str) -> Result<Document> {
        match self.map.get(id) {
            Some(offset_size) => {
                let row = self.db_operations.read_with_offset(offset_size)?;
                let doc: Document = bincode::deserialize(&row.data)?;
                Ok(doc)
            },
            None => Err(IndexError::NotFound.into()),
        }
    }

    fn delete(&mut self, id: &str) -> Result<()> {
        match self.map.get(id) {
            Some(offset_size) => {
                self.db_operations.delete_with_offset(offset_size, self.transaction_id)?;
                self.map.remove(id);
                self.transaction_id += 1;
                Ok(())
            },
            None => Err(IndexError::NotFound.into()),
        }
    }

    fn update(&mut self, id: &str, document: Document) -> Result<()> {
        match self.map.get(id) {
            Some(offset_size) => {
                let data = bincode::serialize(&document)?;
                let new_offset_size = self.db_operations.update_with_offset(offset_size, data, self.transaction_id)?;
                self.map.insert(document.id, new_offset_size);
                self.transaction_id += 1;
                Ok(())
            },
            None => Err(IndexError::NotFound.into()),
        }
    }
}
