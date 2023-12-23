use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use storageengine::operations::{DbOperations, OffsetSize, NONE_SENTINEL};
use anyhow::Result;
use crate::index::{Document, Index, IndexError};
use bloomfilter::BloomFilter;

pub struct LsmTree {
    map: BTreeMap<String, OffsetSize>,
    db_operations: Box<dyn DbOperations>,
    transaction_id: u64,
    bloom_filter: BloomFilter,
    tree_size: usize,
    ss_table_path: String,
}

impl LsmTree {
    pub fn new(mut db_operations: Box<dyn DbOperations>, ss_table_path: String, tree_size: usize, bloom_filter_size: usize) -> Result<Self> {
        let lsm_tree = Self {
            map: BTreeMap::new(),
            db_operations,
            transaction_id: 0,
            bloom_filter: BloomFilter::new(bloom_filter_size),
            tree_size,
            ss_table_path,
        };
        lsm_tree.remove_all_files_ss_table()?;

        let mut offset = 0;
        let rows = db_operations.read_all()?;
        for row in rows {
            // deleted
            if row.header.cmax != NONE_SENTINEL {
                offset += row.header.tuple_length;
                continue;
            }

            let doc: Document = bincode::deserialize(&row.data)?;

            lsm_tree.map.insert(doc.id, OffsetSize {
                offset,
                size: row.header.tuple_length,
            });
            lsm_tree.bloom_filter.insert(doc.id);
            if lsm_tree.map.len() == lsm_tree.tree_size {
                lsm_tree.flush_tree_to_disk()?;
            }

            offset += row.header.tuple_length;
        }

        lsm_tree.transaction_id = lsm_tree.map.len() as u64;

        Ok(lsm_tree)
    }

    fn flush_tree_to_disk(&mut self) -> Result<()> {
        let data = bincode::serialize(&self.map)?;

        let count = fs::read_dir(&self.ss_table_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?
            .iter()
            .count();

        let file_name = format!("{}/ss_table_{}", self.ss_table_path, count);
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(true)
            .create(true)
            .open(filename)?;
        file.write_all(&data)?;
        file.flush()?;

        self.map.clear();

        Ok(())
    }

    fn remove_all_files_ss_table(&mut self) -> Result<()> {
        fs::read_dir(&self.ss_table_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?
            .iter()
            .for_each(|path| {
                fs::remove_file(path)?;
            });

        Ok(())
    }
}

impl Index for LsmTree {
    fn insert(&mut self, document: Document) -> Result<()> {
        let data = bincode::serialize(&document)?;
        let offset_size = self.db_operations.insert(data, self.transaction_id)?;
        self.map.insert(document.id, offset_size);
        self.transaction_id += 1;
        Ok(())
    }

    fn search(&mut self, id: &str) -> Result<Document> {
        if !self.bloom_filter.check(id) {
            return Err(IndexError::NotFound.into());
        }
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
