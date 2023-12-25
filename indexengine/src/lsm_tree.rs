use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use bloomfilter::BloomFilter;
use serde::{Deserialize, Serialize};
use storageengine::operations::{DbOperations, NONE_SENTINEL, OffsetSize};

use crate::index::{Document, Index, IndexError};

pub struct LsmTree {
    map: BTreeMap<String, LsmMapLeaf>,
    db_operations: Box<dyn DbOperations>,
    transaction_id: u64,
    bloom_filter: BloomFilter,
    tree_size: usize,
    ss_table_path: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LsmMapLeaf {
    offset_size: OffsetSize,
    is_deleted: bool,
}

impl LsmTree {
    pub fn new(db_operations: Box<dyn DbOperations>, ss_table_path: String, tree_size: usize, bloom_filter_size: usize) -> Result<Self> {
        let mut lsm_tree = Self {
            map: BTreeMap::new(),
            db_operations,
            transaction_id: 0,
            bloom_filter: BloomFilter::new(bloom_filter_size),
            tree_size,
            ss_table_path,
        };
        lsm_tree.remove_all_files_ss_table()?;

        let mut offset = 0;
        let rows = lsm_tree.db_operations.read_all()?;
        for row in rows {
            // deleted
            if row.header.cmax != NONE_SENTINEL {
                offset += row.header.tuple_length;
                continue;
            }

            let doc: Document = bincode::deserialize(&row.data)?;

            lsm_tree.map.insert(doc.id.clone(), LsmMapLeaf {
                offset_size: OffsetSize {
                    offset,
                    size: row.header.tuple_length,
                },
                is_deleted: false,
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
            .collect::<Result<Vec<_>, std::io::Error>>()?.len();

        let file_name = format!("{}/ss_table_{}", self.ss_table_path, count);
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(true)
            .create(true)
            .open(file_name)?;
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
                fs::remove_file(path).expect("Unable to remove file");
            });

        Ok(())
    }

    fn search_in_files(&mut self, id: &str) -> Result<Document> {
        let files = fs::read_dir(&self.ss_table_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<PathBuf>, std::io::Error>>()?;

        for file in &files {
            let data = fs::read(file)?;
            let map: BTreeMap<String, LsmMapLeaf> = bincode::deserialize(&data)?;
            match map.get(id) {
                Some(lsm_map_leaf) => {
                    let row = self.db_operations.read_with_offset(&lsm_map_leaf.offset_size)?;
                    let doc: Document = bincode::deserialize(&row.data)?;
                    return Ok(doc);
                }
                None => continue,
            }
        }

        Err(IndexError::NotFound.into())
    }

    fn mark_ss_table_as_deleted(&mut self, id: &str) -> Result<()> {
        let files = fs::read_dir(&self.ss_table_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<PathBuf>, std::io::Error>>()?;

        for file in &files {
            let file_name = match file.file_name() {
                Some(name) => name.to_string_lossy().into_owned(),
                None => continue,
            };

            let data = fs::read(file)?;
            let mut map: BTreeMap<String, LsmMapLeaf> = bincode::deserialize(&data)?;
            match map.get_mut(id) {
                Some(lsm_map_leaf) => {
                    if lsm_map_leaf.is_deleted {
                        continue;
                    }

                    self.db_operations.delete_with_offset(&lsm_map_leaf.offset_size, self.transaction_id)?;
                    self.transaction_id += 1;

                    lsm_map_leaf.is_deleted = true;
                    // flush to disk
                    let lsm_tree_data = bincode::serialize(&map)?;
                    let mut f = OpenOptions::new()
                        .write(true)
                        .read(true)
                        .append(true)
                        .create(true)
                        .open(file_name)?;
                    f.write_all(&lsm_tree_data)?;
                    f.flush()?;
                    return Ok(());
                }
                None => continue,
            }
        }

        Err(IndexError::NotFound.into())
    }

    fn update_ss_table(&mut self, id: &str, document: Document) -> Result<()> {
        match self.mark_ss_table_as_deleted(id) {
            Ok(_) => {
                self.insert(document)?;
                Ok(())
            }
            Err(err) => {
                Err(err)
            }
        }
    }
}

impl Index for LsmTree {
    fn insert(&mut self, document: Document) -> Result<()> {
        let data = bincode::serialize(&document)?;
        let offset_size = self.db_operations.insert(data, self.transaction_id)?;
        self.map.insert(document.id, LsmMapLeaf {
            offset_size,
            is_deleted: false,
        });
        if self.map.len() == self.tree_size {
            self.flush_tree_to_disk()?;
        }
        self.transaction_id += 1;

        Ok(())
    }

    fn search(&mut self, id: &str) -> Result<Document> {
        // if item is not in bloom filter, it is not in the db
        if !self.bloom_filter.check(id) {
            return Err(IndexError::NotFound.into());
        }

        match self.map.get(id) {
            // item is in memory
            Some(lsm_map_leaf) => {
                let row = self.db_operations.read_with_offset(&lsm_map_leaf.offset_size)?;
                let doc: Document = bincode::deserialize(&row.data)?;
                Ok(doc)
            }
            None => {
                // item is not in memory, search in all files
                self.search_in_files(id)
            }
        }
    }

    fn delete(&mut self, id: &str) -> Result<()> {
        if !self.bloom_filter.check(id) {
            return Err(IndexError::NotFound.into());
        }

        match self.map.get(id) {
            Some(lsm_map_leaf) => {
                self.db_operations.delete_with_offset(&lsm_map_leaf.offset_size, self.transaction_id)?;
                self.map.remove(id);
                self.transaction_id += 1;
                Ok(())
            }
            None => {
                self.mark_ss_table_as_deleted(id)
            }
        }
    }

    fn update(&mut self, id: &str, document: Document) -> Result<()> {
        if !self.bloom_filter.check(id) {
            return Err(IndexError::NotFound.into());
        }

        match self.map.get(id) {
            Some(lsm_map_leaf) => {
                let data = bincode::serialize(&document)?;
                let new_offset_size = self.db_operations.update_with_offset(&lsm_map_leaf.offset_size, data, self.transaction_id)?;
                self.map.insert(document.id, LsmMapLeaf {
                    offset_size: new_offset_size,
                    is_deleted: false,
                });
                self.transaction_id += 1;
                Ok(())
            }
            None => {
                self.update_ss_table(id, document)
            }
        }
    }
}
