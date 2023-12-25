use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use bloomfilter::BloomFilter;
#[cfg(test)]
use mockall::{mock, predicate::*};
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

        fs::create_dir_all(&self.ss_table_path)?;

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
                    self.bloom_filter.remove(id);

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
        // check if item is already present
        if self.search(&document.id).is_ok() {
            return Err(IndexError::AlreadyExists.into());
        }

        let data = bincode::serialize(&document)?;
        let offset_size = self.db_operations.insert(data, self.transaction_id)?;
        self.map.insert(document.id.clone(), LsmMapLeaf {
            offset_size,
            is_deleted: false,
        });
        if self.map.len() == self.tree_size {
            self.flush_tree_to_disk()?;
        }
        self.bloom_filter.insert(document.id);
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
                self.bloom_filter.remove(id);
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use mockall::predicate;
    use storageengine::operations::{Header, Row};
    #[cfg(test)]
    use tempfile::tempdir;

    use super::*;

    mock! {
        DbOperationsImpl {}
        impl DbOperations for DbOperationsImpl {
            fn insert(&mut self, data: Vec<u8>, transaction_id: u64) -> Result<OffsetSize>;
            fn read_with_offset(&mut self, offset_size: &OffsetSize) -> Result<Row>;
            fn read_all(&mut self) -> Result<Vec<Row>>;
            fn update_with_offset(&mut self, old_offset_size: &OffsetSize, data: Vec<u8>, transaction_id: u64) -> Result<OffsetSize>;
            fn delete_with_offset(&mut self, offset_size: &OffsetSize, transaction_id: u64) -> Result<()>;
        }
    }

    #[test]
    fn insert_adds_document() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_insert()
            .with(predicate::eq(data.clone()), predicate::eq(0 as u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));
        mock.expect_read_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 0, size: 3 }))
            .times(1)
            .returning(move |_| Ok(Row {
                header: Header {
                    xmin: 0,
                    cmax: NONE_SENTINEL,
                    xmax: NONE_SENTINEL,
                    tuple_length: 3,
                    table_oid: 0,
                    ctid: 0,
                    cmin: 0,
                },
                data: data.clone(),
            }));

        let mut lsm_tree = setup_lsm_tree(mock)?;

        let res = lsm_tree.insert(document.clone());

        assert!(res.is_ok());
        assert!(!lsm_tree.map.get(&document.id).unwrap().is_deleted);
        assert!(lsm_tree.bloom_filter.check(&document.id));
        assert!(lsm_tree.map.get(&document.id).unwrap().offset_size.offset == 0);

        let doc = lsm_tree.search(&document.id)?;
        assert_eq!(doc, document);

        Ok(())
    }

    #[test]
    fn insert_adds_document_flush_ss_table() -> Result<()> {
        let document = Document { id: "50".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_read_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 50 * 3, size: 51 * 3 }))
            .times(2)
            .returning(move |_| Ok(Row {
                header: Header {
                    xmin: 0,
                    cmax: NONE_SENTINEL,
                    xmax: NONE_SENTINEL,
                    tuple_length: 3,
                    table_oid: 0,
                    ctid: 0,
                    cmin: 0,
                },
                data: data.clone(),
            }));

        mock.expect_insert()
            .with(predicate::always(), predicate::always())
            .times(101)
            .returning(move |_, _| Ok(OffsetSize { offset: 50 * 3, size: (50 + 1) * 3 }));
        mock.expect_delete_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 50 * 3, size: (50 + 1) * 3 }), predicate::always())
            .times(1)
            .returning(move |_, _| Ok(()));

        let mut lsm_tree = setup_lsm_tree(mock)?;

        for i in 0..100 {
            let document = Document { id: i.to_string(), value: vec![1, 2, 3] };
            let res = lsm_tree.insert(document);
            assert!(res.is_ok());
        }

        let doc = lsm_tree.search("50")?;
        assert_eq!(doc, document);

        let updated_document = Document { id: "50".to_string(), value: vec![4, 5, 6] };
        lsm_tree.update("50", updated_document.clone())?;
        let res = lsm_tree.search("50");
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn search_returns_document_if_in_memory() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_insert()
            .with(predicate::eq(data.clone()), predicate::eq(0 as u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));
        mock.expect_read_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 0, size: 3 }))
            .times(1)
            .returning(move |_| Ok(Row {
                header: Header {
                    xmin: 0,
                    cmax: NONE_SENTINEL,
                    xmax: NONE_SENTINEL,
                    tuple_length: 3,
                    table_oid: 0,
                    ctid: 0,
                    cmin: 0,
                },
                data: data.clone(),
            }));

        let mut lsm_tree = setup_lsm_tree(mock)?;

        lsm_tree.insert(document.clone())?;

        let doc = lsm_tree.search(&document.id)?;

        assert_eq!(doc, document);

        Ok(())
    }

    #[test]
    fn search_returns_error_if_not_found() -> Result<()> {
        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));

        let mut lsm_tree = setup_lsm_tree(mock)?;

        let result = lsm_tree.search("1");

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn delete_removes_document_from_map() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_insert()
            .with(predicate::eq(data.clone()), predicate::eq(0 as u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));
        mock.expect_delete_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 0, size: 3 }), predicate::eq(1 as u64))
            .times(1)
            .returning(move |_, _| Ok(()));

        let mut lsm_tree = setup_lsm_tree(mock)?;
        lsm_tree.insert(document.clone())?;
        lsm_tree.delete(&document.id)?;

        assert!(lsm_tree.map.get(&document.id).is_none());

        Ok(())
    }

    #[test]
    fn delete_returns_error_if_not_found() -> Result<()> {
        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));

        let mut lsm_tree = setup_lsm_tree(mock)?;

        let result = lsm_tree.delete("1");

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn update_modifies_existing_document() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let updated_document = Document { id: "1".to_string(), value: vec![4, 5, 6] };
        let updated_data = bincode::serialize(&updated_document)?;
        let updated_data_clone = bincode::serialize(&updated_document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_insert()
            .with(predicate::eq(data.clone()), predicate::eq(0 as u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));
        mock.expect_read_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 3, size: 3 }))
            .times(1)
            .returning(move |_| Ok(Row {
                header: Header {
                    xmin: 0,
                    cmax: NONE_SENTINEL,
                    xmax: NONE_SENTINEL,
                    tuple_length: 3,
                    table_oid: 0,
                    ctid: 0,
                    cmin: 0,
                },
                data: updated_data_clone.clone(),
            }));
        mock.expect_update_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 0, size: 3 }), predicate::eq(updated_data), predicate::eq(1 as u64))
            .times(1)
            .returning(move |_, _, _| Ok(OffsetSize { offset: 3, size: 3 }));

        let mut lsm_tree = setup_lsm_tree(mock)?;
        lsm_tree.insert(document.clone()).unwrap();

        lsm_tree.update(&updated_document.id, updated_document.clone())?;

        assert!(lsm_tree.map.get(&updated_document.id).is_some());
        assert!(lsm_tree.map.get(&updated_document.id).unwrap().offset_size.offset == 3);

        let doc = lsm_tree.search(&updated_document.id)?;
        assert_eq!(doc, updated_document);

        Ok(())
    }

    #[test]
    fn update_inserts_new_document_if_not_found() -> Result<()> {
        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));

        let mut lsm_tree = setup_lsm_tree(mock)?;
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };

        let res = lsm_tree.update(&document.id, document.clone());
        assert!(res.is_err());

        Ok(())
    }

    fn setup_lsm_tree(mock_db_operations_impl: MockDbOperationsImpl) -> Result<LsmTree> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();
        let db_operations = Box::new(mock_db_operations_impl);
        Ok(LsmTree::new(db_operations, path.to_str().unwrap().to_string(), 10, 100)?)
    }
}
