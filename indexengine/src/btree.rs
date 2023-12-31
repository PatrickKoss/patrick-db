use std::collections::BTreeMap;
use std::hash::Hash;
use std::marker::PhantomData;

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

use storageengine::operations::{DbOperations, NONE_SENTINEL, OffsetSize};

use crate::index::{Document, Index, IndexError};

pub struct BTree<K, V> {
    map: BTreeMap<K, OffsetSize>,
    db_operations: Box<dyn DbOperations>,
    transaction_id: u64,
    phatom: PhantomData<(K, V)>,
}

impl<K, V> BTree<K, V> where K: Serialize + DeserializeOwned + Hash + Eq + std::cmp::Ord + std::marker::Send + std::marker::Sync, V: Serialize + DeserializeOwned {
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

            let doc: Document<K, V> = bincode::deserialize(&row.data)?;

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
            phatom: PhantomData,
        })
    }
}

impl<K, V> Index<K, V> for BTree<K, V> where K: Serialize + DeserializeOwned + Hash + Eq + std::cmp::Ord + std::marker::Send + std::marker::Sync, V: Serialize + DeserializeOwned + std::marker::Send + std::marker::Sync {
    fn insert(&mut self, document: Document<K, V>) -> Result<()> {
        if self.search(&document.id).is_ok() {
            return Err(IndexError::AlreadyExists.into());
        }

        let data = bincode::serialize(&document)?;
        let offset_size = self.db_operations.insert(data, self.transaction_id)?;
        self.map.insert(document.id, offset_size);
        self.transaction_id += 1;
        Ok(())
    }

    fn search(&mut self, id: &K) -> Result<Document<K, V>> {
        match self.map.get(id) {
            Some(offset_size) => {
                let row = self.db_operations.read_with_offset(offset_size)?;
                let doc: Document<K, V> = bincode::deserialize(&row.data)?;
                Ok(doc)
            }
            None => Err(IndexError::NotFound.into()),
        }
    }

    fn delete(&mut self, id: &K) -> Result<()> {
        match self.map.get(id) {
            Some(offset_size) => {
                self.db_operations.delete_with_offset(offset_size, self.transaction_id)?;
                self.map.remove(id);
                self.transaction_id += 1;
                Ok(())
            }
            None => Err(IndexError::NotFound.into()),
        }
    }

    fn update(&mut self, id: &K, document: Document<K, V>) -> Result<()> {
        match self.map.get(id) {
            Some(offset_size) => {
                let data = bincode::serialize(&document)?;
                let new_offset_size = self.db_operations.update_with_offset(offset_size, data, self.transaction_id)?;
                self.map.insert(document.id, new_offset_size);
                self.transaction_id += 1;
                Ok(())
            }
            None => Err(IndexError::NotFound.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    #[cfg(test)]
    use mockall::mock;
    use mockall::predicate;
    use storageengine::operations::{Header, Row};

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
            .with(predicate::eq(data.clone()), predicate::eq(0_u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));

        let mut btree = setup_btree(mock)?;

        let res = btree.insert(document.clone());

        assert!(res.is_ok());
        assert!(btree.map.get(&document.id).is_some());

        let offset_size = btree.map.get(&document.id).unwrap();
        assert_eq!(offset_size.offset, 0);
        assert_eq!(offset_size.size, 3);

        Ok(())
    }

    #[test]
    fn search_returns_document_if_in_memory() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_insert()
            .with(predicate::eq(data.clone()), predicate::eq(0_u64))
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

        let mut btree = setup_btree(mock)?;
        btree.insert(document.clone())?;

        let doc = btree.search(&document.id)?;

        assert_eq!(doc, document);

        Ok(())
    }

    #[test]
    fn search_returns_error_if_not_found() -> Result<()> {
        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));

        let mut btree: BTree<String, i32> = setup_btree(mock)?;

        let result = btree.search(&"1".to_string());

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
            .with(predicate::eq(data.clone()), predicate::eq(0_u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));
        mock.expect_delete_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 0, size: 3 }), predicate::eq(1_u64))
            .times(1)
            .returning(move |_, _| Ok(()));

        let mut btree = setup_btree(mock)?;
        btree.insert(document.clone())?;
        btree.delete(&document.id)?;

        assert!(btree.map.get(&document.id).is_none());

        Ok(())
    }

    #[test]
    fn delete_returns_error_if_not_found() -> Result<()> {
        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));

        let mut btree: BTree<String, i32> = setup_btree(mock)?;

        let result = btree.delete(&"1".to_string());

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn update_modifies_existing_document() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };
        let data = bincode::serialize(&document)?;

        let updated_document = Document { id: "1".to_string(), value: vec![4, 5, 6] };
        let updated_data = bincode::serialize(&updated_document)?;

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));
        mock.expect_insert()
            .with(predicate::eq(data.clone()), predicate::eq(0_u64))
            .times(1)
            .returning(move |_, _| Ok(OffsetSize { offset: 0, size: 3 }));
        mock.expect_update_with_offset()
            .with(predicate::eq(&OffsetSize { offset: 0, size: 3 }), predicate::eq(updated_data), predicate::eq(1_u64))
            .times(1)
            .returning(move |_, _, _| Ok(OffsetSize { offset: 0, size: 3 }));

        let mut btree = setup_btree(mock)?;
        btree.insert(document.clone())?;

        btree.update(&updated_document.id, updated_document.clone())?;

        assert!(btree.map.get(&updated_document.id).is_some());

        let offset_size = btree.map.get(&updated_document.id).unwrap();
        assert_eq!(offset_size.offset, 0);
        assert_eq!(offset_size.size, 3);

        Ok(())
    }

    #[test]
    fn update_inserts_new_document_if_not_found() -> Result<()> {
        let document = Document { id: "1".to_string(), value: vec![1, 2, 3] };

        let mut mock = MockDbOperationsImpl::new();
        mock.expect_read_all().times(1).returning(move || Ok(vec![]));

        let mut btree = setup_btree(mock)?;

        let res = btree.update(&document.id, document.clone());
        assert!(res.is_err());

        Ok(())
    }

    fn setup_btree<K, V>(mock_db_operations_impl: MockDbOperationsImpl) -> Result<BTree<K, V>>
        where K: Serialize + DeserializeOwned + Hash + Eq + std::cmp::Ord + std::marker::Send + std::marker::Sync, V: Serialize + DeserializeOwned + std::marker::Send + std::marker::Sync
    {
        let db_operations = Box::new(mock_db_operations_impl);
        BTree::new(db_operations)
    }
}
