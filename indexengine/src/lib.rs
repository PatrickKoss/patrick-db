use std::hash::Hash;

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub mod index;
pub mod btree;
pub mod lsm_tree;
pub mod no_index;
pub mod hashmap;

#[derive(Debug, Clone)]
pub enum IndexEngine {
    BTree,
    LSM,
    NoIndex,
    HashMap,
}

pub fn new_index_engine<K, V>(index_engine: IndexEngine, db_operations: Box<dyn storageengine::operations::DbOperations>) -> Result<Box<dyn index::Index<K, V>>>
    where K: Serialize + DeserializeOwned + Hash + Eq + std::convert::AsRef<[u8]> + Clone + std::cmp::Ord + 'static, V: Serialize + DeserializeOwned + 'static
{
    match index_engine {
        IndexEngine::BTree => {
            let b_tree = btree::BTree::new(db_operations)?;
            Ok(Box::new(b_tree))
        }
        IndexEngine::LSM => {
            let lsm_tree = lsm_tree::LsmTree::new(db_operations, "./ss_tables".to_string(), 100, 10000)?;
            Ok(Box::new(lsm_tree))
        }
        IndexEngine::NoIndex => {
            let no_index = no_index::NoIndex::new(db_operations);
            Ok(Box::new(no_index))
        }
        IndexEngine::HashMap => {
            let hashmap = hashmap::HashMapIndex::new(db_operations)?;
            Ok(Box::new(hashmap))
        }
    }
}
