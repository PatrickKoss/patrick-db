use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::file_handler::FileHandler;

pub const NONE_SENTINEL: u64 = u64::MAX;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Header {
    pub xmin: u64,
    // stores the ID of the transaction that created this version of the row
    pub xmax: u64,
    // stores the ID of the transaction that deleted it (if it has been deleted)
    pub tuple_length: u64,
    // This field indicates the total length of the tuple, including the header and the data
    pub table_oid: u64,
    // This is the object identifier of the table to which the tuple belongs.
    pub ctid: u64,
    // This field is a physical location identifier for the tuple within its table.
    pub cmin: u64,
    // This field stores the ID of the transaction that created this version of the row.
    pub cmax: u64, // This field stores the ID of the transaction that deleted it (if it has been deleted).
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Row {
    pub header: Header,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct OffsetSize {
    pub offset: u64,
    pub size: u64,
}

pub trait DbOperations {
    fn insert(&mut self, data: Vec<u8>, transaction_id: u64) -> Result<OffsetSize>;
    fn read_with_offset(&mut self, offset_size: &OffsetSize) -> Result<Row>;
    fn read_all(&mut self) -> Result<Vec<Row>>;
    fn update_with_offset(&mut self, old_offset_size: &OffsetSize, data: Vec<u8>, transaction_id: u64) -> Result<OffsetSize>;
    fn delete_with_offset(&mut self, offset_size: &OffsetSize, transaction_id: u64) -> Result<()>;
}

pub struct DbOperationsImpl {
    file_handler: Box<dyn FileHandler>,
}

impl DbOperationsImpl {
    pub fn new(file_handler: Box<dyn FileHandler>) -> Self {
        Self {
            file_handler,
        }
    }
}

impl DbOperations for DbOperationsImpl {
    fn insert(&mut self, data: Vec<u8>, transaction_id: u64) -> Result<OffsetSize> {
        let mut header = Header {
            xmin: transaction_id,
            xmax: NONE_SENTINEL,
            tuple_length: 0, // This will be updated later
            table_oid: 0,
            ctid: 0,
            cmin: transaction_id,
            cmax: NONE_SENTINEL,
        };

        // Calculate the sizes of the header and content
        let header_size = bincode::serialized_size(&header)?;
        let content_size = bincode::serialized_size(&data)?;

        // Update the tuple_length in the header
        header.tuple_length = header_size + content_size;

        let row = Row {
            header,
            data,
        };

        let buf = bincode::serialize(&row)?;
        let offset = self.file_handler.append(&buf)?;

        Ok(OffsetSize {
            offset,
            size: buf.len() as u64,
        })
    }

    fn read_with_offset(&mut self, offset_size: &OffsetSize) -> Result<Row> {
        let buf = self.file_handler.read(offset_size.offset, offset_size.size)?;
        let row = bincode::deserialize::<Row>(&buf[0..])?;

        Ok(row)
    }

    fn read_all(&mut self) -> Result<Vec<Row>> {
        let buf = self.file_handler.read_all()?;
        let mut pos = 0;
        let mut rows: Vec<Row> = Vec::new();
        while let Ok(row) = bincode::deserialize::<Row>(&buf[pos..]) {
            pos += bincode::serialized_size(&row)? as usize;
            rows.push(row);
        }

        Ok(rows)
    }

    fn update_with_offset(&mut self, old_offset_size: &OffsetSize, data: Vec<u8>, transaction_id: u64) -> Result<OffsetSize> {
        let mut row = self.read_with_offset(old_offset_size)?;
        row.header.xmax = transaction_id;
        self.file_handler.update(old_offset_size.offset, &bincode::serialize(&row)?)?;
        let new_offset_size = self.insert(data, transaction_id)?;

        Ok(new_offset_size)
    }

    fn delete_with_offset(&mut self, offset_size: &OffsetSize, transaction_id: u64) -> Result<()> {
        let mut row = self.read_with_offset(offset_size)?;
        row.header.xmax = transaction_id;
        self.file_handler.update(offset_size.offset, &bincode::serialize(&row)?)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::file_handler::FileHandlerImpl;

    use super::*;

    #[test]
    fn insert_adds_row_and_returns_offset_size() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("insert_adds_row_and_returns_offset_size.txt");
        let file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;
        let mut db_operations = DbOperationsImpl::new(Box::new(file_handler));

        let data = vec![1, 2, 3, 4];
        let transaction_id = 1;
        let result = db_operations.insert(data, transaction_id);

        assert!(result.is_ok());
        let offset_size = result.unwrap();
        assert_eq!(offset_size.offset, 0);

        let new_data = vec![5, 6, 7, 8, 9];
        let new_transaction_id = 2;
        let new_result = db_operations.insert(new_data, new_transaction_id);

        assert!(new_result.is_ok());
        let new_offset_size = new_result.unwrap();
        // new offset should be on old offset + old size
        assert_eq!(new_offset_size.offset, offset_size.size);

        Ok(())
    }

    #[test]
    fn read_with_offset_returns_row() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("read_with_offset_returns_row.txt");
        let file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;
        let mut db_operations = DbOperationsImpl::new(Box::new(file_handler));

        let data = vec![1, 2, 3, 4];
        let transaction_id = 1;
        let offset_size = db_operations.insert(data, transaction_id)?;

        let row = db_operations.read_with_offset(&offset_size)?;

        assert_eq!(row.data, vec![1, 2, 3, 4]);

        Ok(())
    }

    #[test]
    fn read_all_returns_all_rows() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("read_all_returns_all_rows.txt");
        let file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;
        let mut db_operations = DbOperationsImpl::new(Box::new(file_handler));

        let data1 = vec![1, 2, 3, 4];
        let transaction_id1 = 1;
        db_operations.insert(data1, transaction_id1)?;

        let data2 = vec![5, 6, 7, 8, 9, 10];
        let transaction_id2 = 2;
        db_operations.insert(data2, transaction_id2)?;

        let rows = db_operations.read_all()?;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].data, vec![1, 2, 3, 4]);
        assert_eq!(rows[1].data, vec![5, 6, 7, 8, 9, 10]);

        Ok(())
    }

    #[test]
    fn update_with_offset_updates_row() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("update_with_offset_updates_row.txt");
        let file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;
        let mut db_operations = DbOperationsImpl::new(Box::new(file_handler));

        let data = vec![1, 2, 3, 4];
        let transaction_id = 1;
        let offset_size = db_operations.insert(data, transaction_id)?;

        let new_data = vec![5, 6, 7, 8];
        let new_transaction_id = 2;
        let new_offset_size = db_operations.update_with_offset(&offset_size, new_data, new_transaction_id)?;

        let row = db_operations.read_with_offset(&new_offset_size)?;

        assert_eq!(row.data, vec![5, 6, 7, 8]);

        Ok(())
    }

    #[test]
    fn delete_with_offset_deletes_row() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("delete_with_offset_deletes_row.txt");
        let file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;
        let mut db_operations = DbOperationsImpl::new(Box::new(file_handler));

        let data = vec![1, 2, 3, 4];
        let transaction_id = 1;
        let offset_size = db_operations.insert(data, transaction_id)?;

        db_operations.delete_with_offset(&offset_size, transaction_id)?;

        let result = db_operations.read_with_offset(&offset_size);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().header.xmax, transaction_id);

        Ok(())
    }
}
