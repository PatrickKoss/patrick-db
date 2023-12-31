use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

use anyhow::Result;

pub trait FileHandler: Send + Sync {
    fn append(&mut self, data: &[u8]) -> Result<u64>;
    fn read(&mut self, offset: u64, size: u64) -> Result<Vec<u8>>;
    fn read_all(&mut self) -> Result<Vec<u8>>;
    fn update(&mut self, offset: u64, data: &[u8]) -> Result<()>;
}

pub struct FileHandlerImpl {
    writer: BufWriter<File>,
    filename: String,
}

impl FileHandlerImpl {
    pub fn new(filename: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(true)
            .create(true)
            .open(filename)?;

        Ok(Self {
            writer: BufWriter::new(file),
            filename: filename.to_string(),
        })
    }
}

impl FileHandler for FileHandlerImpl {
    fn append(&mut self, data: &[u8]) -> Result<u64> {
        let offset = self.writer.stream_position()?;
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(offset)
    }

    fn read(&mut self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(true)
            .create(true)
            .open(&self.filename)?;

        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut f = OpenOptions::new()
            .write(true)
            .read(true)
            .append(true)
            .create(true)
            .open(&self.filename)
            .unwrap();
        let metadata = fs::metadata(&self.filename)?;
        let mut buffer = vec![0; metadata.len() as usize];
        f.read_exact(&mut buffer).expect("buffer overflow");

        Ok(buffer)
    }

    fn update(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(&self.filename)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_writes_data_to_file_and_returns_offset() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("append_writes_data_to_file_and_returns_offset.txt");
        let mut file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;

        let data = b"Hello, world!";
        let offset = file_handler.append(data)?;

        assert_eq!(offset, 0);

        let offset = file_handler.append(data)?;
        assert_eq!(offset, data.len() as u64);

        Ok(())
    }

    #[test]
    fn read_reads_data_from_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("read_reads_data_from_file.txt");
        let mut file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;

        let data = b"Hello, world!";
        file_handler.append(data)?;

        let read_data = file_handler.read(0, data.len() as u64)?;

        assert_eq!(read_data, data);

        Ok(())
    }

    #[test]
    fn read_all_reads_all_data_from_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("read_all_reads_all_data_from_file.txt");
        let mut file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;

        let data = b"Hello, world!";
        file_handler.append(data)?;

        let read_data = file_handler.read_all()?;

        assert_eq!(read_data, data);

        Ok(())
    }

    #[test]
    fn update_overwrites_data_in_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file_path = dir.path().join("update_overwrites_data_in_file.txt");
        let mut file_handler = FileHandlerImpl::new(file_path.to_str().unwrap())?;

        let data = b"Hello, world!";
        file_handler.append(data)?;

        let new_data = b"Hello, Rust!";
        file_handler.update(0, new_data)?;

        let read_data = file_handler.read(0, new_data.len() as u64)?;

        assert_eq!(read_data, new_data);

        Ok(())
    }
}
