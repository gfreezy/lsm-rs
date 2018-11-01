use crate::block::make_records_from_buf;
use crate::block::Record;
use crate::types::BLOCK_MAX_SIZE;
use crate::types::BLOCK_MIN_FREE_SIZE;
use crate::types::WAL_LOG_MAX_SIZE;
use failure::Fallible;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::path::PathBuf;

// 4MB per file
pub struct Wal {
    path: PathBuf,
    file: File,
    used: usize,
    current_block_used: usize,
}

impl Wal {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)
            .expect("open file");
        Wal {
            path: path.as_ref().to_path_buf(),
            file,
            used: 0,
            current_block_used: 0,
        }
    }

    pub fn make_records<'a>(&self, buf: &'a [u8]) -> Vec<Record<'a>> {
        make_records_from_buf(self.current_block_free_space(), buf)
    }

    pub fn write_records<'a>(&mut self, records: Vec<Record<'a>>) -> Fallible<Vec<Record<'a>>> {
        if self.free_space() > 0 && self.free_space() <= BLOCK_MIN_FREE_SIZE {
            self.write_trailer()?;
        }

        if self.free_space() == 0 {
            return Ok(records);
        }

        let mut iter = records.into_iter();
        while let Some(record) = iter.next() {
            match self.write_record(&record) {
                Err(ref e) if e.kind() == ErrorKind::WriteZero => break,
                Err(e) => return Err(e.into()),
                Ok(..) => {}
            }
        }
        let left = iter.collect();
        Ok(left)
    }

    fn write_record(&mut self, record: &Record) -> io::Result<()> {
        // no enough space for current block
        if self.current_block_free_space() > 0
            && self.current_block_free_space() <= BLOCK_MIN_FREE_SIZE
        {
            self.write_trailer()?;
        }

        // start a new block
        if self.current_block_free_space() == 0 {
            self.new_block()?;
        }

        record.write_to(self)
    }

    fn new_block(&mut self) -> io::Result<()> {
        if self.free_space() == 0 {
            return Err(ErrorKind::WriteZero.into());
        }
        self.current_block_used = 0;
        Ok(())
    }

    fn write_trailer(&mut self) -> io::Result<()> {
        let mut trailer = vec![0; self.current_block_free_space()];
        self.write_all(&mut trailer)
    }

    fn free_space(&self) -> usize {
        WAL_LOG_MAX_SIZE - self.used
    }

    fn current_block_free_space(&self) -> usize {
        BLOCK_MAX_SIZE - self.current_block_used
    }
}

impl Write for Wal {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let s = self.file.write(buf)?;
        self.used += s;
        self.current_block_used += s;
        Ok(s)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.flush().expect("flush error");
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use spectral::prelude::*;

    #[test]
    fn test_write_wal() {
        let mut wal = Wal::new("test.wal");
        let buf = [1; 1000];
        let records = wal.make_records(&buf);
        let ret = wal.write_records(records);
        assert_that(&ret).is_ok().is_empty();
    }

    #[test]
    fn test_write_wal_overflow() {
        let mut wal = Wal::new("test.wal");
        let buf = [1; 5 * 1024 * 1024];
        let records = wal.make_records(&buf);
        assert_that(&records).has_length(5 * 1024 / 32);
        let ret = wal.write_records(records);
        assert_that(&ret).is_ok().has_length(1024 / 32);
    }
}
