use crate::block::make_records_from_buf;
use crate::block::Record;
use crate::types::BLOCK_MAX_SIZE;
use crate::types::BLOCK_MIN_FREE_SIZE;
use crate::types::WAL_LOG_MAX_SIZE;
use failure::Fallible;
use std::fs::File;
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
        Wal {
            path: path.as_ref().into(),
            file: File::open(path).expect("open file"),
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
