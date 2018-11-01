use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use crate::types::BLOCK_MAX_SIZE;
use crc::{crc32, Hasher32};
use std;
use std::io;

const RECORD_EXTRA_SIZE: usize = 7; // 7 bytes

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Type {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record<'a> {
    checksum: u32,
    length: u16,
    typ: Type,
    data: &'a [u8],
}

impl<'a> Record<'a> {
    pub fn new(typ: Type, data: &'a [u8]) -> Self {
        assert!(data.len() < std::u16::MAX as usize, "data too big for u16");

        let checksum = Self::compute_checksum(typ, &data);
        let length = data.len() as u16;
        Record {
            checksum,
            length,
            typ,
            data,
        }
    }

    pub fn write_to<W: WriteBytesExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32::<LittleEndian>(self.checksum)?;
        writer.write_u16::<LittleEndian>(self.length)?;
        writer.write_u8(self.typ as u8)?;
        writer.write_all(self.data)
    }

    fn compute_checksum(typ: Type, data: &[u8]) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&[typ as u8]);
        digest.write(data);
        digest.sum32()
    }
}

pub fn make_records_from_buf(free_block_size: usize, buf: &[u8]) -> Vec<Record> {
    if free_block_size < RECORD_EXTRA_SIZE {
        return make_records_from_buf(BLOCK_MAX_SIZE, buf);
    }

    // have enough space for buf in record format
    if free_block_size >= buf.len() + RECORD_EXTRA_SIZE {
        return vec![Record::new(Type::Full, buf)];
    }

    let mut records = vec![];
    let mut index = free_block_size - RECORD_EXTRA_SIZE;
    records.push(Record::new(Type::First, &buf[..index]));
    while index + (BLOCK_MAX_SIZE - RECORD_EXTRA_SIZE) < buf.len() {
        records.push(Record::new(
            Type::Middle,
            &buf[index..index + BLOCK_MAX_SIZE - RECORD_EXTRA_SIZE],
        ));
        index += BLOCK_MAX_SIZE - RECORD_EXTRA_SIZE;
    }
    records.push(Record::new(Type::Last, &buf[index..]));
    records
}

mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use spectral::prelude::*;

    #[test]
    fn test_make_records_from_buf() {
        assert_that(&make_records_from_buf(10, &[1; 1000])).equals_iterator(
            &vec![
                Record::new(Type::First, &[1; 3]),
                Record::new(Type::Last, &[1; 1000 - 3]),
            ]
                .iter(),
        );
    }

    #[test]
    fn test_make_records_into_3blocks() {
        const BUF_SIZE: usize = 33 * 1024;
        const LAST_BLOCK_FREE_SIZE: usize = 100;
        assert_that(&make_records_from_buf(LAST_BLOCK_FREE_SIZE, &[1; BUF_SIZE])).equals_iterator(
            &vec![
                Record::new(Type::First, &[1; LAST_BLOCK_FREE_SIZE - RECORD_EXTRA_SIZE]),
                Record::new(Type::Middle, &[1; BLOCK_MAX_SIZE - RECORD_EXTRA_SIZE]),
                Record::new(
                    Type::Last,
                    &[1; BUF_SIZE - BLOCK_MAX_SIZE - LAST_BLOCK_FREE_SIZE + 2 * RECORD_EXTRA_SIZE],
                ),
            ]
                .iter(),
        );
    }

    #[test]
    fn test_make_records_into_2blocks() {
        const BUF_SIZE: usize = 4 * 1024;
        const LAST_BLOCK_FREE_SIZE: usize = 100;
        assert_that(&make_records_from_buf(LAST_BLOCK_FREE_SIZE, &[1; BUF_SIZE])).equals_iterator(
            &vec![
                Record::new(Type::First, &[1; LAST_BLOCK_FREE_SIZE - RECORD_EXTRA_SIZE]),
                Record::new(
                    Type::Last,
                    &[1; BUF_SIZE - LAST_BLOCK_FREE_SIZE + RECORD_EXTRA_SIZE],
                ),
            ]
                .iter(),
        )
    }
}
