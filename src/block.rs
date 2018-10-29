use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use crate::types::{BLOCK_MAX_SIZE, BLOCK_MIN_FREE_SIZE};
use crc::{crc32, Hasher32};
use std;
use std::io;

const RECORD_EXTRA_SIZE: usize = 7; // 6 bytes

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Type {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

#[derive(Debug, Clone)]
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
    if free_block_size <= BLOCK_MIN_FREE_SIZE {
        return make_records_from_buf(BLOCK_MAX_SIZE, buf);
    }

    // have enough space for buf in record format
    if free_block_size >= buf.len() + RECORD_EXTRA_SIZE {
        return vec![Record::new(Type::Full, buf)];
    }

    let mut records = vec![];
    let mut index = free_block_size - RECORD_EXTRA_SIZE;
    records.push(Record::new(Type::First, &buf[..index]));
    while index + BLOCK_MAX_SIZE < buf.len() {
        records.push(Record::new(
            Type::Middle,
            &buf[index..index + BLOCK_MAX_SIZE],
        ));
        index += BLOCK_MAX_SIZE;
    }
    records.push(Record::new(Type::Last, &buf[index..]));
    records
}
