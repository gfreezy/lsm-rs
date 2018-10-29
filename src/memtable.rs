use crate::types::{Key, Value, TOMBSTONE};
use failure::Fallible;
use failure::{bail, ensure};
use std::collections::BTreeMap;
use std::sync::RwLock;

#[derive(PartialEq, Debug)]
pub enum SetRet {
    AvailableSpace,
    ThresholdReached,
}

#[derive(Debug)]
pub struct MemTable {
    map: RwLock<BTreeMap<Key, Value>>,
    // max memory size in bytes, including key and value
    max_size: usize,
    // current size in bytes, including key and value
    size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        MemTable {
            map: RwLock::new(BTreeMap::new()),
            max_size,
            size: 0,
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Fallible<SetRet> {
        // tombstone is not allowed to use
        ensure!(key != TOMBSTONE, "not allow to set tombstone");
        // first check whether threshold is reached
        ensure!(!self.is_threshold_reached(), "threshold reached");

        let key_size = key.len();
        let value_size = value.len();
        let _ = self
            .map
            .write()
            .expect("acquire write lock in insert")
            .insert(key, value);

        // add up size
        self.size += key_size + value_size;

        if self.is_threshold_reached() {
            Ok(SetRet::ThresholdReached)
        } else {
            Ok(SetRet::AvailableSpace)
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        self.map
            .read()
            .expect("acquire read lock in get")
            .get(key)
            .cloned()
    }

    pub fn remove(&mut self, key: Key) -> Fallible<SetRet> {
        self.set(key, TOMBSTONE.to_vec())
    }

    pub fn is_threshold_reached(&self) -> bool {
        self.size >= self.max_size
    }
}

#[derive(Debug)]
pub struct ImmutableMemtable {
    map: BTreeMap<Key, Value>,
    // max memory size in bytes
    max_size: usize,
    size: usize,
}

impl From<MemTable> for ImmutableMemtable {
    fn from(memtable: MemTable) -> Self {
        let map = memtable.map.into_inner().expect("into memtable");
        ImmutableMemtable {
            map,
            max_size: memtable.max_size,
            size: memtable.size,
        }
    }
}

impl ImmutableMemtable {
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Value)> {
        self.map.iter()
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use spectral::prelude::*;

    #[test]
    fn test_memtable_set() {
        let mut memtable = MemTable::new(10);
        // used 8 bytes, 2 bytes left
        assert_that(&memtable.set(b"key".to_vec(), b"value".to_vec()))
            .is_ok()
            .is_equal_to(&SetRet::AvailableSpace);
        assert_that(&memtable.is_threshold_reached()).is_false();
        // use 16 bytes, threshold reached
        assert_that(&memtable.set(b"key".to_vec(), b"value".to_vec()))
            .is_ok()
            .is_equal_to(&SetRet::ThresholdReached);
        assert_that(&memtable.is_threshold_reached()).is_true();
    }

    #[test]
    fn test_memtable_get() {
        let mut memtable = MemTable::new(10);
        assert_that(&memtable.get(b"key")).is_none();
        assert_that(&memtable.set(b"key".to_vec(), b"value".to_vec()))
            .is_ok()
            .is_equal_to(&SetRet::AvailableSpace);
        assert_eq!(memtable.get(b"key"), Some(b"value".to_vec()));
        assert_eq!(memtable.get(b"key2"), None);
    }

    #[test]
    fn test_memtable_remove() {
        let mut memtable = MemTable::new(10);
        // used 8 bytes, 2 bytes left
        assert_that(&memtable.set(b"key".to_vec(), b"value".to_vec()))
            .is_ok()
            .is_equal_to(&SetRet::AvailableSpace);
        assert_that(&memtable.get(b"key"))
            .is_some()
            .is_equal_to(&b"value".to_vec());
        assert_that(&memtable.remove(b"key".to_vec())).is_ok();
        assert_that(&memtable.get(b"key"))
            .is_some()
            .is_equal_to(&TOMBSTONE.to_vec());
    }

    #[test]
    fn test_into_immutable_memtable() {
        let mut memtable = MemTable::new(10000);
        let mut sst = vec![];
        for i in 0..100 {
            let key = vec![b'k', b'e', b'y', i];
            let value = vec![b'v', b'a', b'l', b'u', b'e', i];
            sst.push((key.clone(), value.clone()));
            assert_that(&memtable.set(key, value))
                .is_ok()
                .is_equal_to(&SetRet::AvailableSpace);
        }

        let immutable: ImmutableMemtable = memtable.into();
        assert_that(&immutable.get(b"key1"))
            .is_some()
            .is_equal_to(&b"value1".to_vec());

        let iteror_sst: Vec<(Key, Value)> = immutable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_that(&sst).is_equal_to(&iteror_sst);
    }
}
