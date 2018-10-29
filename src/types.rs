pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
// 1810212258 encode to bytes in big endian
pub const TOMBSTONE: &[u8] = &[0x6b, 0xe5, 0xa5, 0xa2];

pub const WAL_LOG_MAX_SIZE: usize = 4 * 1024 * 1024;

pub const BLOCK_MAX_SIZE: usize = 32 * 1024; // 32KB
pub const BLOCK_MIN_FREE_SIZE: usize = 6; // 6 bytes
