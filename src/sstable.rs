use std::fs::File;
use std::path::PathBuf;

pub struct SSTable {
    path: PathBuf,
    file: File,
}
