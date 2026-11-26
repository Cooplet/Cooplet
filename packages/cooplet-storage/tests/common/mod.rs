use std::{fs, path::PathBuf};

use cooplet_platform::PrivateFs;

pub type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = std::env::temp_dir().join(cooplet_protocol::BlobId::generate()?.as_str());
        Ok(Self(PrivateFs::ensure_private_dir(&path)?))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
