use std::{
    fs::{self, DirBuilder},
    io,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
};

pub(crate) struct Scratch(pub PathBuf);

impl Scratch {
    pub(crate) fn new() -> io::Result<Self> {
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
        let path =
            std::env::temp_dir().join(format!("cooplet-test-{:032x}", u128::from_be_bytes(random)));
        DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
