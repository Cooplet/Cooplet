use std::{
    ffi::CString,
    fs::{self, DirBuilder, File, Metadata, OpenOptions, Permissions},
    io::{self, Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

use crate::{PlatformError, PublishStage, current_euid};

pub(crate) const NOFOLLOW_FILE: libc::c_int = libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC;

pub struct PrivateFs;

impl PrivateFs {
    /// Validate or create a private directory under a trusted parent.
    pub fn ensure_private_dir(path: &Path) -> Result<PathBuf, PlatformError> {
        let path: PathBuf = path.components().collect();
        let created = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(PlatformError::insecure(&path, "symbolic link"));
            }
            Ok(_) => false,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match DirBuilder::new().mode(0o700).create(&path) {
                    Ok(()) => true,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        let dir = Self::open_dir_nofollow(&path)?;
        let metadata = dir.metadata()?;
        if !metadata.is_dir()
            || metadata.uid() != current_euid()
            || metadata.mode() & 0o7777 != 0o700
        {
            return Err(PlatformError::insecure(
                &path,
                "expected a directory owned by the current user with mode 0700",
            ));
        }
        verify_path(&path, &metadata)?;
        let canonical = fs::canonicalize(&path)?;
        verify_path(&canonical, &metadata)?;
        if created {
            let parent = canonical
                .parent()
                .ok_or_else(|| PlatformError::InvalidPath(path.display().to_string()))?;
            Self::open_dir_nofollow(parent)?.sync_all()?;
        }
        Ok(canonical)
    }

    pub fn open_dir_nofollow(path: &Path) -> Result<File, PlatformError> {
        let path: PathBuf = path.components().collect();
        Ok(OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?)
    }

    pub fn read_private_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, PlatformError> {
        let path: PathBuf = path.components().collect();
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FILE)
            .open(&path)?;
        let metadata = file.metadata()?;
        verify_private_file(&path, &metadata, 0o600)?;
        verify_path(&path, &metadata)?;
        if metadata.len() > max_bytes {
            return Err(PlatformError::FileTooLarge {
                limit_bytes: max_bytes,
            });
        }
        let mut bytes = Vec::new();
        file.take(max_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > max_bytes {
            return Err(PlatformError::FileTooLarge {
                limit_bytes: max_bytes,
            });
        }
        Ok(bytes)
    }

    /// Replace a private file atomically. An error after the rename carries
    /// the durability uncertainty and leaves the visible destination in place.
    pub fn write_private_file_atomic(
        dir: &Path,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), PlatformError> {
        Self::write_private_file_with(
            dir,
            name,
            |mut file| {
                file.write_all(bytes)?;
                file.sync_all()
            },
            File::sync_all,
        )
    }

    fn write_private_file_with(
        dir: &Path,
        name: &str,
        write_staging: impl FnOnce(&File) -> io::Result<()>,
        sync_dir: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<(), PlatformError> {
        component(name)?;
        let dir = Self::ensure_private_dir(dir)?;
        let dir_fd = Self::open_dir_nofollow(&dir)?;
        let (file, mut staging) = open_staging(&dir)?;
        write_staging(&file)?;
        fs::rename(&staging.path, dir.join(name))?;
        staging.armed = false;
        sync_dir(&dir_fd).map_err(|source| PlatformError::PublishedDurabilityUnknown {
            stage: PublishStage::RenameDirectorySync,
            source,
        })
    }

    pub fn seal_readonly(file: &File) -> Result<(), PlatformError> {
        file.set_permissions(Permissions::from_mode(0o400))?;
        file.sync_all()?;
        Ok(())
    }

    pub fn open_sealed_file(path: &Path) -> Result<File, PlatformError> {
        let file = OpenOptions::new().read(true).custom_flags(NOFOLLOW_FILE).open(path)?;
        let metadata = file.metadata()?;
        verify_private_file(path, &metadata, 0o400)?;
        verify_path(path, &metadata)?;
        Ok(file)
    }
}

pub(crate) fn component(name: &str) -> Result<CString, PlatformError> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains(['/', '\\']) {
        return Err(PlatformError::InvalidPath(name.into()));
    }
    CString::new(name).map_err(|_| PlatformError::InvalidPath(name.into()))
}

pub(crate) fn verify_private_file(
    path: &Path,
    metadata: &Metadata,
    mode: u32,
) -> Result<(), PlatformError> {
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != current_euid()
        || metadata.mode() & 0o7777 != mode
    {
        return Err(PlatformError::insecure(
            path,
            "expected a regular single-link file owned by the current user with private mode",
        ));
    }
    Ok(())
}

pub(crate) fn verify_path(path: &Path, opened: &Metadata) -> Result<(), PlatformError> {
    match fs::symlink_metadata(path) {
        Ok(current) if (current.dev(), current.ino()) == (opened.dev(), opened.ino()) => Ok(()),
        Ok(_) => Err(PlatformError::PathReplaced),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(PlatformError::PathReplaced),
        Err(error) => Err(error.into()),
    }
}

fn open_staging(dir: &Path) -> Result<(File, StagingName), PlatformError> {
    for _ in 0..16 {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
        let path = dir.join(format!(".tmp.{:032x}", u128::from_be_bytes(bytes)));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
        {
            Ok(file) => {
                let staging = StagingName { path, armed: true };
                file.set_permissions(Permissions::from_mode(0o600))?;
                return Ok((file, staging));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "private staging name collisions",
    )
    .into())
}

struct StagingName {
    path: PathBuf,
    armed: bool,
}

impl Drop for StagingName {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn sidecar_path(path: &Path) -> Result<PathBuf, PlatformError> {
    let path: PathBuf = path.components().collect();
    let filename = path
        .file_name()
        .filter(|name| {
            let bytes = name.as_bytes();
            bytes.len() > b".lock".len() && bytes.ends_with(b".lock")
        })
        .ok_or_else(|| PlatformError::InvalidLockPath(path.display().to_string()))?;
    let parent = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(fs::canonicalize(parent)?.join(filename))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;
    use crate::test_support::Scratch;

    #[test]
    fn validates_modes_links_and_size() -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let dir = scratch.0.join("state");
        let canonical = PrivateFs::ensure_private_dir(&dir)?;
        assert_eq!(fs::metadata(&canonical)?.mode() & 0o777, 0o700);
        let alias = scratch.0.join("alias");
        symlink(&dir, &alias)?;
        assert!(PrivateFs::ensure_private_dir(&alias).is_err());
        assert!(PrivateFs::open_dir_nofollow(&alias.join(".")).is_err());
        fs::set_permissions(&dir, Permissions::from_mode(0o755))?;
        assert!(PrivateFs::ensure_private_dir(&dir).is_err());
        fs::set_permissions(&dir, Permissions::from_mode(0o700))?;

        PrivateFs::write_private_file_atomic(&dir, "cred", b"data")?;
        let path = dir.join("cred");
        assert_eq!(fs::metadata(&path)?.mode() & 0o777, 0o600);
        assert_eq!(PrivateFs::read_private_file(&path, 4)?, b"data");
        assert!(matches!(
            PrivateFs::read_private_file(&path, 3),
            Err(PlatformError::FileTooLarge { .. })
        ));
        symlink(&path, dir.join("symlink"))?;
        assert!(PrivateFs::read_private_file(&dir.join("symlink"), 4).is_err());
        fs::hard_link(&path, dir.join("hardlink"))?;
        assert!(PrivateFs::read_private_file(&path, 4).is_err());
        fs::remove_file(dir.join("hardlink"))?;
        PrivateFs::seal_readonly(&File::open(&path)?)?;
        assert_eq!(fs::metadata(&path)?.mode() & 0o777, 0o400);
        assert!(PrivateFs::read_private_file(&path, 4).is_err());
        Ok(())
    }

    #[test]
    fn atomic_replace_distinguishes_before_and_after_rename()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        PrivateFs::write_private_file_atomic(&scratch.0, "cred", b"old")?;
        let before = PrivateFs::write_private_file_with(
            &scratch.0,
            "cred",
            |mut file| {
                file.write_all(b"partial")?;
                Err(io::Error::other("staging write or sync failed"))
            },
            File::sync_all,
        );
        assert!(matches!(before, Err(PlatformError::Io(_))));
        assert_eq!(fs::read(scratch.0.join("cred"))?, b"old");
        assert_eq!(fs::read_dir(&scratch.0)?.count(), 1);

        let after = PrivateFs::write_private_file_with(
            &scratch.0,
            "cred",
            |mut file| {
                file.write_all(b"new")?;
                file.sync_all()
            },
            |_| Err(io::Error::other("directory sync failed")),
        );
        assert!(matches!(
            after,
            Err(PlatformError::PublishedDurabilityUnknown {
                stage: PublishStage::RenameDirectorySync,
                ..
            })
        ));
        assert_eq!(fs::read(scratch.0.join("cred"))?, b"new");
        assert_eq!(fs::read_dir(&scratch.0)?.count(), 1);
        Ok(())
    }
}
