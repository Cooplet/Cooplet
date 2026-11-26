use std::{
    ffi::{CStr, CString},
    fs::{self, DirBuilder, File, Metadata, OpenOptions, Permissions},
    io::{self, Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

use crate::{
    AtomicPublish, PlatformError, PublishOutcome, PublishStage, current_euid, random_bytes,
};

pub(crate) const NOFOLLOW_FILE: libc::c_int = libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC;

pub struct PrivateFs;

impl PrivateFs {
    /// Validate or create a private directory under a trusted parent.
    pub fn ensure_private_dir(path: &Path) -> Result<PathBuf, PlatformError> {
        Self::ensure_private_dir_with(path, File::sync_all)
    }

    fn ensure_private_dir_with(
        path: &Path,
        sync_parent: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<PathBuf, PlatformError> {
        let path: PathBuf = path.components().collect();
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(PlatformError::insecure(&path, "symbolic link"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match DirBuilder::new().mode(0o700).create(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
        let dir = Self::open_dir_nofollow(&path)?;
        let metadata = dir.metadata()?;
        verify_private_dir(&path, &metadata)?;
        verify_path(&path, &metadata)?;
        let canonical = fs::canonicalize(&path)?;
        verify_path(&canonical, &metadata)?;
        // An existing entry might have just been created by another caller
        // or left behind after its parent sync failed. Complete that sync.
        if let Some(parent) = canonical.parent() {
            sync_parent(&Self::open_dir_nofollow(parent)?)?;
        }
        verify_path(&canonical, &metadata)?;
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
            |file| write_private_bytes(file, bytes),
            File::sync_all,
        )
    }

    /// Create a durable private file without replacing an existing owner.
    pub fn create_private_file_atomic(
        dir: &Path,
        name: &str,
        bytes: &[u8],
    ) -> Result<PublishOutcome, PlatformError> {
        let name = component(name)?;
        let (dir, fd, identity) = private_directory(dir)?;
        let (file, mut staging) = open_staging(&fd)?;
        write_private_bytes(&file, bytes)?;
        verify_path(&dir, &identity)?;
        let result = AtomicPublish::publish_noreplace_at(&fd, &staging.name, &fd, &name)?;
        if result == PublishOutcome::Published {
            staging.armed = false;
            verify_published_path(&dir, &identity)?;
        } else {
            verify_path(&dir, &identity)?;
        }
        Ok(result)
    }

    fn write_private_file_with(
        dir: &Path,
        name: &str,
        write_staging: impl FnOnce(&File) -> io::Result<()>,
        sync_dir: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<(), PlatformError> {
        let name = component(name)?;
        let (dir, dir_fd, identity) = private_directory(dir)?;
        let (file, mut staging) = open_staging(&dir_fd)?;
        write_staging(&file)?;
        verify_path(&dir, &identity)?;
        replace_entry(&dir_fd, &staging.name, &name)?;
        staging.armed = false;
        sync_dir(&dir_fd).map_err(|source| PlatformError::PublishedDurabilityUnknown {
            stage: PublishStage::RenameDirectorySync,
            source,
        })?;
        verify_published_path(&dir, &identity)
    }

    pub fn seal_readonly(file: &File) -> Result<(), PlatformError> {
        file.set_permissions(Permissions::from_mode(0o400))?;
        file.sync_all()?;
        Ok(())
    }

    pub fn open_sealed_file(path: &Path) -> Result<File, PlatformError> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FILE)
            .open(path)?;
        let metadata = file.metadata()?;
        verify_private_file(path, &metadata, 0o400)?;
        verify_path(path, &metadata)?;
        Ok(file)
    }
}

fn private_directory(path: &Path) -> Result<(PathBuf, File, Metadata), PlatformError> {
    let path = PrivateFs::ensure_private_dir(path)?;
    let file = PrivateFs::open_dir_nofollow(&path)?;
    let identity = file.metadata()?;
    verify_private_dir(&path, &identity)?;
    verify_path(&path, &identity)?;
    Ok((path, file, identity))
}

fn verify_private_dir(path: &Path, metadata: &Metadata) -> Result<(), PlatformError> {
    if !metadata.is_dir() || metadata.uid() != current_euid() || metadata.mode() & 0o7777 != 0o700 {
        return Err(PlatformError::insecure(
            path,
            "expected a directory owned by the current user with mode 0700",
        ));
    }
    Ok(())
}

fn write_private_bytes(mut file: &File, bytes: &[u8]) -> io::Result<()> {
    file.write_all(bytes)?;
    file.sync_all()
}

fn verify_published_path(dir: &Path, identity: &Metadata) -> Result<(), PlatformError> {
    verify_path(dir, identity).map_err(|error| PlatformError::PublishedDurabilityUnknown {
        stage: PublishStage::DirectoryIdentity,
        source: io::Error::other(error),
    })
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

fn open_staging(dir: &File) -> Result<(File, StagingName<'_>), PlatformError> {
    for _ in 0..16 {
        let mut bytes = [0; 16];
        random_bytes(&mut bytes)?;
        let name = component(&format!(".tmp.{:032x}", u128::from_be_bytes(bytes)))?;
        match create_entry(dir, &name) {
            Ok(file) => {
                let staging = StagingName {
                    dir,
                    name,
                    armed: true,
                };
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

struct StagingName<'a> {
    dir: &'a File,
    name: CString,
    armed: bool,
}

impl Drop for StagingName<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = unlink_entry(self.dir, &self.name);
        }
    }
}

fn create_entry(dir: &File, name: &CStr) -> io::Result<File> {
    let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: the descriptor and C string remain valid, and openat owns a
    // newly allocated fd on success. The variadic mode is integer-promoted.
    unsafe {
        let fd = libc::openat(dir.as_raw_fd(), name.as_ptr(), flags, 0o600 as libc::c_uint);
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(File::from_raw_fd(fd))
        }
    }
}

fn replace_entry(dir: &File, source: &CStr, target: &CStr) -> io::Result<()> {
    // SAFETY: both validated names and the same borrowed fd live through renameat.
    if unsafe {
        libc::renameat(
            dir.as_raw_fd(),
            source.as_ptr(),
            dir.as_raw_fd(),
            target.as_ptr(),
        )
    } == 0
    {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub(crate) fn unlink_entry(dir: &File, name: &CStr) -> io::Result<()> {
    // SAFETY: the borrowed directory fd and validated C string live through unlinkat.
    if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
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

    #[test]
    fn private_write_and_cleanup_keep_the_original_directory_fd()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let live = scratch.0.join("state");
        let rotated = scratch.0.join("rotated");
        PrivateFs::write_private_file_atomic(&live, "cred", b"old")?;
        let result = PrivateFs::write_private_file_with(
            &live,
            "cred",
            |file| {
                write_private_bytes(file, b"new")?;
                fs::rename(&live, &rotated)?;
                DirBuilder::new().mode(0o700).create(&live)?;
                fs::write(live.join("cred"), b"replacement")
            },
            File::sync_all,
        );
        assert!(matches!(result, Err(PlatformError::PathReplaced)));
        assert_eq!(fs::read(rotated.join("cred"))?, b"old");
        assert_eq!(fs::read(live.join("cred"))?, b"replacement");
        assert_eq!(fs::read_dir(rotated)?.count(), 1);
        assert_eq!(fs::read_dir(live)?.count(), 1);
        Ok(())
    }

    #[test]
    fn private_create_is_no_replace_and_leaves_no_staging_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        assert_eq!(
            PrivateFs::create_private_file_atomic(&scratch.0, "owner", b"first")?,
            PublishOutcome::Published
        );
        assert_eq!(
            PrivateFs::create_private_file_atomic(&scratch.0, "owner", b"other")?,
            PublishOutcome::AlreadyExists
        );
        assert_eq!(
            PrivateFs::read_private_file(&scratch.0.join("owner"), 5)?,
            b"first"
        );
        assert_eq!(fs::read_dir(&scratch.0)?.count(), 1);
        Ok(())
    }

    #[test]
    fn existing_directory_still_requires_a_successful_parent_sync()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let child = scratch.0.join("child");
        let first = PrivateFs::ensure_private_dir_with(&child, |_| {
            Err(io::Error::other("parent sync failed"))
        });
        assert!(matches!(first, Err(PlatformError::Io(_))));
        assert!(child.is_dir());
        let parent = scratch.0.metadata()?;
        let second = PrivateFs::ensure_private_dir_with(&child, |file| {
            let metadata = file.metadata()?;
            assert_eq!(
                (metadata.dev(), metadata.ino()),
                (parent.dev(), parent.ino())
            );
            Err(io::Error::other("parent sync still failing"))
        });
        assert!(matches!(second, Err(PlatformError::Io(_))));
        PrivateFs::ensure_private_dir(&child)?;
        Ok(())
    }
}
