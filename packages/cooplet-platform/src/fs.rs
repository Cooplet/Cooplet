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

use crate::{PlatformError, PublishStage, current_euid, random_bytes};

pub(crate) const NOFOLLOW_FILE: libc::c_int = libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC;

pub struct PrivateFs;

impl PrivateFs {
    /// Validate or create a private directory under a trusted parent.
    pub fn ensure_private_dir(path: &Path) -> Result<PathBuf, PlatformError> {
        Self::ensure_private_dir_with(path, File::sync_all)
    }

    /// Verify and canonicalize an existing private directory.
    pub fn validate_private_dir(path: &Path) -> Result<PathBuf, PlatformError> {
        let path: PathBuf = path.components().collect();
        canonical_private_directory(&path, false).map(|(canonical, _, _)| canonical)
    }

    fn ensure_private_dir_with(
        path: &Path,
        sync_parent: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<PathBuf, PlatformError> {
        Self::ensure_private_dir_fd_with(path, sync_parent).map(|(canonical, _, _)| canonical)
    }

    fn ensure_private_dir_fd_with(
        path: &Path,
        sync_parent: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<(PathBuf, File, Metadata), PlatformError> {
        let path: PathBuf = path.components().collect();
        let mut created = false;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(PlatformError::insecure(&path, "symbolic link"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match DirBuilder::new().mode(0o700).create(&path) {
                    Ok(()) => created = true,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
        let (canonical, dir_fd, metadata) = canonical_private_directory(&path, created)?;
        // An existing entry might have just been created by another caller
        // or left behind after its parent sync failed. Complete that sync
        // when the parent resides on the same filesystem.
        if let Some(parent) = canonical.parent()
            && parent != canonical
        {
            let parent_dir = Self::open_dir_nofollow(parent)?;
            if parent_dir.metadata()?.dev() == metadata.dev() {
                sync_parent(&parent_dir)?;
            }
        }
        verify_path(&canonical, &metadata)?;
        Ok((canonical, dir_fd, metadata))
    }

    pub fn open_dir_nofollow(path: &Path) -> Result<File, PlatformError> {
        let path: PathBuf = path.components().collect();
        Ok(OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?)
    }

    /// Open a direct private child directory relative to an already-verified parent fd.
    pub fn open_dir_at(parent: &File, name: &str) -> Result<File, PlatformError> {
        let c_name = component(name)?;
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        let dir = openat_entry(parent, &c_name, flags, 0)?;
        let metadata = dir.metadata()?;
        verify_private_dir(Path::new(name), &metadata)?;
        Ok(dir)
    }

    /// Create a new private file (`0600`) exclusively inside an open directory fd.
    pub fn create_private_file_at(dir: &File, name: &str) -> Result<File, PlatformError> {
        let c_name = component(name)?;
        let file = create_entry(dir, &c_name)?;
        if let Err(error) = file.set_permissions(Permissions::from_mode(0o600)) {
            let _ = unlink_entry(dir, &c_name);
            return Err(error.into());
        }
        Ok(file)
    }

    /// Open a sealed read-only (`0400`) regular file relative to an open directory fd.
    pub fn open_sealed_at(dir: &File, name: &str) -> Result<File, PlatformError> {
        let c_name = component(name)?;
        let file = openat_entry(dir, &c_name, libc::O_RDONLY | NOFOLLOW_FILE, 0)?;
        let metadata = file.metadata()?;
        verify_private_file(Path::new(name), &metadata, 0o400)?;
        Ok(file)
    }

    /// Unlink a non-directory entry relative to an open directory fd.
    /// Returns `Ok(false)` when the entry is already absent.
    pub fn unlink_file_at(dir: &File, name: &str) -> Result<bool, PlatformError> {
        let c_name = component(name)?;
        match unlink_entry_flags(dir, &c_name, 0) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Remove an empty child directory relative to an open directory fd.
    /// Returns `Ok(false)` when the directory is already absent.
    pub fn remove_dir_at(dir: &File, name: &str) -> Result<bool, PlatformError> {
        let c_name = component(name)?;
        match unlink_entry_flags(dir, &c_name, libc::AT_REMOVEDIR) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Remove stale staging files left by an interrupted `write_private_file_atomic`
    /// call for `target` inside an existing private directory.
    pub fn clean_interrupted_writes(dir: &Path, target: &str) -> Result<usize, PlatformError> {
        let prefix = staging_prefix(target)?;
        let path: PathBuf = dir.components().collect();
        let (canonical, dir_fd, identity) = canonical_private_directory(&path, false)?;
        let mut removed = 0usize;
        for entry in fs::read_dir(&canonical)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !is_staging_name(&name, &prefix) {
                continue;
            }
            if !entry.file_type()?.is_file() {
                continue;
            }
            if Self::unlink_file_at(&dir_fd, &name)? {
                removed += 1;
            }
        }
        if removed > 0 {
            dir_fd.sync_all()?;
        }
        verify_path(&canonical, &identity)?;
        Ok(removed)
    }

    /// Open a regular single-link file owned by the current user with mode `0600`.
    pub fn open_private_file(path: &Path) -> Result<File, PlatformError> {
        let path: PathBuf = path.components().collect();
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FILE)
            .open(&path)?;
        let metadata = file.metadata()?;
        verify_private_file(&path, &metadata, 0o600)?;
        verify_path(&path, &metadata)?;
        Ok(file)
    }

    pub fn read_private_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, PlatformError> {
        let file = Self::open_private_file(path)?;
        let metadata = file.metadata()?;
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

    fn write_private_file_with(
        dir: &Path,
        name: &str,
        write_staging: impl FnOnce(&File) -> io::Result<()>,
        sync_dir: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<(), PlatformError> {
        let target = component(name)?;
        let (dir, dir_fd, identity) = private_directory(dir)?;
        let (file, mut staging) = open_staging(&dir_fd, name)?;
        write_staging(&file)?;
        verify_path(&dir, &identity)?;
        replace_entry(&dir_fd, &staging.name, &target)?;
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
        let path: PathBuf = path.components().collect();
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FILE)
            .open(&path)?;
        let metadata = file.metadata()?;
        verify_private_file(&path, &metadata, 0o400)?;
        verify_path(&path, &metadata)?;
        Ok(file)
    }

    /// Open a regular file confined beneath `canonical_root` by stepping
    /// component-by-component with `openat(..., O_NOFOLLOW | O_CLOEXEC)` and
    /// opening the leaf with `O_NONBLOCK` so concurrent `RENAME_EXCHANGE`
    /// symlink swaps cannot escape `canonical_root` and FIFOs cannot block.
    ///
    /// When `excluded_dir` is `Some((dev, ino))`, rejects any directory or leaf
    /// whose `(dev, ino)` matches that excluded directory.
    pub fn open_confined_regular_file(
        canonical_root: &Path,
        relative: &Path,
        excluded_dir: Option<(u64, u64)>,
    ) -> Result<File, PlatformError> {
        let raw = relative.as_os_str().as_bytes();
        if raw.ends_with(b"/") || raw.ends_with(b"/.") {
            return Err(PlatformError::InvalidPath(relative.display().to_string()));
        }
        let mut names = Vec::new();
        for part in relative.components() {
            match part {
                std::path::Component::Normal(name) => names.push(os_component(name)?),
                _ => {
                    return Err(PlatformError::InvalidPath(relative.display().to_string()));
                }
            }
        }
        let Some((leaf, parents)) = names.split_last() else {
            return Err(PlatformError::InvalidPath(relative.display().to_string()));
        };

        let mut dir = Self::open_dir_nofollow(canonical_root)?;
        check_not_excluded(&dir.metadata()?, excluded_dir)?;
        let dir_flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        for parent in parents {
            dir = openat_entry(&dir, parent, dir_flags, 0)?;
            check_not_excluded(&dir.metadata()?, excluded_dir)?;
        }

        let file = openat_entry(&dir, leaf, libc::O_RDONLY | NOFOLLOW_FILE, 0)?;
        let metadata = file.metadata()?;
        check_not_excluded(&metadata, excluded_dir)?;
        if !metadata.is_file() {
            return Err(PlatformError::InvalidPath("expected a regular file".into()));
        }
        Ok(file)
    }
}

fn check_not_excluded(
    metadata: &Metadata,
    excluded_dir: Option<(u64, u64)>,
) -> Result<(), PlatformError> {
    if excluded_dir == Some((metadata.dev(), metadata.ino())) {
        return Err(PlatformError::PathExcluded);
    }
    Ok(())
}

fn os_component(name: &std::ffi::OsStr) -> Result<CString, PlatformError> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&b'/')
        || bytes.contains(&b'\\')
    {
        return Err(PlatformError::InvalidPath(
            Path::new(name).display().to_string(),
        ));
    }
    CString::new(bytes)
        .map_err(|_| PlatformError::InvalidPath(Path::new(name).display().to_string()))
}

fn canonical_private_directory(
    path: &Path,
    clear_created_mode: bool,
) -> Result<(PathBuf, File, Metadata), PlatformError> {
    let dir = PrivateFs::open_dir_nofollow(path)?;
    let mut metadata = dir.metadata()?;
    if clear_created_mode
        && metadata.is_dir()
        && metadata.uid() == current_euid()
        && metadata.mode() & 0o5077 == 0
        && metadata.mode() & 0o7777 != 0o700
    {
        dir.set_permissions(Permissions::from_mode(0o700))?;
        metadata = dir.metadata()?;
    }
    verify_private_dir(path, &metadata)?;
    verify_path(path, &metadata)?;
    let canonical = fs::canonicalize(path)?;
    verify_path(&canonical, &metadata)?;
    Ok((canonical, dir, metadata))
}

fn private_directory(path: &Path) -> Result<(PathBuf, File, Metadata), PlatformError> {
    let path: PathBuf = path.components().collect();
    match canonical_private_directory(&path, false) {
        Ok(result) => Ok(result),
        Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            PrivateFs::ensure_private_dir_fd_with(&path, File::sync_all)
        }
        Err(error) => Err(error),
    }
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

fn staging_prefix(target: &str) -> Result<String, PlatformError> {
    let _ = component(target)?;
    // Reserve space for `.tmp.` (5) + `.` (1) + 32 hex chars within POSIX NAME_MAX (255).
    if target.len() <= 200 {
        Ok(format!(".tmp.{target}."))
    } else {
        Ok(".tmp.".into())
    }
}

fn is_staging_name(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix).is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    })
}

fn open_staging<'a>(dir: &'a File, target: &str) -> Result<(File, StagingName<'a>), PlatformError> {
    let prefix = staging_prefix(target)?;
    for _ in 0..16 {
        let mut bytes = [0; 16];
        random_bytes(&mut bytes)?;
        let name = component(&format!("{prefix}{:032x}", u128::from_be_bytes(bytes)))?;
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

fn openat_entry(
    dir: &File,
    name: &CStr,
    flags: libc::c_int,
    mode: libc::c_uint,
) -> io::Result<File> {
    loop {
        // SAFETY: the descriptor and C string remain valid across openat,
        // the variadic mode is integer-promoted, and a non-negative return
        // transfers exclusive ownership of a new fd to File::from_raw_fd.
        let result = unsafe {
            let fd = libc::openat(dir.as_raw_fd(), name.as_ptr(), flags, mode);
            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(File::from_raw_fd(fd))
            }
        };
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            other => return other,
        }
    }
}

fn create_entry(dir: &File, name: &CStr) -> io::Result<File> {
    let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    openat_entry(dir, name, flags, 0o600 as libc::c_uint)
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

pub(crate) fn unlink_entry_flags(dir: &File, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: the borrowed directory fd and validated C string live through unlinkat.
    if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), flags) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub(crate) fn unlink_entry(dir: &File, name: &CStr) -> io::Result<()> {
    unlink_entry_flags(dir, name, 0)
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

    #[test]
    fn clears_inherited_setgid_on_newly_created_private_dir()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let parent = scratch.0.join("sgid-parent");
        DirBuilder::new().mode(0o755).create(&parent)?;
        fs::set_permissions(&parent, Permissions::from_mode(0o2755))?;
        let child = parent.join("state");
        let canonical = PrivateFs::ensure_private_dir(&child)?;
        assert_eq!(fs::metadata(&canonical)?.mode() & 0o7777, 0o700);
        // Existing directories with setgid remain rejected.
        let preexisting = parent.join("preexisting");
        DirBuilder::new().mode(0o700).create(&preexisting)?;
        fs::set_permissions(&preexisting, Permissions::from_mode(0o2700))?;
        assert!(matches!(
            PrivateFs::ensure_private_dir(&preexisting),
            Err(PlatformError::InsecurePermissions { .. })
        ));
        Ok(())
    }

    #[test]
    fn fd_relative_operations_and_scoped_interrupted_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let root = PrivateFs::ensure_private_dir(&scratch.0.join("root"))?;
        let sub = PrivateFs::ensure_private_dir(&root.join("sub"))?;
        let root_fd = PrivateFs::open_dir_nofollow(&root)?;
        let sub_fd = PrivateFs::open_dir_at(&root_fd, "sub")?;

        let mut file = PrivateFs::create_private_file_at(&sub_fd, "blob.bin")?;
        file.write_all(b"sealed-data")?;
        PrivateFs::seal_readonly(&file)?;
        drop(file);

        let mut opened = PrivateFs::open_sealed_at(&sub_fd, "blob.bin")?;
        let mut buf = Vec::new();
        opened.read_to_end(&mut buf)?;
        assert_eq!(buf, b"sealed-data");

        let mut by_path = PrivateFs::open_sealed_file(&sub.join("./blob.bin"))?;
        buf.clear();
        by_path.read_to_end(&mut buf)?;
        assert_eq!(buf, b"sealed-data");

        assert!(PrivateFs::unlink_file_at(&sub_fd, "blob.bin")?);
        assert!(!PrivateFs::unlink_file_at(&sub_fd, "blob.bin")?);
        assert!(PrivateFs::remove_dir_at(&root_fd, "sub")?);
        assert!(!PrivateFs::remove_dir_at(&root_fd, "sub")?);

        // Scoped interrupted-write cleanup only touches `.tmp.<target>.<32hex>` files.
        let target_tmp = root.join(format!(".tmp.database.owner.{:032x}", 1u128));
        let other_tmp = root.join(format!(".tmp.other.file.{:032x}", 2u128));
        let keep_file = root.join("database.owner");
        fs::write(&target_tmp, b"partial")?;
        fs::write(&other_tmp, b"other")?;
        fs::write(&keep_file, b"owner")?;
        assert_eq!(
            PrivateFs::clean_interrupted_writes(&root, "database.owner")?,
            1
        );
        assert!(!target_tmp.exists());
        assert!(other_tmp.exists());
        assert!(keep_file.exists());

        // Negative checks on fd-relative operations: symlinks, insecure modes, hardlinks, traversal.
        let wide_dir = root.join("wide");
        DirBuilder::new().mode(0o755).create(&wide_dir)?;
        fs::set_permissions(&wide_dir, Permissions::from_mode(0o755))?;
        assert!(matches!(
            PrivateFs::open_dir_at(&root_fd, "wide"),
            Err(PlatformError::InsecurePermissions { .. })
        ));
        let dir_link = root.join("dir_link");
        symlink(&wide_dir, &dir_link)?;
        assert!(PrivateFs::open_dir_at(&root_fd, "dir_link").is_err());
        assert!(matches!(
            PrivateFs::open_dir_at(&root_fd, ".."),
            Err(PlatformError::InvalidPath(_))
        ));

        let sealed_file = PrivateFs::create_private_file_at(&root_fd, "sealed")?;
        PrivateFs::seal_readonly(&sealed_file)?;
        drop(sealed_file);
        fs::hard_link(root.join("sealed"), root.join("sealed_link"))?;
        assert!(matches!(
            PrivateFs::open_sealed_at(&root_fd, "sealed"),
            Err(PlatformError::InsecurePermissions { .. })
        ));
        assert!(PrivateFs::unlink_file_at(&root_fd, "sealed_link")?);
        assert!(PrivateFs::open_sealed_at(&root_fd, "sealed").is_ok());
        assert!(matches!(
            PrivateFs::unlink_file_at(&root_fd, "../escape"),
            Err(PlatformError::InvalidPath(_))
        ));
        assert!(matches!(
            PrivateFs::remove_dir_at(&root_fd, ".."),
            Err(PlatformError::InvalidPath(_))
        ));
        Ok(())
    }

    #[test]
    fn open_confined_regular_file_blocks_symlinks_exclusions_and_fifos()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let root = fs::canonicalize(&scratch.0)?;
        let nested = root.join("a/b");
        fs::create_dir_all(&nested)?;
        let file_path = nested.join("data.txt");
        fs::write(&file_path, b"confined-ok")?;

        let mut file =
            PrivateFs::open_confined_regular_file(&root, Path::new("a/b/data.txt"), None)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "confined-ok");

        // Rejects directory leaf and non-normal components.
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new("a/b"), None),
            Err(PlatformError::InvalidPath(_))
        ));
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new("../data.txt"), None),
            Err(PlatformError::InvalidPath(_))
        ));
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new(""), None),
            Err(PlatformError::InvalidPath(_))
        ));
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new("a/b/data.txt/"), None),
            Err(PlatformError::InvalidPath(_))
        ));
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new("a/b/data.txt/."), None),
            Err(PlatformError::InvalidPath(_))
        ));

        // Rejects excluded intermediate directory and excluded root.
        let excluded_meta = fs::symlink_metadata(root.join("a"))?;
        let excluded = Some((excluded_meta.dev(), excluded_meta.ino()));
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new("a/b/data.txt"), excluded),
            Err(PlatformError::PathExcluded)
        ));
        let root_meta = fs::symlink_metadata(&root)?;
        assert!(matches!(
            PrivateFs::open_confined_regular_file(
                &root,
                Path::new("a/b/data.txt"),
                Some((root_meta.dev(), root_meta.ino()))
            ),
            Err(PlatformError::PathExcluded)
        ));

        // Rejects symlink intermediate directory and symlink leaf without following.
        let outside = Scratch::new()?;
        fs::write(outside.0.join("secret.txt"), b"secret")?;
        symlink(&outside.0, root.join("escape_dir"))?;
        symlink(outside.0.join("secret.txt"), root.join("escape_file"))?;
        assert!(
            PrivateFs::open_confined_regular_file(&root, Path::new("escape_dir/secret.txt"), None)
                .is_err()
        );
        assert!(
            PrivateFs::open_confined_regular_file(&root, Path::new("escape_file"), None).is_err()
        );

        // Rejects FIFO without blocking.
        let fifo = root.join("pipe");
        let status = std::process::Command::new("mkfifo").arg(&fifo).status()?;
        assert!(status.success());
        assert!(matches!(
            PrivateFs::open_confined_regular_file(&root, Path::new("pipe"), None),
            Err(PlatformError::InvalidPath(_))
        ));
        Ok(())
    }
}
