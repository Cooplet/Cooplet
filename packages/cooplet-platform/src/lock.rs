use std::{
    fs::{self, File, Metadata, OpenOptions, Permissions, TryLockError},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use crate::{
    PlatformError,
    fs::{NOFOLLOW_FILE, sidecar_path, verify_path, verify_private_file},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockProbe {
    Idle,
    OwnedByLiveProcess,
}

#[derive(Debug)]
pub struct FileLock {
    file: File,
    path: PathBuf,
}

impl FileLock {
    pub fn acquire_exclusive(path: &Path) -> Result<Self, PlatformError> {
        let path = sidecar_path(path)?;
        let file = open_or_create_sidecar(&path)?;
        Self::lock(path, file, File::try_lock)
    }

    /// Hold a shared guard for an existing sidecar without creating it.
    pub fn acquire_existing_shared(path: &Path) -> Result<Self, PlatformError> {
        let path = sidecar_path(path)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FILE)
            .open(&path)?;
        Self::lock(path, file, File::try_lock_shared)
    }

    fn lock(
        path: PathBuf,
        file: File,
        try_lock: fn(&File) -> Result<(), TryLockError>,
    ) -> Result<Self, PlatformError> {
        verified_lock_metadata(&file, &path)?;
        for attempt in 0..5 {
            match try_lock(&file) {
                Ok(()) => {
                    let lock = Self { file, path };
                    lock.verify_path()?;
                    return Ok(lock);
                }
                Err(TryLockError::WouldBlock) if attempt < 4 => {
                    thread::sleep(Duration::from_millis(20))
                }
                Err(TryLockError::WouldBlock) => break,
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
        Err(PlatformError::LockBusy)
    }

    pub fn probe_owner_state(path: &Path) -> Result<LockProbe, PlatformError> {
        let path = sidecar_path(path)?;
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FILE)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(LockProbe::Idle),
            Err(error) => return Err(error.into()),
        };
        verified_lock_metadata(&file, &path)?;
        match file.try_lock_shared() {
            Ok(()) => {
                let verified = verified_lock_metadata(&file, &path);
                let unlocked = file.unlock();
                verified?;
                unlocked?;
                Ok(LockProbe::Idle)
            }
            Err(TryLockError::WouldBlock) => {
                verified_lock_metadata(&file, &path)?;
                Ok(LockProbe::OwnedByLiveProcess)
            }
            Err(TryLockError::Error(error)) => Err(error.into()),
        }
    }

    pub fn verify_path(&self) -> Result<(), PlatformError> {
        verified_lock_metadata(&self.file, &self.path).map(|_| ())
    }

    /// Check whether this lock already owns the file at another path.
    pub fn protects_path(&self, path: &Path) -> Result<bool, PlatformError> {
        let opened = verified_lock_metadata(&self.file, &self.path)?;
        match fs::symlink_metadata(path) {
            Ok(current) => Ok((current.dev(), current.ino()) == (opened.dev(), opened.ino())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

fn open_or_create_sidecar(path: &Path) -> Result<File, PlatformError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(NOFOLLOW_FILE)
        .open(path)
    {
        Ok(file) => {
            // Ensure exact 0600 on newly created lockfiles even under a strict umask.
            file.set_permissions(Permissions::from_mode(0o600))?;
            Ok(file)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(open_sidecar(path)?),
        Err(error) => Err(error.into()),
    }
}

fn open_sidecar(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(NOFOLLOW_FILE)
        .open(path)
}

fn verified_lock_metadata(file: &File, path: &Path) -> Result<Metadata, PlatformError> {
    let metadata = file.metadata()?;
    verify_path(path, &metadata)?;
    verify_private_file(path, &metadata, 0o600)?;
    Ok(metadata)
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, Permissions};
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    use super::*;
    use crate::test_support::Scratch;

    #[test]
    fn exclusive_lock_and_probe_use_the_same_inode() -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let path = scratch.0.join("node.lock");
        assert_eq!(FileLock::probe_owner_state(&path)?, LockProbe::Idle);
        assert!(matches!(
            FileLock::acquire_existing_shared(&path),
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
        assert!(!path.exists());

        let lock = FileLock::acquire_exclusive(&path)?;
        assert_eq!(path.metadata()?.mode() & 0o777, 0o600);
        assert!(lock.protects_path(&path)?);
        let alias = scratch.0.join("node-alias.lock");
        symlink(&path, &alias)?;
        assert!(!lock.protects_path(&alias)?);
        assert!(!lock.protects_path(&scratch.0.join("absent.lock"))?);
        assert!(matches!(
            FileLock::acquire_existing_shared(&path),
            Err(PlatformError::LockBusy)
        ));
        assert_eq!(
            FileLock::probe_owner_state(&path)?,
            LockProbe::OwnedByLiveProcess
        );
        assert!(matches!(
            FileLock::acquire_exclusive(&path),
            Err(PlatformError::LockBusy)
        ));
        fs::rename(&path, scratch.0.join("old.lock"))?;
        let replacement = FileLock::acquire_exclusive(&path)?;
        assert!(matches!(
            lock.verify_path(),
            Err(PlatformError::PathReplaced)
        ));
        drop(lock);
        assert_eq!(
            FileLock::probe_owner_state(&path)?,
            LockProbe::OwnedByLiveProcess
        );
        drop(replacement);
        assert_eq!(FileLock::probe_owner_state(&path)?, LockProbe::Idle);
        let reader = FileLock::acquire_existing_shared(&path)?;
        let other_reader = FileLock::acquire_existing_shared(&path)?;
        assert_eq!(FileLock::probe_owner_state(&path)?, LockProbe::Idle);
        assert!(matches!(
            FileLock::acquire_exclusive(&path),
            Err(PlatformError::LockBusy)
        ));
        drop((reader, other_reader));
        let _writer = FileLock::acquire_exclusive(&path)?;
        Ok(())
    }

    #[test]
    fn rejects_untrusted_sidecars_without_touching_the_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let db = scratch.0.join("node.sqlite3");
        fs::write(&db, b"original")?;
        let sidecar = scratch.0.join("node.lock");
        symlink(&db, &sidecar)?;
        for path in [&db, &sidecar, &scratch.0.join(".lock")] {
            assert!(FileLock::acquire_exclusive(path).is_err());
            assert!(FileLock::probe_owner_state(path).is_err());
        }
        assert_eq!(fs::read(&db)?, b"original");

        fs::remove_file(&sidecar)?;
        let lock = FileLock::acquire_exclusive(&sidecar)?;
        drop(lock);
        fs::set_permissions(&sidecar, Permissions::from_mode(0o644))?;
        assert!(FileLock::acquire_exclusive(&sidecar).is_err());
        assert!(FileLock::probe_owner_state(&sidecar).is_err());
        fs::set_permissions(&sidecar, Permissions::from_mode(0o600))?;
        fs::hard_link(&sidecar, scratch.0.join("other.lock"))?;
        assert!(FileLock::acquire_exclusive(&sidecar).is_err());
        assert!(FileLock::probe_owner_state(&sidecar).is_err());
        Ok(())
    }
}
