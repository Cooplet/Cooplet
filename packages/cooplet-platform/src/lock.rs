use std::{
    fs::{self, File, Metadata, OpenOptions, TryLockError},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(NOFOLLOW_FILE)
            .open(&path)?;
        let lock = Self { file, path };
        lock.verify_path()?;
        for attempt in 0..5 {
            match lock.file.try_lock() {
                Ok(()) => {
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
        let lock = Self { file, path };
        lock.verify_path()?;
        let state = match lock.file.try_lock_shared() {
            Ok(()) => LockProbe::Idle,
            Err(TryLockError::WouldBlock) => LockProbe::OwnedByLiveProcess,
            Err(TryLockError::Error(error)) => return Err(error.into()),
        };
        lock.verify_path()?;
        if state == LockProbe::Idle {
            lock.file.unlock()?;
        }
        Ok(state)
    }

    pub fn verify_path(&self) -> Result<(), PlatformError> {
        self.verified_metadata().map(|_| ())
    }

    /// Check whether this lock already owns the file at another path.
    pub fn protects_path(&self, path: &Path) -> Result<bool, PlatformError> {
        let opened = self.verified_metadata()?;
        match fs::symlink_metadata(path) {
            Ok(current) => Ok((current.dev(), current.ino()) == (opened.dev(), opened.ino())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn verified_metadata(&self) -> Result<Metadata, PlatformError> {
        let metadata = self.file.metadata()?;
        verify_path(&self.path, &metadata)?;
        verify_private_file(&self.path, &metadata, 0o600)?;
        Ok(metadata)
    }
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
        assert!(!path.exists());

        let lock = FileLock::acquire_exclusive(&path)?;
        assert_eq!(path.metadata()?.mode() & 0o777, 0o600);
        assert!(lock.protects_path(&path)?);
        let alias = scratch.0.join("node-alias.lock");
        symlink(&path, &alias)?;
        assert!(!lock.protects_path(&alias)?);
        assert!(!lock.protects_path(&scratch.0.join("absent.lock"))?);
        assert!(matches!(
            FileLock::acquire_exclusive(&path),
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
