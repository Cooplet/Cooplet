use std::{ffi::CStr, fs::File, io, os::fd::AsRawFd};

use crate::{
    PlatformError, PublishStage,
    fs::{component, unlink_entry},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    Published,
    AlreadyExists,
}

pub struct AtomicPublish;

impl AtomicPublish {
    /// Atomically install a staging name without overwriting an existing name.
    /// A successful install consumes the staging name. On a collision, the
    /// caller continues to own its staging file.
    pub fn publish_noreplace(
        stage_dir: &File,
        tmp_name: &str,
        dest_dir: &File,
        final_name: &str,
    ) -> Result<PublishOutcome, PlatformError> {
        let tmp_name = component(tmp_name)?;
        let final_name = component(final_name)?;
        match rename_noreplace(stage_dir, &tmp_name, dest_dir, &final_name) {
            Ok(()) => {
                sync_dir(dest_dir, PublishStage::DestinationSync)?;
                sync_dir(stage_dir, PublishStage::StagingSync)?;
                Ok(PublishOutcome::Published)
            }
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                Ok(PublishOutcome::AlreadyExists)
            }
            Err(error) if is_noreplace_unsupported(&error) => {
                publish_via_link(stage_dir, &tmp_name, dest_dir, &final_name)
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn is_noreplace_unsupported(error: &io::Error) -> bool {
    #[cfg(target_os = "macos")]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOTSUP)
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP)
        )
    }
}

fn rename_noreplace(
    stage_dir: &File,
    tmp: &CStr,
    dest_dir: &File,
    final_name: &CStr,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    // SAFETY: the borrowed directory fds and terminated names live through the call.
    let status = unsafe {
        libc::renameat2(
            stage_dir.as_raw_fd(),
            tmp.as_ptr(),
            dest_dir.as_raw_fd(),
            final_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    // SAFETY: the borrowed directory fds and terminated names live through the call.
    let status = unsafe {
        libc::renameatx_np(
            stage_dir.as_raw_fd(),
            tmp.as_ptr(),
            dest_dir.as_raw_fd(),
            final_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn publish_via_link(
    stage_dir: &File,
    tmp: &CStr,
    dest_dir: &File,
    final_name: &CStr,
) -> Result<PublishOutcome, PlatformError> {
    publish_via_link_with(stage_dir, tmp, dest_dir, final_name, |dir| {
        sync_dir(dir, PublishStage::DestinationSync)
    })
}

fn publish_via_link_with(
    stage_dir: &File,
    tmp: &CStr,
    dest_dir: &File,
    final_name: &CStr,
    sync_dest: impl FnOnce(&File) -> Result<(), PlatformError>,
) -> Result<PublishOutcome, PlatformError> {
    // SAFETY: the borrowed directory fds and terminated names live through the call.
    let linked = unsafe {
        libc::linkat(
            stage_dir.as_raw_fd(),
            tmp.as_ptr(),
            dest_dir.as_raw_fd(),
            final_name.as_ptr(),
            0,
        )
    };
    if linked != 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::EEXIST) {
            Ok(PublishOutcome::AlreadyExists)
        } else {
            Err(error.into())
        };
    }
    if let Err(error) = sync_dest(dest_dir) {
        // Avoid leaving an extra staging hardlink (nlink == 2) that would block
        // open_sealed_file if the destination entry persists after the sync failure.
        let _ = unlink_entry(stage_dir, tmp);
        return Err(error);
    }
    if let Err(source) = unlink_entry(stage_dir, tmp)
        && source.kind() != io::ErrorKind::NotFound
    {
        return Err(PlatformError::PublishedDurabilityUnknown {
            stage: PublishStage::StagingUnlink,
            source,
        });
    }
    sync_dir(stage_dir, PublishStage::StagingSync)?;
    Ok(PublishOutcome::Published)
}

fn sync_dir(dir: &File, stage: PublishStage) -> Result<(), PlatformError> {
    dir.sync_all()
        .map_err(|source| PlatformError::PublishedDurabilityUnknown { stage, source })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    use super::*;
    use crate::{PrivateFs, test_support::Scratch};

    #[test]
    fn rename_and_link_never_replace_a_destination() -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let stage = PrivateFs::ensure_private_dir(&scratch.0.join("stage"))?;
        let dest = PrivateFs::ensure_private_dir(&scratch.0.join("dest"))?;
        let stage_dir = PrivateFs::open_dir_nofollow(&stage)?;
        let dest_dir = PrivateFs::open_dir_nofollow(&dest)?;
        for (name, fallback) in [("rename", false), ("link", true)] {
            let publish = || {
                if fallback {
                    publish_via_link(&stage_dir, &component(name)?, &dest_dir, &component(name)?)
                } else {
                    AtomicPublish::publish_noreplace(&stage_dir, name, &dest_dir, name)
                }
            };
            fs::write(stage.join(name), b"first")?;
            assert_eq!(publish()?, PublishOutcome::Published);
            assert!(!stage.join(name).exists());
            assert_eq!(fs::metadata(dest.join(name))?.nlink(), 1);
            fs::write(stage.join(name), b"second")?;
            assert_eq!(publish()?, PublishOutcome::AlreadyExists);
            assert_eq!(fs::read(dest.join(name))?, b"first");
            assert_eq!(fs::read(stage.join(name))?, b"second");
        }
        Ok(())
    }

    #[test]
    fn noreplace_unsupported_classifies_fallback_errnos() {
        for errno in [libc::ENOSYS, libc::EINVAL, libc::EOPNOTSUPP, libc::ENOTSUP] {
            assert!(is_noreplace_unsupported(&io::Error::from_raw_os_error(
                errno
            )));
        }
        assert!(!is_noreplace_unsupported(&io::Error::from_raw_os_error(
            libc::EEXIST
        )));
        assert!(!is_noreplace_unsupported(&io::Error::from_raw_os_error(
            libc::EXDEV
        )));
    }

    #[test]
    fn link_fallback_unlinks_staging_on_destination_sync_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = Scratch::new()?;
        let stage = PrivateFs::ensure_private_dir(&scratch.0.join("stage"))?;
        let dest = PrivateFs::ensure_private_dir(&scratch.0.join("dest"))?;
        let stage_dir = PrivateFs::open_dir_nofollow(&stage)?;
        let dest_dir = PrivateFs::open_dir_nofollow(&dest)?;
        fs::write(stage.join("blob.part"), b"payload")?;
        let result = publish_via_link_with(
            &stage_dir,
            &component("blob.part")?,
            &dest_dir,
            &component("blob.bin")?,
            |_| {
                Err(PlatformError::PublishedDurabilityUnknown {
                    stage: PublishStage::DestinationSync,
                    source: io::Error::other("simulated sync error"),
                })
            },
        );
        assert!(matches!(
            result,
            Err(PlatformError::PublishedDurabilityUnknown {
                stage: PublishStage::DestinationSync,
                ..
            })
        ));
        assert!(!stage.join("blob.part").exists());
        assert_eq!(fs::metadata(dest.join("blob.bin"))?.nlink(), 1);
        Ok(())
    }
}
