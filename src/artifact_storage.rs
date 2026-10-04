//! Physical locations used to retain Cargo artifacts.

use std::path::Path;

use crate::util::{CargoResult, Filesystem};

#[derive(Clone, Copy)]
pub(crate) enum DependencyCache {
    GitDatabase,
    GitCheckout,
    RegistryIndex,
    RegistrySource,
}

/// Selects the physical locations used for Cargo's durable artifacts.
///
/// Implementations may place artifacts in Cargo's conventional filesystem
/// layout or provide an alternate backing store. Cargo itself continues to
/// own artifact formats, resolution, validation, and compilation.
pub(crate) trait ArtifactStorage {
    /// Directory holding downloaded registry archives.
    fn registry_archive_dir(&self) -> Filesystem;

    /// Materializes the registry archive cache before Cargo reads it.
    fn prepare_registry_archives(&self) -> CargoResult<()>;

    /// Materializes one missing archive on demand.
    fn prepare_registry_archive(&self, _key: &str, _path: &Path) -> CargoResult<()> {
        Ok(())
    }

    /// Whether an archive can be restored, without materializing it during resolution.
    fn registry_archive_is_retained(&self, _key: &str) -> CargoResult<bool> {
        Ok(false)
    }

    /// Imports one verified registry archive after Cargo writes it.
    fn persist_registry_archive(&self, key: &str, path: &Path) -> CargoResult<()>;

    fn prepare_dependency_artifacts(
        &self,
        cache: DependencyCache,
        key: &str,
        directory: &Filesystem,
    ) -> CargoResult<()>;

    fn persist_dependency_artifacts(
        &self,
        cache: DependencyCache,
        key: &str,
        directory: &Filesystem,
    ) -> CargoResult<()>;

    /// Directory holding mutable build artifacts for a workspace, if this
    /// backend replaces Cargo's normal workspace-local target directory.
    fn workspace_artifact_dir(&self, workspace_manifest_path: &Path) -> Option<Filesystem>;

    /// Materializes a workspace's target and build directories before Cargo uses them.
    fn prepare_workspace_artifacts(
        &self,
        workspace_manifest_path: &Path,
        target_dir: &Filesystem,
        build_dir: &Filesystem,
    ) -> CargoResult<()>;

    /// Imports a workspace's mutable target and build directories.
    fn persist_workspace_artifacts(
        &self,
        workspace_manifest_path: &Path,
        target_dir: &Filesystem,
        build_dir: &Filesystem,
    ) -> CargoResult<()>;

    fn persist_git_database(
        &self,
        key: &str,
        directory: &Filesystem,
        _revision: &str,
    ) -> CargoResult<()> {
        self.persist_dependency_artifacts(DependencyCache::GitDatabase, key, directory)
    }

    /// Whether Cargo's global-cache tracker owns this backend's artifacts.
    fn participates_in_global_cache(&self) -> bool;

    /// Starts a scope in which registry and dependency persistence may be
    /// batched. Scopes nest; only the outermost one persists.
    fn defer_persistence(&self) {}

    /// Ends a deferral scope. The outermost scope persists every request made
    /// within it.
    fn finish_deferred_persistence(&self) -> CargoResult<()> {
        Ok(())
    }
}

/// Cargo's historical on-disk artifact layout.
pub(crate) struct FilesystemStorage {
    registry_archive_dir: Filesystem,
}

impl FilesystemStorage {
    pub(crate) fn new(registry_archive_dir: Filesystem) -> Self {
        Self {
            registry_archive_dir,
        }
    }
}

impl ArtifactStorage for FilesystemStorage {
    fn registry_archive_dir(&self) -> Filesystem {
        self.registry_archive_dir.clone()
    }

    fn prepare_registry_archives(&self) -> CargoResult<()> {
        Ok(())
    }

    fn persist_registry_archive(&self, _key: &str, _path: &Path) -> CargoResult<()> {
        Ok(())
    }

    fn prepare_dependency_artifacts(
        &self,
        _cache: DependencyCache,
        _key: &str,
        _directory: &Filesystem,
    ) -> CargoResult<()> {
        Ok(())
    }

    fn persist_dependency_artifacts(
        &self,
        _cache: DependencyCache,
        _key: &str,
        _directory: &Filesystem,
    ) -> CargoResult<()> {
        Ok(())
    }

    fn workspace_artifact_dir(&self, _workspace_manifest_path: &Path) -> Option<Filesystem> {
        None
    }

    fn prepare_workspace_artifacts(
        &self,
        _workspace_manifest_path: &Path,
        _target_dir: &Filesystem,
        _build_dir: &Filesystem,
    ) -> CargoResult<()> {
        Ok(())
    }

    fn persist_workspace_artifacts(
        &self,
        _workspace_manifest_path: &Path,
        _target_dir: &Filesystem,
        _build_dir: &Filesystem,
    ) -> CargoResult<()> {
        Ok(())
    }

    fn participates_in_global_cache(&self) -> bool {
        true
    }
}
