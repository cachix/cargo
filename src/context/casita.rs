//! Casita implementation of Cargo's artifact storage interface.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
#[cfg(windows)]
use std::fs::{File, OpenOptions};
use std::io::{BufRead as _, BufReader, Write as _};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use crate::artifact_storage::{ArtifactStorage, DependencyCache};
use crate::util::{CargoResult, Filesystem};

const IPC_VERSION: u64 = 1;
const IPC_MAX_FRAME_BYTES: usize = 1_048_576;

/// Storage adapter that persists Cargo artifacts as Casita directory roots.
///
/// By default, Cargo compiles into a retained workspace checkout. It asks Casita to check
/// out the appropriate root if the working copy is absent, then imports it when
/// the build completes. Cargo links only a synchronous JSON-RPC client; the
/// Casita process owns all repository and async implementation dependencies.
#[derive(Clone)]
pub(super) struct CasitaArtifactStorage {
    repository: PathBuf,
    archives: Filesystem,
}

impl CasitaArtifactStorage {
    pub(super) fn default_repository(archives: Filesystem) -> Self {
        let repository = dirs::data_dir()
            .map(|dir| dir.join("casita"))
            .or_else(|| dirs::home_dir().map(|home| home.join(".casita")))
            .unwrap_or_else(|| PathBuf::from(".casita-data"));
        Self {
            repository,
            archives,
        }
    }

    fn workspace_hash(workspace_manifest_path: &Path) -> String {
        let real_path = std::fs::canonicalize(workspace_manifest_path)
            .unwrap_or_else(|_| workspace_manifest_path.to_owned());
        crate::util::hex::short_hash(&real_path)
    }

    fn registry_root() -> &'static str {
        "cargo/registry/archives"
    }

    fn dependency_root(cache: DependencyCache, key: &str) -> String {
        let prefix = match cache {
            DependencyCache::GitDatabase => "git/db",
            DependencyCache::GitCheckout => "git/checkouts",
            DependencyCache::RegistryIndex => "registry/index",
            DependencyCache::RegistrySource => "registry/src",
        };
        format!("cargo/{prefix}/{key}")
    }

    fn workspace_roots(
        &self,
        workspace_manifest_path: &Path,
        target_dir: &Filesystem,
        build_dir: &Filesystem,
    ) -> Vec<(String, Filesystem)> {
        let target = cargo_util::paths::normalize_path(target_dir.as_path_unlocked());
        let build = cargo_util::paths::normalize_path(build_dir.as_path_unlocked());
        let default = self
            .workspace_artifact_dir(workspace_manifest_path)
            .unwrap();
        if target == default.as_path_unlocked() && build == target {
            return vec![(
                format!(
                    "cargo/workspaces/{}/target",
                    Self::workspace_hash(workspace_manifest_path)
                ),
                Filesystem::new(target),
            )];
        }
        let identity = |path: &Path| match path.strip_prefix(default.as_path_unlocked()) {
            Ok(relative) => (true, relative.to_owned()),
            Err(_) => (false, path.to_owned()),
        };
        let layout = crate::util::hex::short_hash(&(identity(&target), identity(&build)));
        let root = format!(
            "cargo/workspaces/{}/layouts/{layout}",
            Self::workspace_hash(workspace_manifest_path)
        );
        if build.starts_with(&target) {
            vec![(format!("{root}/target"), Filesystem::new(target))]
        } else if target.starts_with(&build) {
            vec![(format!("{root}/build"), Filesystem::new(build))]
        } else {
            vec![
                (format!("{root}/target"), Filesystem::new(target)),
                (format!("{root}/build"), Filesystem::new(build)),
            ]
        }
    }

    fn checkout_dir(&self, name: &str) -> Filesystem {
        Filesystem::new(std::env::temp_dir().join("cargo-casita").join(format!(
            "{name}-{}",
            blake3::hash(self.repository.as_os_str().as_encoded_bytes()).to_hex()
        )))
    }

    #[cfg(unix)]
    fn endpoint(&self) -> PathBuf {
        let hash = blake3::hash(self.repository.to_string_lossy().as_bytes()).to_hex();
        PathBuf::from("/tmp")
            .join("casita")
            .join(format!("cargo-{}.sock", &hash.as_str()[..16]))
    }

    #[cfg(windows)]
    fn endpoint(&self) -> String {
        let hash = blake3::hash(self.repository.to_string_lossy().as_bytes()).to_hex();
        format!(r"\\.\pipe\casita-cargo-v{}", &hash.as_str()[..16])
    }

    fn snapshot_root(root: &str) -> String {
        format!(
            "cargo/snapshots/v2/{}",
            root.strip_prefix("cargo/").unwrap_or(root)
        )
    }

    fn checkout_root(&self, root: &str, checkout: &Filesystem) -> CargoResult<()> {
        let _lock = self.lock_repository()?;
        self.checkout_root_locked(root, checkout)
    }

    fn checkout_root_locked(&self, root: &str, checkout: &Filesystem) -> CargoResult<()> {
        let checkout = checkout.as_path_unlocked().to_owned();
        match std::fs::read_dir(&checkout) {
            Ok(mut entries) => {
                if entries.next().transpose()?.is_some() {
                    return Ok(());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(parent) = checkout.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let staging = tempfile::tempdir_in(checkout.parent().expect("checkout has a parent"))?;
        let staged = staging.path().join("checkout");
        let data = if self
            .client()?
            .checkout(&Self::snapshot_root(root), staged.clone())?
        {
            self.restore_snapshot(&staged)?
        } else {
            std::fs::remove_dir(&staged)?;
            let previous = format!(
                "cargo/snapshots/v1/{}",
                root.strip_prefix("cargo/").unwrap_or(root)
            );
            if self.client()?.checkout(&previous, staged.clone())? {
                self.restore_snapshot(&staged)?
            } else {
                std::fs::remove_dir(&staged)?;
                if !self.client()?.checkout(root, staged.clone())? {
                    return Ok(());
                }
                staged
            }
        };
        if std::fs::read_dir(&data)?.next().transpose()?.is_none() {
            return Ok(());
        }
        match std::fs::remove_dir(&checkout) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        std::fs::rename(data, checkout)?;
        Ok(())
    }

    fn restore_snapshot(&self, staged: &Path) -> CargoResult<PathBuf> {
        let data = staged.join("data");
        let manifest = staged.join("native.json");
        let mut native_directories = Vec::new();
        if manifest.exists() {
            let imports: Vec<NativeRestore> = serde_json::from_slice(&std::fs::read(manifest)?)?;
            for (index, import) in imports.into_iter().enumerate() {
                let restored = staged.join(format!("native-{index}"));
                match self
                    .client()?
                    .restore(&import.importer, &import.root, &restored)?
                {
                    Some(true) => {}
                    Some(false) => {
                        bail!("Casita snapshot references missing root `{}`", import.root)
                    }
                    None => bail!(
                        "Casita cannot restore snapshot importer `{}`",
                        import.importer
                    ),
                }
                for mapping in import.paths {
                    let source = safe_snapshot_path(&restored, &mapping.source)?;
                    let destination = safe_snapshot_path(&data, &mapping.destination)?;
                    if import.importer != "filesystem"
                        && std::fs::symlink_metadata(&source)?.is_dir()
                    {
                        native_directories.push(mapping.destination.clone());
                    }
                    if mapping.destination.as_os_str().is_empty() {
                        // Whole-tree imports replace the empty metadata envelope.
                        // remove_dir also rejects a malformed nonempty destination.
                        std::fs::remove_dir(&destination)?;
                    }
                    std::fs::rename(source, destination)?;
                }
            }
        }
        let timestamps: SnapshotTimestamps =
            serde_json::from_slice(&std::fs::read(staged.join("timestamps.json"))?)?;
        timestamps.restore(&data, &native_directories)?;
        Ok(data)
    }

    fn import_root(&self, root: &str, checkout: &Filesystem) -> CargoResult<()> {
        self.import_snapshot(root, checkout, None)
    }

    fn import_snapshot(
        &self,
        root: &str,
        checkout: &Filesystem,
        native: Option<NativeSource<'_>>,
    ) -> CargoResult<()> {
        let _lock = self.lock_repository()?;
        self.import_snapshot_locked(root, checkout, native)
    }

    fn import_snapshot_locked(
        &self,
        root: &str,
        checkout: &Filesystem,
        native: Option<NativeSource<'_>>,
    ) -> CargoResult<()> {
        let _timing = StorageTiming::new("snapshot.import", root);
        let checkout = checkout.as_path_unlocked();
        let stamp_path = self.stamp_path(root);
        let previous = std::fs::read(&stamp_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<SnapshotStamp>(&bytes).ok());
        let mut files = previous
            .as_ref()
            .map(|stamp| stamp.files.clone())
            .unwrap_or_default();
        let fingerprint = snapshot_fingerprint(checkout, &mut files)?;
        if let Some(mut stamp) = previous {
            // Pending Git generations may contain objects absent locally.
            let pending_git = matches!(native, Some(NativeSource::Git { .. }))
                && self.read_native_journal(&blake3::hash(root.as_bytes()).to_hex().to_string())?
                    != stamp
                        .imports
                        .iter()
                        .map(|import| import.root.clone())
                        .collect();
            if !pending_git
                && stamp.fingerprint == fingerprint
                && self.stamp_is_live(root, &stamp)?
            {
                self.prune_owned_roots(root, &stamp.imports)?;
                if native.is_some() {
                    self.prune_legacy_roots()?;
                }
                if stamp.files != files {
                    stamp.files = files;
                    Self::write_stamp(&stamp_path, &stamp)?;
                }
                tracing::debug!(root, "Casita snapshot unchanged");
                return Ok(());
            }
        }
        // Invalidate other Cargo processes' catalogs before changing any roots.
        // The repository lock keeps readers outside this publication interval.
        self.advance_snapshot_epoch()?;
        let staging = match checkout.parent() {
            Some(parent) => tempfile::tempdir_in(parent).or_else(|_| tempfile::tempdir())?,
            None => tempfile::tempdir()?,
        };
        let data = staging.path().join("data");
        let direct = native.is_none() && checkout.is_dir();
        let timestamps = if direct {
            SnapshotTimestamps::capture_inner(checkout, &data, false)?
        } else {
            SnapshotTimestamps::capture(checkout, &data)?
        };
        let mut imports = Vec::new();
        let manages_native_roots = native.is_some();
        if direct {
            // Import the working tree itself so unchanged inode/ctime identities
            // remain reusable by both Casita's ingest cache and our digest memo.
            // A unique generation keeps the previous snapshot valid until the
            // metadata envelope below is durable, including after interruption.
            let native_root = format!(
                "{}/filesystem/{}",
                Self::native_prefix(root),
                blake3::hash(staging.path().as_os_str().as_encoded_bytes())
            );
            self.track_native_root(&native_root, None)?;
            self.client()?.import(&native_root, checkout.to_owned())?;
            imports.push(NativeRestore {
                importer: "filesystem".into(),
                root: native_root,
                paths: vec![NativePath {
                    source: PathBuf::new(),
                    destination: PathBuf::new(),
                }],
            });
        }
        match native {
            Some(NativeSource::Git { revision }) => {
                let repo = git2::Repository::open_bare(checkout)?;
                let mut objects = BTreeSet::new();
                repo.odb()?.foreach(|oid| {
                    objects.insert(oid.to_string());
                    true
                })?;
                if !objects.contains(revision) {
                    bail!("resolved Git revision is missing from the object database");
                }
                let prefix = Self::native_prefix(root);
                let local_root = format!(
                    "{prefix}/git/{}",
                    blake3::hash(&serde_json::to_vec(&objects)?).to_hex()
                );
                let mut prior_databases = Vec::new();
                let owner = blake3::hash(root.as_bytes()).to_hex().to_string();
                // A fresh Cargo home can have fewer objects than the durable cache.
                // Merge prior generations before replacing them, under the repository lock.
                for previous in self.read_native_journal(&owner)? {
                    if !previous.starts_with(&format!("{prefix}/git/")) || previous == local_root {
                        continue;
                    }
                    let prior = tempfile::tempdir()?;
                    let path = prior.path().join("database");
                    match self.client()?.restore("git", &previous, &path)? {
                        Some(true) => {
                            git2::Repository::open_bare(&path)?.odb()?.foreach(|oid| {
                                objects.insert(oid.to_string());
                                true
                            })?;
                            prior_databases.push(prior);
                        }
                        Some(false) => {}
                        None => bail!("Casita cannot restore previously retained Git objects"),
                    }
                }
                // Pin every cached object in an isolated import view, including objects no
                // longer reachable after a force-push. Do not modify Cargo's real refs.
                let source = tempfile::tempdir()?;
                let import_repo = git2::Repository::init_bare(source.path())?;
                let objects_path = std::fs::canonicalize(checkout.join("objects"))?;
                let mut alternates = format!("{}\n", serde_json::to_string(&objects_path)?);
                for previous in &prior_databases {
                    alternates.push_str(&format!(
                        "{}\n",
                        serde_json::to_string(&previous.path().join("database/objects"))?
                    ));
                }
                std::fs::write(
                    import_repo.path().join("objects/info/alternates"),
                    alternates,
                )?;
                let tags = import_repo.path().join("refs/tags");
                for oid in &objects {
                    std::fs::write(
                        tags.join(format!("cargo-retained-{oid}")),
                        format!("{oid}\n"),
                    )?;
                }
                let view = Self::staging_view(root);
                let native_root = format!(
                    "{}/git/{}",
                    Self::native_prefix(root),
                    blake3::hash(&serde_json::to_vec(&objects)?).to_hex()
                );
                if let Some(import) = self.capture_native(
                    "git",
                    json!({"path": source.path(), "view": view}),
                    json!({}),
                    &native_root,
                    &data,
                    None,
                )? {
                    imports.push(import);
                }
            }
            Some(NativeSource::Archives) => {
                for entry in walkdir::WalkDir::new(checkout) {
                    let entry = entry?;
                    if !entry.file_type().is_file()
                        || entry.path().extension().is_none_or(|ext| ext != "crate")
                    {
                        continue;
                    }
                    let relative = entry.path().strip_prefix(checkout)?.to_owned();
                    let native_root = format!(
                        "{}/blob/{}",
                        Self::native_prefix(root),
                        file_digest(entry.path())?
                    );
                    if let Some(import) = self.capture_native(
                        "blob",
                        json!({"path": entry.path(), "root": native_root}),
                        json!({}),
                        &native_root,
                        &data,
                        Some(&relative),
                    )? {
                        imports.push(import);
                    }
                }
            }
            Some(NativeSource::Tar { archive, package }) if archive.is_file() => {
                let native_root = format!(
                    "{}/tar/{}",
                    Self::native_prefix(root),
                    file_digest(&archive)?
                );
                if let Some(import) = self.capture_native(
                    "tar",
                    json!({"path": archive, "root": native_root}),
                    json!({"compression": "gzip"}),
                    &native_root,
                    &data,
                    Some(Path::new(package)),
                )? {
                    imports.push(import);
                }
            }
            _ => {}
        }
        serde_json::to_writer(
            std::fs::File::create(staging.path().join("timestamps.json"))?,
            &timestamps,
        )?;
        if !imports.is_empty() {
            serde_json::to_writer(
                std::fs::File::create(staging.path().join("native.json"))?,
                &imports,
            )?;
        }
        let object = self
            .client()?
            .import(&Self::snapshot_root(root), staging.path().to_owned())?;
        // The new snapshot is durable before any previous generation is released.
        self.prune_owned_roots(root, &imports)?;
        if manages_native_roots {
            self.prune_legacy_roots()?;
        }
        // Remove staging hardlinks before caching source change times.
        drop(staging);
        // Never bless concurrent edits as the bytes imported above.
        if snapshot_fingerprint(checkout, &mut files)? == fingerprint {
            let stamp = SnapshotStamp {
                fingerprint,
                object,
                imports,
                files,
            };
            Self::write_stamp(&stamp_path, &stamp)?;
        }
        Ok(())
    }

    fn write_stamp(path: &Path, stamp: &SnapshotStamp) -> CargoResult<()> {
        let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
        serde_json::to_writer(file.as_file_mut(), stamp)?;
        file.persist(path)?;
        Ok(())
    }

    fn stamp_path(&self, root: &str) -> PathBuf {
        self.repository
            .join(".cargo-snapshot-state")
            .join(format!("{}.json", blake3::hash(root.as_bytes()).to_hex()))
    }

    fn advance_snapshot_epoch(&self) -> CargoResult<()> {
        let directory = self.repository.join(".cargo-snapshot-state");
        std::fs::create_dir_all(&directory)?;
        let mut epoch = tempfile::NamedTempFile::new_in(&directory)?;
        use std::io::Write as _;
        let identity = epoch.path().display().to_string();
        write!(epoch, "{identity}")?;
        epoch.as_file().sync_all()?;
        epoch.persist(directory.join("epoch"))?;
        #[cfg(unix)]
        std::fs::File::open(directory)?.sync_all()?;
        Ok(())
    }

    fn stamp_is_live(&self, root: &str, stamp: &SnapshotStamp) -> CargoResult<bool> {
        self.with_catalog(|roots| {
            roots.get(&Self::snapshot_root(root)) == Some(&stamp.object)
                && stamp
                    .imports
                    .iter()
                    .all(|import| roots.contains_key(&import.root))
        })
    }

    fn with_catalog<T>(
        &self,
        lookup: impl FnOnce(&BTreeMap<String, String>) -> T,
    ) -> CargoResult<T> {
        // One catalog read per observed publication epoch, rather than a CLI
        // invocation for every unchanged dependency. External root maintenance
        // is observed on the next Cargo invocation as well.
        type Catalog = (Vec<u8>, BTreeMap<String, String>);
        static CATALOGS: Mutex<BTreeMap<PathBuf, Catalog>> = Mutex::new(BTreeMap::new());
        let epoch = match std::fs::read(self.repository.join(".cargo-snapshot-state/epoch")) {
            Ok(epoch) => epoch,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let mut catalogs = CATALOGS.lock().unwrap();
        if catalogs
            .get(&self.repository)
            .is_none_or(|(seen, _)| seen != &epoch)
        {
            catalogs.insert(self.repository.clone(), (epoch, self.roots("cargo")?));
        }
        let (_, roots) = &catalogs[&self.repository];
        Ok(lookup(roots))
    }

    fn capture_native(
        &self,
        importer: &str,
        parameters: Value,
        options: Value,
        root: &str,
        data: &Path,
        relative: Option<&Path>,
    ) -> CargoResult<Option<NativeRestore>> {
        let staging_root = if importer == "git" {
            Some(format!(
                "git/{}",
                parameters
                    .get("view")
                    .and_then(Value::as_str)
                    .context("missing Git view")?
            ))
        } else {
            None
        };
        self.track_native_root(root, staging_root.as_deref())?;
        let Some(result) = self.client()?.importer(importer, parameters, options)? else {
            return Ok(None);
        };
        if importer == "git" {
            let object = result
                .get("view")
                .and_then(Value::as_str)
                .context("Casita Git import returned no view")?;
            self.root_command(&["root", "set", root, object])?;
        }
        // Exercise the matching restore before omitting anything from the snapshot.
        let scratch = tempfile::tempdir_in(data.parent().unwrap())?;
        let restored = scratch.path().join("restored");
        match self.client()?.restore(importer, root, &restored)? {
            None => return Ok(None),
            Some(false) => bail!("Casita did not retain imported root `{root}`"),
            Some(true) => {}
        }
        let mut paths = Vec::new();
        match importer {
            "git" => {
                std::fs::remove_dir_all(data.join("objects"))?;
                paths.push(NativePath {
                    source: "objects".into(),
                    destination: "objects".into(),
                });
            }
            "blob" => {
                let relative = relative.unwrap();
                if file_digest(&restored)? != file_digest(&data.join(relative))? {
                    bail!("Casita restored different archive bytes for `{root}`");
                }
                std::fs::remove_file(data.join(relative))?;
                paths.push(NativePath {
                    source: PathBuf::new(),
                    destination: relative.to_owned(),
                });
            }
            "tar" => {
                let package = relative.unwrap();
                for entry in walkdir::WalkDir::new(data) {
                    let entry = entry?;
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let relative = entry.path().strip_prefix(data)?;
                    let source = package.join(relative);
                    if source.to_str().is_none() {
                        continue;
                    }
                    let native = restored.join(&source);
                    // Cargo's .cargo-ok, edits, modes, and archive filtering remain authoritative.
                    if native.symlink_metadata().is_ok_and(|m| m.is_file())
                        && file_digest(&native)? == file_digest(entry.path())?
                        && std::fs::metadata(&native)?.permissions()
                            == std::fs::metadata(entry.path())?.permissions()
                    {
                        paths.push(NativePath {
                            source,
                            destination: relative.to_owned(),
                        });
                    }
                }
                for path in &paths {
                    std::fs::remove_file(data.join(&path.destination))?;
                }
            }
            _ => unreachable!(),
        }
        Ok(Some(NativeRestore {
            importer: importer.to_owned(),
            root: root.to_owned(),
            paths,
        }))
    }

    fn lock_repository(&self) -> CargoResult<std::fs::File> {
        std::fs::create_dir_all(&self.repository)?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.repository.join(".cargo-storage.lock"))?;
        crate::util::flock::lock_exclusive(&lock)?;
        self.initialize_native_journal()?;
        Ok(lock)
    }

    fn native_prefix(root: &str) -> String {
        format!("cargo/native/{}", blake3::hash(root.as_bytes()).to_hex())
    }

    fn staging_view(root: &str) -> String {
        format!("cargo-stage-{}", blake3::hash(root.as_bytes()).to_hex())
    }

    fn root_command(&self, args: &[&str]) -> CargoResult<String> {
        let output = Command::new("casita")
            .arg("--repository")
            .arg(&self.repository)
            .args(args)
            .output()
            .context("could not run Casita root maintenance")?;
        if !output.status.success() {
            bail!(
                "Casita root maintenance failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(String::from_utf8(output.stdout)?)
    }

    fn roots(&self, prefix: &str) -> CargoResult<BTreeMap<String, String>> {
        self.root_command(&["root", "ls", prefix])?
            .lines()
            .map(|line| {
                let (object, name) = line
                    .split_once("  ")
                    .context("invalid Casita root listing")?;
                if object.is_empty() || name.is_empty() {
                    bail!("invalid Casita root listing");
                }
                Ok((name.to_owned(), object.to_owned()))
            })
            .collect()
    }

    fn journal_path(&self, owner: &str) -> PathBuf {
        self.repository
            .join(".cargo-native-roots")
            .join(format!("{owner}.json"))
    }

    fn read_native_journal(&self, owner: &str) -> CargoResult<BTreeSet<String>> {
        match std::fs::read(self.journal_path(owner)) {
            Ok(contents) => {
                let roots: BTreeSet<String> = serde_json::from_slice(&contents)?;
                let prefix = format!("cargo/native/{owner}/");
                let staging = format!("git/cargo-stage-{owner}");
                if roots
                    .iter()
                    .any(|root| !root.starts_with(&prefix) && root != &staging)
                {
                    bail!("Casita native root journal contains a root belonging to another owner");
                }
                Ok(roots)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
            Err(error) => Err(error.into()),
        }
    }

    fn write_native_journal(&self, owner: &str, roots: &BTreeSet<String>) -> CargoResult<()> {
        let path = self.journal_path(owner);
        let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
        serde_json::to_writer(file.as_file_mut(), roots)?;
        file.as_file().sync_all()?;
        file.persist(path)?;
        #[cfg(unix)]
        std::fs::File::open(self.repository.join(".cargo-native-roots"))?.sync_all()?;
        Ok(())
    }

    fn initialize_native_journal(&self) -> CargoResult<()> {
        let directory = self.repository.join(".cargo-native-roots");
        let ready = directory.join("ready");
        if ready.exists() {
            return Ok(());
        }
        std::fs::create_dir_all(&directory)?;
        let mut owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for root in self
            .roots("cargo/native")?
            .into_keys()
            .chain(self.roots("git")?.into_keys())
        {
            let owner = root
                .strip_prefix("cargo/native/")
                .and_then(|rest| rest.split('/').next())
                .or_else(|| root.strip_prefix("git/cargo-stage-"));
            if let Some(owner) = owner
                .filter(|owner| owner.len() == 64 && owner.bytes().all(|c| c.is_ascii_hexdigit()))
            {
                owners.entry(owner.to_owned()).or_default().insert(root);
            }
        }
        for (owner, roots) in owners {
            self.write_native_journal(&owner, &roots)?;
        }
        std::fs::File::create(ready)?.sync_all()?;
        #[cfg(unix)]
        std::fs::File::open(directory)?.sync_all()?;
        Ok(())
    }

    fn track_native_root(&self, root: &str, extra: Option<&str>) -> CargoResult<()> {
        let owner = root
            .strip_prefix("cargo/native/")
            .and_then(|rest| rest.split('/').next())
            .context("native root has no owner")?;
        let mut roots = self.read_native_journal(owner)?;
        roots.insert(root.to_owned());
        if let Some(extra) = extra {
            roots.insert(extra.to_owned());
        }
        // Record intent before import, so interrupted publications remain discoverable.
        self.write_native_journal(owner, &roots)
    }

    fn remove_native_root(&self, root: &str) -> CargoResult<()> {
        if let Err(error) = self.root_command(&["root", "rm", root]) {
            // An interrupted import may never have published its root, or cleanup
            // may have removed it before the journal update reached disk.
            if self.roots(root)?.contains_key(root) {
                return Err(error);
            }
        }
        Ok(())
    }

    fn prune_owned_roots(&self, root: &str, imports: &[NativeRestore]) -> CargoResult<()> {
        let owner = blake3::hash(root.as_bytes()).to_hex().to_string();
        let keep = imports
            .iter()
            .map(|import| import.root.clone())
            .collect::<BTreeSet<_>>();
        let mut recorded = self.read_native_journal(&owner)?;
        for name in recorded.difference(&keep).cloned().collect::<Vec<_>>() {
            self.remove_native_root(&name)?;
            recorded.remove(&name);
            self.write_native_journal(&owner, &recorded)?;
        }
        Ok(())
    }

    fn prune_legacy_roots(&self) -> CargoResult<()> {
        // Older snapshots shared content-addressed roots without ownership. Reconcile
        // once per process under the same lock used for publication and restoration.
        static CHECKED: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());
        let mut checked = CHECKED.lock().unwrap();
        if checked.contains(&self.repository) {
            return Ok(());
        }
        let candidates = self
            .roots("cargo/objects")?
            .into_keys()
            .filter(|name| {
                ["cargo/objects/blobs/", "cargo/objects/tar/"]
                    .iter()
                    .any(|prefix| {
                        name.strip_prefix(prefix).is_some_and(|suffix| {
                            suffix.len() == 64 && suffix.bytes().all(|c| c.is_ascii_hexdigit())
                        })
                    })
            })
            .chain(self.roots("git")?.into_keys().filter(|name| {
                name.strip_prefix("git/cargo-").is_some_and(|suffix| {
                    suffix.len() == 64 && suffix.bytes().all(|c| c.is_ascii_hexdigit())
                })
            }))
            .collect::<BTreeSet<_>>();
        if !candidates.is_empty() {
            let mut keep = BTreeSet::new();
            for (_, object) in self.roots("cargo/snapshots/v2")? {
                let listing = self.root_command(&["tree", "list", &object])?;
                for line in listing.lines() {
                    let fields = line.split_whitespace().collect::<Vec<_>>();
                    if fields.len() == 4 && fields[0] == "f" && fields[2] == "native.json" {
                        let contents = self.root_command(&["cat", fields[3], "--verified"])?;
                        let imports: Vec<NativeRestore> = serde_json::from_str(&contents)?;
                        keep.extend(imports.into_iter().map(|import| import.root));
                    }
                }
            }
            for name in candidates.difference(&keep) {
                self.root_command(&["root", "rm", name])?;
            }
        }
        checked.insert(self.repository.clone());
        Ok(())
    }

    fn persist_archive_locked(&self, key: &str, path: &Path) -> CargoResult<()> {
        let staging = tempfile::tempdir()?;
        let archive = staging.path().join("archive.crate");
        std::fs::copy(path, &archive)?;
        filetime::set_file_mtime(
            &archive,
            filetime::FileTime::from_last_modification_time(&std::fs::metadata(path)?),
        )?;
        self.import_snapshot_locked(
            &format!("cargo/registry/archive/{key}"),
            &Filesystem::new(staging.path().to_owned()),
            Some(NativeSource::Archives),
        )
    }

    fn with_deferred<T>(&self, f: impl FnOnce(&mut DeferredPersistence) -> T) -> T {
        // Storage handles are created per request; the queue belongs to the repository.
        static DEFERRED: Mutex<BTreeMap<PathBuf, DeferredPersistence>> =
            Mutex::new(BTreeMap::new());
        let mut deferred = DEFERRED.lock().unwrap();
        f(deferred.entry(self.repository.clone()).or_default())
    }

    /// Queues a request if a deferral scope is open. Repeated requests for
    /// one root collapse into a single import of its final contents.
    fn defer(&self, root: &str, request: DeferredRequest) -> bool {
        self.with_deferred(|deferred| {
            if deferred.depth == 0 {
                return false;
            }
            deferred.requests.insert(root.to_owned(), request);
            true
        })
    }

    fn persist_requests(&self, requests: BTreeMap<String, DeferredRequest>) -> CargoResult<()> {
        let archives = requests
            .values()
            .filter_map(|request| match request {
                DeferredRequest::Archive { key, .. } => Some(key.clone()),
                DeferredRequest::Dependency { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        for request in requests.into_values() {
            match request {
                DeferredRequest::Archive { key, path } => {
                    let _lock = self.lock_repository()?;
                    self.persist_archive_locked(&key, &path)?;
                }
                DeferredRequest::Dependency {
                    cache,
                    key,
                    directory,
                } => {
                    if cache == DependencyCache::RegistrySource {
                        // An interrupted deferral can leave a downloaded archive
                        // unpersisted. Cargo will not download it again, so
                        // persist it alongside the source unpacked from it.
                        let archive_key = format!("{key}.crate");
                        let archive = self.archives.as_path_unlocked().join(&archive_key);
                        if !archives.contains(&archive_key)
                            && archive.is_file()
                            && !self.registry_archive_is_retained(&archive_key)?
                        {
                            let _lock = self.lock_repository()?;
                            self.persist_archive_locked(&archive_key, &archive)?;
                        }
                    }
                    self.persist_dependency_now(cache, &key, &directory)?;
                }
            }
        }
        Ok(())
    }

    fn persist_dependency_now(
        &self,
        cache: DependencyCache,
        key: &str,
        directory: &Filesystem,
    ) -> CargoResult<()> {
        let native = if matches!(cache, DependencyCache::RegistrySource) {
            key.rsplit_once('/').map(|(_, package)| NativeSource::Tar {
                archive: self
                    .registry_archive_dir()
                    .as_path_unlocked()
                    .join(format!("{key}.crate")),
                package,
            })
        } else {
            None
        };
        self.import_snapshot(&Self::dependency_root(cache, key), directory, native)
    }

    fn client(&self) -> CargoResult<CasitaClient> {
        #[cfg(unix)]
        {
            let endpoint = self.endpoint();
            match UnixStream::connect(&endpoint) {
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    self.start_server()?
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "could not connect to Casita IPC endpoint {}",
                            endpoint.display()
                        )
                    });
                }
            }
            CasitaClient::connect(endpoint)
        }
        #[cfg(windows)]
        {
            let endpoint = self.endpoint();
            match OpenOptions::new().read(true).write(true).open(&endpoint) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    self.start_server()?
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("could not connect to Casita IPC endpoint {endpoint}")
                    });
                }
            }
            CasitaClient::connect(endpoint)
        }
        #[cfg(not(any(unix, windows)))]
        {
            bail!("Casita artifact storage currently requires Unix-domain sockets")
        }
    }

    #[cfg(any(unix, windows))]
    fn start_server(&self) -> CargoResult<()> {
        let endpoint = self.endpoint();
        #[cfg(unix)]
        let ready = || UnixStream::connect(&endpoint).is_ok();
        #[cfg(windows)]
        let ready = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&endpoint)
                .is_ok()
        };
        #[cfg(unix)]
        let endpoint_name = endpoint.display().to_string();
        #[cfg(windows)]
        let endpoint_name = endpoint.clone();
        start_ipc_server(
            Command::new("casita")
                .arg("--repository")
                .arg(&self.repository)
                .arg("ipc"),
            &endpoint_name,
            ready,
            Duration::from_secs(5),
        )
    }
}

#[cfg(any(unix, windows))]
fn start_ipc_server(
    command: &mut Command,
    endpoint: &str,
    ready: impl Fn() -> bool,
    timeout: Duration,
) -> CargoResult<()> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    // A file avoids blocking the daemon when its stderr exceeds a pipe buffer.
    let mut stderr = tempfile::tempfile()?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr.try_clone()?)
        .spawn()
        .context("could not start the Casita IPC service")?;
    let started = Instant::now();
    let failure = loop {
        if ready() {
            // Reap the daemon if it exits while this Cargo process is still alive.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            return Ok(());
        }
        match child.try_wait() {
            Ok(Some(status)) => break format!("exited with {status}"),
            Ok(None) => {}
            Err(error) => break format!("could not check process status: {error}"),
        }
        if started.elapsed() >= timeout {
            break format!(
                "did not become ready within {} seconds",
                timeout.as_secs_f64()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    // Do not leave a timed-out startup running after Cargo reports failure.
    let _ = child.kill();
    let _ = child.wait();
    let length = stderr.metadata()?.len();
    stderr.seek(SeekFrom::Start(length.saturating_sub(64 * 1024)))?;
    let mut output = Vec::new();
    stderr.take(64 * 1024).read_to_end(&mut output)?;
    bail!(
        "Casita IPC service {failure} at {endpoint}\n{}",
        String::from_utf8_lossy(&output).trim_end()
    )
}

impl ArtifactStorage for CasitaArtifactStorage {
    fn registry_archive_dir(&self) -> Filesystem {
        self.archives.clone()
    }

    fn prepare_registry_archives(&self) -> CargoResult<()> {
        let _timing = StorageTiming::new("archives.prepare", "registry");
        let _lock = self.lock_repository()?;
        let checkout = self.registry_archive_dir();
        let directory = checkout.as_path_unlocked();
        let ready = directory.join(".casita-ready");
        let identity = blake3::hash(self.repository.as_os_str().as_encoded_bytes())
            .to_hex()
            .to_string();
        if std::fs::read(&ready).ok().as_deref() == Some(identity.as_bytes()) {
            return Ok(());
        }
        // Migrate the old whole-cache snapshot once, before retiring its root.
        // Restore separately so existing local archives do not hide legacy entries.
        let legacy = tempfile::tempdir()?;
        let legacy_cache = Filesystem::new(legacy.path().join("archives"));
        self.checkout_root_locked(Self::registry_root(), &legacy_cache)?;
        std::fs::create_dir_all(directory)?;
        if legacy_cache.as_path_unlocked().exists() {
            for entry in walkdir::WalkDir::new(legacy_cache.as_path_unlocked()) {
                let entry = entry?;
                if entry.file_type().is_file()
                    && entry.path().extension().is_some_and(|ext| ext == "crate")
                {
                    let relative = entry.path().strip_prefix(legacy_cache.as_path_unlocked())?;
                    let mut parent = directory.to_owned();
                    for component in relative.parent().unwrap().components() {
                        parent.push(component);
                        match std::fs::symlink_metadata(&parent) {
                            Ok(metadata) if metadata.is_dir() => {}
                            Ok(_) => bail!("invalid archive cache directory"),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                                std::fs::create_dir(&parent)?
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let destination = safe_snapshot_path(directory, relative)?;
                    if !destination.exists() {
                        std::fs::create_dir_all(destination.parent().unwrap())?;
                        std::fs::copy(entry.path(), &destination)?;
                        filetime::set_file_mtime(
                            &destination,
                            filetime::FileTime::from_last_modification_time(&entry.metadata()?),
                        )?;
                    }
                }
            }
        }
        let mut migrated = false;
        for entry in walkdir::WalkDir::new(directory) {
            let entry = entry?;
            if entry.file_type().is_file()
                && entry.path().extension().is_some_and(|ext| ext == "crate")
            {
                let key = entry
                    .path()
                    .strip_prefix(directory)?
                    .to_str()
                    .context("archive key is not UTF-8")?
                    .replace('\\', "/");
                self.persist_archive_locked(&key, entry.path())?;
                migrated = true;
            }
        }
        if migrated {
            for root in [
                Self::snapshot_root(Self::registry_root()),
                "cargo/snapshots/v1/registry/archives".into(),
                Self::registry_root().into(),
            ] {
                if self.roots(&root)?.contains_key(&root) {
                    self.root_command(&["root", "rm", &root])?;
                }
            }
            self.prune_owned_roots(Self::registry_root(), &[])?;
        }
        std::fs::write(ready, identity)?;
        Ok(())
    }

    fn registry_archive_is_retained(&self, key: &str) -> CargoResult<bool> {
        let _lock = self.lock_repository()?;
        self.with_catalog(|roots| {
            roots.contains_key(&Self::snapshot_root(&format!(
                "cargo/registry/archive/{key}"
            )))
        })
    }

    fn prepare_registry_archive(&self, key: &str, path: &Path) -> CargoResult<()> {
        if path.is_file() && path.metadata()?.len() > 0 {
            return Ok(());
        }
        let _lock = self.lock_repository()?;
        if path.is_file() && path.metadata()?.len() > 0 {
            return Ok(());
        }
        std::fs::create_dir_all(path.parent().context("archive has no parent")?)?;
        let scratch = tempfile::tempdir_in(path.parent().unwrap())?;
        let restored = Filesystem::new(scratch.path().join("archive"));
        self.checkout_root_locked(&format!("cargo/registry/archive/{key}"), &restored)?;
        let archive = restored.as_path_unlocked().join("archive.crate");
        if archive.is_file() {
            std::fs::rename(archive, path)?;
        }
        Ok(())
    }

    fn persist_registry_archive(&self, key: &str, path: &Path) -> CargoResult<()> {
        let request = DeferredRequest::Archive {
            key: key.to_owned(),
            path: path.to_owned(),
        };
        if self.defer(&format!("cargo/registry/archive/{key}"), request) {
            return Ok(());
        }
        let _lock = self.lock_repository()?;
        self.persist_archive_locked(key, path)
    }

    fn prepare_dependency_artifacts(
        &self,
        cache: DependencyCache,
        key: &str,
        directory: &Filesystem,
    ) -> CargoResult<()> {
        self.checkout_root(&Self::dependency_root(cache, key), directory)
    }

    fn persist_dependency_artifacts(
        &self,
        cache: DependencyCache,
        key: &str,
        directory: &Filesystem,
    ) -> CargoResult<()> {
        let request = DeferredRequest::Dependency {
            cache,
            key: key.to_owned(),
            directory: directory.clone(),
        };
        if self.defer(&Self::dependency_root(cache, key), request) {
            return Ok(());
        }
        self.persist_dependency_now(cache, key, directory)
    }

    fn persist_git_database(
        &self,
        key: &str,
        directory: &Filesystem,
        revision: &str,
    ) -> CargoResult<()> {
        self.import_snapshot(
            &Self::dependency_root(DependencyCache::GitDatabase, key),
            directory,
            Some(NativeSource::Git { revision }),
        )
    }

    fn workspace_artifact_dir(&self, workspace_manifest_path: &Path) -> Option<Filesystem> {
        Some(self.checkout_dir(&format!(
            "workspace-{}",
            Self::workspace_hash(workspace_manifest_path)
        )))
    }

    fn prepare_workspace_artifacts(
        &self,
        workspace_manifest_path: &Path,
        target_dir: &Filesystem,
        build_dir: &Filesystem,
    ) -> CargoResult<()> {
        for (root, checkout) in self.workspace_roots(workspace_manifest_path, target_dir, build_dir)
        {
            self.checkout_root(&root, &checkout)?;
        }
        Ok(())
    }

    fn persist_workspace_artifacts(
        &self,
        workspace_manifest_path: &Path,
        target_dir: &Filesystem,
        build_dir: &Filesystem,
    ) -> CargoResult<()> {
        for (root, checkout) in self.workspace_roots(workspace_manifest_path, target_dir, build_dir)
        {
            self.import_root(&root, &checkout)?;
        }
        Ok(())
    }

    fn participates_in_global_cache(&self) -> bool {
        false
    }

    fn defer_persistence(&self) {
        self.with_deferred(|deferred| deferred.depth += 1);
    }

    fn finish_deferred_persistence(&self, background: bool) -> CargoResult<()> {
        let requests = self.with_deferred(|deferred| {
            deferred.depth = deferred.depth.saturating_sub(1);
            if deferred.depth == 0 {
                std::mem::take(&mut deferred.requests)
            } else {
                BTreeMap::new()
            }
        });
        if requests.is_empty() {
            return Ok(());
        }
        if !background {
            return self.persist_requests(requests);
        }
        let storage = self.clone();
        let handle = std::thread::Builder::new()
            .name("cargo-casita-persist".into())
            .spawn(move || storage.persist_requests(requests))?;
        self.with_deferred(|deferred| deferred.background.push(handle));
        Ok(())
    }

    fn wait_for_persistence(&self) -> CargoResult<()> {
        let handles = self.with_deferred(|deferred| std::mem::take(&mut deferred.background));
        let mut result = Ok(());
        for handle in handles {
            let joined = match handle.join() {
                Ok(joined) => joined,
                Err(_) => Err(anyhow::format_err!("Casita persistence thread panicked")),
            };
            // Wait for every thread before reporting the first failure.
            if result.is_ok() {
                result = joined;
            }
        }
        result
    }
}

#[derive(Default)]
struct DeferredPersistence {
    depth: usize,
    /// Requests keyed by Casita root.
    requests: BTreeMap<String, DeferredRequest>,
    background: Vec<std::thread::JoinHandle<CargoResult<()>>>,
}

enum DeferredRequest {
    Archive {
        key: String,
        path: PathBuf,
    },
    Dependency {
        cache: DependencyCache,
        key: String,
        directory: Filesystem,
    },
}

enum NativeSource<'a> {
    Git { revision: &'a str },
    Archives,
    Tar { archive: PathBuf, package: &'a str },
}

#[derive(Serialize, Deserialize)]
struct NativeRestore {
    importer: String,
    root: String,
    paths: Vec<NativePath>,
}

#[derive(Serialize, Deserialize)]
struct NativePath {
    source: PathBuf,
    destination: PathBuf,
}

fn safe_snapshot_path(base: &Path, relative: &Path) -> CargoResult<PathBuf> {
    if relative
        .components()
        .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        bail!("invalid relative path in Casita snapshot");
    }
    let mut path = base.to_owned();
    for component in relative.components() {
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            bail!("symlink in Casita snapshot path");
        }
        path.push(component);
    }
    Ok(path)
}

fn file_digest(path: &Path) -> CargoResult<String> {
    let mut hash = blake3::Hasher::new();
    hash.update_reader(std::fs::File::open(path)?)?;
    Ok(hash.finalize().to_hex().to_string())
}

#[derive(Serialize, Deserialize)]
struct SnapshotStamp {
    fingerprint: String,
    object: String,
    imports: Vec<NativeRestore>,
    #[serde(default)]
    files: BTreeMap<String, CachedFileDigest>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CachedFileDigest {
    identity: Option<String>,
    digest: String,
}

fn file_identity(metadata: &std::fs::Metadata) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // Coarse change times cannot safely distinguish same-size, same-mtime edits.
        if metadata.ctime_nsec() == 0 {
            return None;
        }
        Some(format!(
            "{}:{}:{}:{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mode(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec()
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        // A portable modification time alone is not proof that bytes are unchanged.
        None
    }
}

/// Hash the inputs to a snapshot without staging, importing, or restoring them.
/// Include bytes as well as metadata: source edits with preserved mtimes must
/// still be persisted. Directory times, permissions, and symlinks are inputs too.
fn snapshot_fingerprint(
    directory: &Path,
    files: &mut BTreeMap<String, CachedFileDigest>,
) -> CargoResult<String> {
    let _timing = StorageTiming::new("snapshot.fingerprint", "filesystem");
    let mut hash = blake3::Hasher::new();
    let previous = std::mem::take(files);
    let mut bytes_hashed = 0u64;
    let mut observed_digests = BTreeMap::new();
    #[cfg(unix)]
    let mut links = BTreeMap::new();
    if !directory.exists() {
        hash.update(b"missing");
        return Ok(hash.finalize().to_hex().to_string());
    }
    for entry in walkdir::WalkDir::new(directory).sort_by_file_name() {
        let entry = entry?;
        let relative = entry.path().strip_prefix(directory)?;
        let key = SnapshotTimestamps::path_key(relative);
        hash.update(key.as_bytes());
        let metadata = entry.metadata()?;
        let mtime = filetime::FileTime::from_last_modification_time(&metadata);
        hash.update(&mtime.unix_seconds().to_le_bytes());
        hash.update(&mtime.nanoseconds().to_le_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = metadata.permissions().mode();
            hash.update(&mode.to_le_bytes());
            let canonical = if metadata.is_dir() || mode & 0o111 != 0 {
                0o755
            } else {
                0o644
            };
            if !metadata.is_symlink() && mode & 0o7777 != canonical {
                // Older filesystem snapshots retained only the executable bit.
                // Refresh those which require the new explicit mode metadata.
                hash.update(b"permissions-v1");
            }
        }
        #[cfg(not(unix))]
        hash.update(&[u8::from(metadata.permissions().readonly())]);
        if metadata.is_file() {
            hash.update(b"file");
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                if let Some(first) = links.get(&(metadata.dev(), metadata.ino())) {
                    hash.update(b"hardlink");
                    hash.update(String::as_bytes(first));
                } else {
                    links.insert((metadata.dev(), metadata.ino()), key.clone());
                }
            }
            let identity = file_identity(&metadata);
            #[cfg(unix)]
            let hardlinked = {
                use std::os::unix::fs::MetadataExt as _;
                metadata.nlink() > 1
            };
            #[cfg(not(unix))]
            let hardlinked = false;
            let digest = match identity
                .as_ref()
                .filter(|_| hardlinked)
                .and_then(|identity| observed_digests.get(identity))
                .or_else(|| {
                    previous
                        .get(&key)
                        .filter(|cached| identity.is_some() && cached.identity == identity)
                        .map(|cached| &cached.digest)
                }) {
                Some(digest) => digest.clone(),
                None => {
                    bytes_hashed = bytes_hashed.saturating_add(metadata.len());
                    file_digest(entry.path())?
                }
            };
            if let Some(identity) = identity.as_ref().filter(|_| hardlinked) {
                observed_digests.insert(identity.clone(), digest.clone());
            }
            hash.update(digest.as_bytes());
            files.insert(key, CachedFileDigest { identity, digest });
        } else if metadata.is_symlink() {
            hash.update(b"symlink");
            hash.update(
                SnapshotTimestamps::path_key(&std::fs::read_link(entry.path())?).as_bytes(),
            );
        } else if metadata.is_dir() {
            hash.update(b"directory");
        }
    }
    tracing::debug!(path = %directory.display(), bytes_hashed, "Casita snapshot fingerprint");
    Ok(hash.finalize().to_hex().to_string())
}

#[derive(Default, Serialize, Deserialize)]
struct SnapshotTimestamps {
    entries: BTreeMap<String, ModificationTime>,
    #[serde(default)]
    hardlinks: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct ModificationTime {
    seconds: i64,
    nanos: u32,
    #[serde(default)]
    mode: Option<u32>,
}

impl SnapshotTimestamps {
    fn path_key(path: &Path) -> String {
        // Native path components also cover filenames that are not UTF-8.
        let mut hash = blake3::Hasher::new();
        for component in path.components() {
            hash.update(component.as_os_str().as_encoded_bytes());
            hash.update(&[0]);
        }
        hash.finalize().to_hex().to_string()
    }

    fn capture(source: &Path, data: &Path) -> CargoResult<Self> {
        Self::capture_inner(source, data, true)
    }

    fn capture_inner(source: &Path, data: &Path, stage_files: bool) -> CargoResult<Self> {
        std::fs::create_dir(data)?;
        let mut timestamps = Self::default();
        #[cfg(unix)]
        let mut links = BTreeMap::new();
        match std::fs::metadata(source) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => bail!("artifact source is not a directory: {}", source.display()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(timestamps),
            Err(error) => return Err(error.into()),
        }
        for entry in walkdir::WalkDir::new(source) {
            let entry = entry?;
            let relative = entry.path().strip_prefix(source)?;
            let destination = data.join(relative);
            let metadata = entry.metadata()?;
            if entry.depth() != 0 {
                if metadata.is_dir() {
                    if stage_files {
                        std::fs::create_dir(&destination)?;
                    }
                } else if metadata.is_file() {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt as _;
                        let key = Self::path_key(relative);
                        if let Some(first) = links.get(&(metadata.dev(), metadata.ino())) {
                            timestamps.hardlinks.insert(key, String::clone(first));
                        } else {
                            links.insert((metadata.dev(), metadata.ino()), key);
                        }
                    }
                    if stage_files {
                        #[cfg(unix)]
                        let linked = std::fs::hard_link(entry.path(), &destination).is_ok();
                        #[cfg(not(unix))]
                        let linked = false;
                        if !linked {
                            std::fs::copy(entry.path(), &destination)?;
                        }
                    }
                } else if metadata.is_symlink() && stage_files {
                    let target = std::fs::read_link(entry.path())?;
                    #[cfg(unix)]
                    std::os::unix::fs::symlink(target, &destination)?;
                    #[cfg(windows)]
                    {
                        use std::os::windows::fs::FileTypeExt as _;
                        if metadata.file_type().is_symlink_dir() {
                            std::os::windows::fs::symlink_dir(target, &destination)?;
                        } else {
                            std::os::windows::fs::symlink_file(target, &destination)?;
                        }
                    }
                } else if !metadata.is_symlink() {
                    continue;
                }
            }
            let mtime = filetime::FileTime::from_last_modification_time(&metadata);
            timestamps.entries.insert(
                Self::path_key(relative),
                ModificationTime {
                    seconds: mtime.unix_seconds(),
                    nanos: mtime.nanoseconds(),
                    mode: {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt as _;
                            Some(metadata.permissions().mode() & 0o7777)
                        }
                        #[cfg(not(unix))]
                        {
                            None
                        }
                    },
                },
            );
        }
        Ok(timestamps)
    }

    fn restore(&self, data: &Path, native_directories: &[PathBuf]) -> CargoResult<()> {
        if !std::fs::symlink_metadata(data)?.is_dir() {
            bail!("artifact snapshot data is not a directory");
        }
        // An absent source is stored as an empty snapshot, without a root timestamp.
        if self.entries.is_empty() && std::fs::read_dir(data)?.next().transpose()?.is_none() {
            return Ok(());
        }
        // Restore relationships before directory timestamps. Cargo otherwise
        // relinks lifted outputs and makes an unchanged target look modified.
        if !self.hardlinks.is_empty() {
            let mut files = BTreeMap::new();
            for entry in walkdir::WalkDir::new(data) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    files.insert(
                        Self::path_key(entry.path().strip_prefix(data)?),
                        entry.into_path(),
                    );
                }
            }
            for (alias, first) in &self.hardlinks {
                // Native Git views may replace original pack paths with loose objects.
                if let (Some(alias), Some(first)) = (files.get(alias), files.get(first)) {
                    std::fs::remove_file(alias)?;
                    if std::fs::hard_link(first, alias).is_err() {
                        std::fs::copy(first, alias)?;
                    }
                }
            }
        }
        for entry in walkdir::WalkDir::new(data).contents_first(true) {
            let entry = entry?;
            let relative = entry.path().strip_prefix(data)?;
            let timestamp = self.entries.get(&Self::path_key(relative));
            if timestamp.is_none()
                && native_directories
                    .iter()
                    .any(|path| relative.starts_with(path))
            {
                // Native Git restoration may change packs into loose objects.
                continue;
            }
            let timestamp = timestamp.with_context(|| {
                format!("missing artifact timestamp for {}", entry.path().display())
            })?;
            #[cfg(unix)]
            if !entry.file_type().is_symlink() {
                use std::os::unix::fs::PermissionsExt as _;
                if let Some(mode) = timestamp.mode {
                    std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(mode))?;
                }
            }
            if timestamp.nanos >= 1_000_000_000 {
                bail!("invalid artifact timestamp for {}", entry.path().display());
            }
            let mtime = filetime::FileTime::from_unix_time(timestamp.seconds, timestamp.nanos);
            if entry.file_type().is_symlink() {
                let atime = filetime::FileTime::from_last_access_time(&entry.metadata()?);
                filetime::set_symlink_file_times(entry.path(), atime, mtime)?;
            } else {
                filetime::set_file_mtime(entry.path(), mtime)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct InitializeParams {
    versions: [u64; 1],
    max_frame_bytes: usize,
}

#[derive(Deserialize)]
struct InitializeResult {
    version: u64,
    max_frame_bytes: usize,
    capabilities: Vec<String>,
}

#[derive(Serialize)]
struct ArtifactParams {
    root: String,
    path: PathBuf,
}

#[derive(Deserialize)]
struct CheckoutResult {
    present: bool,
}

#[derive(Deserialize)]
struct ImportResult {
    object: String,
}

struct CasitaClient {
    client: jsonrpc::Client,
}

impl CasitaClient {
    #[cfg(any(unix, windows))]
    fn connect(endpoint: LocalEndpoint) -> CargoResult<Self> {
        let transport = LocalSocketTransport::connect(endpoint)?;
        let client = jsonrpc::Client::with_transport(transport);
        let initialized: InitializeResult = call(
            &client,
            "rpc.initialize",
            InitializeParams {
                versions: [IPC_VERSION],
                max_frame_bytes: IPC_MAX_FRAME_BYTES,
            },
        )?;
        if initialized.version != IPC_VERSION
            || initialized.max_frame_bytes > IPC_MAX_FRAME_BYTES
            || !initialized
                .capabilities
                .iter()
                .any(|capability| capability == "artifact.checkout")
            || !initialized
                .capabilities
                .iter()
                .any(|capability| capability == "artifact.import")
        {
            bail!("Casita IPC service returned an incompatible initialization response")
        }
        Ok(Self { client })
    }

    #[cfg(any(unix, windows))]
    fn checkout(&self, root: &str, path: PathBuf) -> CargoResult<bool> {
        let result: CheckoutResult = call(
            &self.client,
            "artifact.checkout",
            ArtifactParams {
                root: root.to_owned(),
                path,
            },
        )?;
        self.shutdown()?;
        Ok(result.present)
    }

    #[cfg(any(unix, windows))]
    fn import(&self, root: &str, path: PathBuf) -> CargoResult<String> {
        // Keep this flat request compatible with the original filesystem-only service.
        let result: ImportResult = call(
            &self.client,
            "artifact.import",
            ArtifactParams {
                root: root.to_owned(),
                path,
            },
        )?;
        self.shutdown()?;
        Ok(result.object)
    }

    fn importer(
        &self,
        importer: &str,
        parameters: Value,
        options: Value,
    ) -> CargoResult<Option<Value>> {
        self.optional_request(
            "artifact.import",
            json!({"importer": importer, "parameters": parameters, "options": options}),
        )
    }

    fn restore(&self, importer: &str, root: &str, path: &Path) -> CargoResult<Option<bool>> {
        if importer == "filesystem" {
            // Filesystem snapshots also work with the original IPC service,
            // which guarantees checkout but has no generic restore method.
            return self.checkout(root, path.to_owned()).map(Some);
        }
        self.optional_request::<CheckoutResult>(
            "artifact.restore",
            json!({"importer": importer, "root": root, "path": path}),
        )
        .map(|result| result.map(|result| result.present))
    }

    fn optional_request<R: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> CargoResult<Option<R>> {
        let result = call(&self.client, method, params);
        // Shutdown must not hide the operation's error or leave a failed session around.
        let shutdown = self.shutdown();
        match result {
            Ok(value) => {
                shutdown?;
                Ok(Some(value))
            }
            Err(error) if unsupported_operation(&error) => {
                shutdown?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(any(unix, windows))]
    fn shutdown(&self) -> CargoResult<()> {
        call::<_, ()>(&self.client, "rpc.shutdown", ())
    }
}

fn unsupported_operation(error: &anyhow::Error) -> bool {
    let Some(jsonrpc::Error::Rpc(error)) = error.downcast_ref::<jsonrpc::Error>() else {
        return false;
    };
    if error.code == -32601 {
        return true;
    }
    error
        .data
        .as_ref()
        .and_then(|data| serde_json::from_str::<Value>(data.get()).ok())
        .and_then(|data| {
            data.get("category")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|category| {
            matches!(
                category.as_str(),
                "unsupported_importer" | "unsupported_option"
            )
        })
}

fn call<P: Serialize, R: DeserializeOwned>(
    client: &jsonrpc::Client,
    method: &str,
    params: P,
) -> CargoResult<R> {
    let _timing = StorageTiming::new("ipc", method);
    let params = jsonrpc::try_arg(params)?;
    client
        .call(method, Some(&params))
        .with_context(|| format!("Casita IPC `{method}` request failed"))
}

struct StorageTiming<'a> {
    operation: &'a str,
    root: &'a str,
    started: Instant,
}

impl<'a> StorageTiming<'a> {
    fn new(operation: &'a str, root: &'a str) -> Self {
        Self {
            operation,
            root,
            started: Instant::now(),
        }
    }
}

impl Drop for StorageTiming<'_> {
    fn drop(&mut self) {
        tracing::debug!(
            operation = self.operation,
            root = self.root,
            elapsed_ms = self.started.elapsed().as_secs_f64() * 1000.0,
            "Casita storage operation"
        );
    }
}

#[cfg(unix)]
type LocalEndpoint = PathBuf;
#[cfg(windows)]
type LocalEndpoint = String;

#[cfg(any(unix, windows))]
struct LocalSocketTransport {
    endpoint: LocalEndpoint,
    connection: Mutex<LocalSocketConnection>,
}

#[cfg(unix)]
type LocalStream = UnixStream;
#[cfg(windows)]
type LocalStream = File;

#[cfg(any(unix, windows))]
struct LocalSocketConnection {
    reader: BufReader<LocalStream>,
    writer: LocalStream,
}

#[cfg(any(unix, windows))]
impl LocalSocketTransport {
    fn connect(endpoint: LocalEndpoint) -> CargoResult<Self> {
        #[cfg(unix)]
        let writer = UnixStream::connect(&endpoint).with_context(|| {
            format!(
                "could not connect to Casita IPC endpoint {}",
                endpoint.display()
            )
        })?;
        #[cfg(windows)]
        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&endpoint)
            .with_context(|| format!("could not connect to Casita IPC endpoint {endpoint}"))?;
        #[cfg(unix)]
        {
            writer.set_read_timeout(Some(Duration::from_secs(60 * 60)))?;
            writer.set_write_timeout(Some(Duration::from_secs(60 * 60)))?;
        }
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Self {
            endpoint,
            connection: Mutex::new(LocalSocketConnection { reader, writer }),
        })
    }

    fn request(&self, request: jsonrpc::Request<'_>) -> Result<jsonrpc::Response, jsonrpc::Error> {
        let mut connection = self.connection.lock().map_err(|_| {
            transport_error(std::io::Error::other(
                "Casita IPC connection lock was poisoned",
            ))
        })?;
        let request = serde_json::to_vec(&request)?;
        if request.len() > IPC_MAX_FRAME_BYTES {
            return Err(transport_error(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Casita IPC request exceeds the negotiated frame limit",
            )));
        }
        connection
            .writer
            .write_all(&request)
            .and_then(|_| connection.writer.write_all(b"\n"))
            .and_then(|_| connection.writer.flush())
            .map_err(transport_error)?;

        let mut response = String::new();
        connection
            .reader
            .read_line(&mut response)
            .map_err(transport_error)?;
        if !response.ends_with('\n') || response.len() - 1 > IPC_MAX_FRAME_BYTES {
            return Err(transport_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Casita IPC returned an invalid or oversized frame",
            )));
        }
        serde_json::from_str(response.trim_end_matches('\n')).map_err(Into::into)
    }
}

#[cfg(any(unix, windows))]
impl jsonrpc::Transport for LocalSocketTransport {
    fn send_request(
        &self,
        request: jsonrpc::Request<'_>,
    ) -> Result<jsonrpc::Response, jsonrpc::Error> {
        self.request(request)
    }

    fn send_batch(
        &self,
        requests: &[jsonrpc::Request<'_>],
    ) -> Result<Vec<jsonrpc::Response>, jsonrpc::Error> {
        requests
            .iter()
            .cloned()
            .map(|request| self.request(request))
            .collect()
    }

    fn fmt_target(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(unix)]
        return write!(formatter, "{}", self.endpoint.display());
        #[cfg(windows)]
        return write!(formatter, "{}", self.endpoint);
    }
}

#[cfg(any(unix, windows))]
fn transport_error(error: impl std::error::Error + Send + Sync + 'static) -> jsonrpc::Error {
    jsonrpc::Error::Transport(Box::new(error))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn metadata_capture_preserves_file_identity_and_symlink_times() {
        let source = tempfile::tempdir().unwrap();
        let file = source.path().join("file");
        std::fs::write(&file, "unchanged bytes").unwrap();
        std::os::unix::fs::symlink("file", source.path().join("link")).unwrap();
        std::fs::hard_link(&file, source.path().join("alias")).unwrap();
        let before = file_identity(&file.metadata().unwrap());
        let staging = tempfile::tempdir().unwrap();
        let data = staging.path().join("data");
        let timestamps = SnapshotTimestamps::capture_inner(source.path(), &data, false).unwrap();
        assert_eq!(before, file_identity(&file.metadata().unwrap()));
        assert_eq!(std::fs::read_dir(&data).unwrap().count(), 0);
        assert!(
            timestamps
                .entries
                .contains_key(&SnapshotTimestamps::path_key(Path::new("link")))
        );
        assert_eq!(timestamps.hardlinks.len(), 1);
    }

    #[test]
    fn snapshot_restores_hardlinks_before_timestamps() {
        use std::os::unix::fs::MetadataExt as _;
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("original"), "bytes").unwrap();
        std::fs::hard_link(source.path().join("original"), source.path().join("alias")).unwrap();
        let staging = tempfile::tempdir().unwrap();
        let data = staging.path().join("data");
        let timestamps = SnapshotTimestamps::capture(source.path(), &data).unwrap();
        // Model a filesystem checkout which creates separate copies.
        std::fs::remove_file(data.join("alias")).unwrap();
        std::fs::write(data.join("alias"), "bytes").unwrap();
        timestamps.restore(&data, &[]).unwrap();
        assert_eq!(
            data.join("original").metadata().unwrap().ino(),
            data.join("alias").metadata().unwrap().ino()
        );
        assert_eq!(
            snapshot_fingerprint(source.path(), &mut BTreeMap::new()).unwrap(),
            snapshot_fingerprint(&data, &mut BTreeMap::new()).unwrap()
        );
    }

    #[test]
    fn snapshot_restores_private_file_permissions() {
        use std::os::unix::fs::PermissionsExt as _;
        let source = tempfile::tempdir().unwrap();
        let path = source.path().join("lock");
        std::fs::write(&path, "bytes").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let staging = tempfile::tempdir().unwrap();
        let data = staging.path().join("data");
        let timestamps = SnapshotTimestamps::capture(source.path(), &data).unwrap();
        std::fs::remove_file(data.join("lock")).unwrap();
        std::fs::write(data.join("lock"), "bytes").unwrap();
        timestamps.restore(&data, &[]).unwrap();
        assert_eq!(
            data.join("lock").metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn snapshot_fingerprint_detects_edits_with_preserved_mtime() {
        let directory = tempfile::tempdir().unwrap();
        let mut files = BTreeMap::new();
        let path = directory.path().join("source");
        std::fs::write(&path, "before").unwrap();
        let before = snapshot_fingerprint(directory.path(), &mut files).unwrap();
        let mtime = filetime::FileTime::from_last_modification_time(&path.metadata().unwrap());
        std::fs::write(&path, "edited").unwrap();
        filetime::set_file_mtime(&path, mtime).unwrap();
        assert_ne!(
            before,
            snapshot_fingerprint(directory.path(), &mut files).unwrap()
        );
    }

    #[test]
    fn snapshot_fingerprint_detects_hardlink_edits_with_preserved_mtime() {
        let directory = tempfile::tempdir().unwrap();
        let mut files = BTreeMap::new();
        let path = directory.path().join("source");
        std::fs::write(&path, "before").unwrap();
        std::fs::hard_link(&path, directory.path().join("alias")).unwrap();
        let before = snapshot_fingerprint(directory.path(), &mut files).unwrap();
        let key = SnapshotTimestamps::path_key(Path::new("source"));
        let prior_digest = files[&key].digest.clone();
        let mtime = filetime::FileTime::from_last_modification_time(&path.metadata().unwrap());
        std::fs::write(&path, "edited").unwrap();
        filetime::set_file_mtime(&path, mtime).unwrap();
        let after = snapshot_fingerprint(directory.path(), &mut files).unwrap();
        let alias_key = SnapshotTimestamps::path_key(Path::new("alias"));
        assert_ne!(before, after);
        assert_ne!(files[&key].digest, prior_digest);
        assert_eq!(files[&key].digest, files[&alias_key].digest);
    }

    #[test]
    fn snapshot_fingerprint_detects_permissions_and_symlink_targets() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().unwrap();
        let mut files = BTreeMap::new();
        let path = directory.path().join("source");
        std::fs::write(&path, "contents").unwrap();
        let before = snapshot_fingerprint(directory.path(), &mut files).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_ne!(
            before,
            snapshot_fingerprint(directory.path(), &mut files).unwrap()
        );
        let link = directory.path().join("link");
        std::os::unix::fs::symlink("source", &link).unwrap();
        let before = snapshot_fingerprint(directory.path(), &mut files).unwrap();
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink("missing", &link).unwrap();
        assert_ne!(
            before,
            snapshot_fingerprint(directory.path(), &mut files).unwrap()
        );
    }

    struct ImportTransport {
        category: Option<&'static str>,
    }

    impl jsonrpc::Transport for ImportTransport {
        fn send_request(
            &self,
            request: jsonrpc::Request<'_>,
        ) -> Result<jsonrpc::Response, jsonrpc::Error> {
            let mut response = json!({"jsonrpc": "2.0", "id": request.id});
            if request.method == "rpc.shutdown" {
                response["result"] = Value::Null;
            } else if let Some(category) = self.category {
                response["error"] =
                    json!({"code": -32602, "message": "fixture", "data": {"category": category}});
            } else {
                assert_eq!(request.method, "artifact.import");
                response["result"] = serde_json::from_str::<Value>(request.params.unwrap().get())?;
            }
            Ok(serde_json::from_value(response)?)
        }
        fn send_batch(
            &self,
            _: &[jsonrpc::Request<'_>],
        ) -> Result<Vec<jsonrpc::Response>, jsonrpc::Error> {
            unreachable!()
        }
        fn fmt_target(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("test")
        }
    }

    #[test]
    fn importer_requests_are_generic_and_fallback_is_explicit() {
        let client = CasitaClient {
            client: jsonrpc::Client::with_transport(ImportTransport { category: None }),
        };
        for importer in [
            "git",
            "tar",
            "blob",
            "copy",
            "nar",
            "filesystem_nar",
            "casitar",
            "future_importer",
        ] {
            let params = json!({"path": "/input", "destinations": ["one", "two"]});
            let options = json!({"future_option": true});
            let result = client
                .importer(importer, params.clone(), options.clone())
                .unwrap()
                .unwrap();
            assert_eq!(
                result,
                json!({"importer": importer, "parameters": params, "options": options})
            );
        }
        for category in [
            "unsupported_importer",
            "unsupported_option",
            "invalid_parameters",
            "execution_failure",
        ] {
            let client = CasitaClient {
                client: jsonrpc::Client::with_transport(ImportTransport {
                    category: Some(category),
                }),
            };
            let result = client.importer("tar", json!({}), json!({}));
            if category.starts_with("unsupported_") {
                assert!(result.unwrap().is_none());
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn native_journal_rejects_roots_from_another_owner() {
        let temp = tempfile::tempdir().unwrap();
        let storage = CasitaArtifactStorage {
            repository: temp.path().to_owned(),
            archives: Filesystem::new(temp.path().join("archives")),
        };
        std::fs::create_dir(temp.path().join(".cargo-native-roots")).unwrap();
        storage
            .write_native_journal("owner", &BTreeSet::from(["unrelated/user/root".to_owned()]))
            .unwrap();
        assert!(
            storage
                .read_native_journal("owner")
                .unwrap_err()
                .to_string()
                .contains("another owner")
        );
    }

    #[test]
    fn only_unsupported_errors_allow_fallback() {
        for (category, expected) in [
            ("unsupported_importer", true),
            ("unsupported_option", true),
            ("invalid_parameters", false),
            ("execution_failure", false),
        ] {
            let error = anyhow::Error::new(jsonrpc::Error::Rpc(jsonrpc::error::RpcError {
                code: -32602,
                message: "fixture".into(),
                data: Some(
                    serde_json::value::to_raw_value(&json!({"category": category})).unwrap(),
                ),
            }))
            .context("IPC failed");
            assert_eq!(unsupported_operation(&error), expected, "{category}");
        }
        let error = anyhow::Error::new(jsonrpc::Error::Rpc(jsonrpc::error::RpcError {
            code: -32602,
            message: "unsupported importer".into(),
            data: None,
        }));
        assert!(!unsupported_operation(&error));
        assert!(!unsupported_operation(&anyhow::anyhow!(
            "transport failure"
        )));
    }

    #[test]
    fn snapshot_paths_reject_escape_and_symlink_ancestors() {
        let temp = tempfile::tempdir().unwrap();
        assert!(safe_snapshot_path(temp.path(), Path::new("../escape")).is_err());
        assert!(safe_snapshot_path(temp.path(), Path::new("/absolute")).is_err());
        std::os::unix::fs::symlink(temp.path(), temp.path().join("link")).unwrap();
        assert!(safe_snapshot_path(temp.path(), Path::new("link/file")).is_err());
    }

    #[test]
    fn reports_early_exit_and_stderr() {
        let error = start_ipc_server(
            Command::new("sh").args(["-c", "echo startup-failed >&2; exit 7"]),
            "test-endpoint",
            || false,
            Duration::from_secs(5),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("exit status: 7"), "{error}");
        assert!(error.contains("startup-failed"), "{error}");
        assert!(error.contains("test-endpoint"), "{error}");
    }

    #[test]
    fn waits_for_delayed_readiness() {
        let started = Instant::now();
        start_ipc_server(
            Command::new("sleep").arg("3"),
            "test-endpoint",
            || started.elapsed() >= Duration::from_millis(2200),
            Duration::from_secs(5),
        )
        .unwrap();
    }

    #[test]
    fn times_out_and_retains_stderr_without_pipe_deadlock() {
        let error = start_ipc_server(
            Command::new("sh").args([
                "-c",
                "i=0; while [ $i -lt 10000 ]; do echo startup-output >&2; i=$((i+1)); done; exec sleep 30",
            ]),
            "test-endpoint",
            || false,
            Duration::from_secs(1),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("did not become ready within 1 seconds"),
            "{error}"
        );
        assert!(error.contains("startup-output"), "{error}");
        assert!(error.len() < 66 * 1024);
    }
}
