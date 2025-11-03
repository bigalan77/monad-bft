// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use futures::stream;

use crate::prelude::*;

// Number of concurrent uploads
const UPLOAD_CONCURRENCY: usize = 10;

pub async fn generic_dir_archiver(
    store: KVStoreErased,
    folder_path: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
    min_age: Option<Duration>,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_frequency);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Keys we have confirmed exist in S3 across ticks.
    let mut known_in_s3: HashSet<String> = HashSet::new();

    loop {
        interval.tick().await;
        info!("Scanning for files to upload...");

        let result = archive_dir(
            store.clone(),
            &mut known_in_s3,
            folder_path.clone(),
            &metrics,
            min_age,
        )
        .await;

        match result {
            Ok(()) => info!(?folder_path, "Finished scanning for files to upload"),
            Err(e) => error!(?folder_path, ?e, "Failed to archive files in directory"),
        }
    }
}

async fn archive_dir(
    store: KVStoreErased,
    known_in_s3: &mut HashSet<String>,
    folder_path: PathBuf,
    metrics: &Metrics,
    min_age: Option<Duration>,
) -> Result<()> {
    // Derive S3 key prefix from the last path component (basename)
    let dir_os_name = folder_path
        .file_name()
        .ok_or_else(|| eyre!("Folder path must have a basename (last component)"))?;
    let dir_name = dir_os_name
        .to_str()
        .ok_or_else(|| eyre!("Folder name must be valid UTF-8"))?;
    if dir_name == "." || dir_name.is_empty() {
        bail!("Invalid directory name for key prefix: {dir_name}");
    }

    // Build local map of key -> file path (non-recursive)
    let mut local: HashMap<String, PathBuf> = HashMap::new();
    let mut rd = match tokio::fs::read_dir(&folder_path).await {
        Ok(x) => x,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                // Directory missing: treat as empty
                return Ok(());
            }
            return Err(e).wrap_err("Failed to open directory");
        }
    };

    while let Some(entry) = rd.next_entry().await? {
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }

        // Freshness filter
        if let Some(min_age) = min_age {
            let now = std::time::SystemTime::now();
            let too_new = match meta
                .modified()
                .wrap_err("Failed to get modified time")
                .and_then(|m| {
                    now.duration_since(m)
                        .wrap_err("Failed to get duration since modified time")
                }) {
                Ok(age) => age < min_age,
                Err(_) => false,
            };
            if too_new {
                debug!(path=?entry.path(), "Skipping fresh file (< min_age)");
                continue;
            }
        }

        let fname = entry.file_name();
        let fname_str = fname.to_string_lossy();
        let key = format!("{dir_name}/{fname_str}");
        local.insert(key, entry.path());
        metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_DISCOVERED);
    }

    if local.is_empty() {
        debug!(?folder_path, "No local files found this tick");
        return Ok(());
    }

    // GC: drop known keys not present locally
    known_in_s3.retain(|k| local.contains_key(k));
    // Remove keys that are already known to be in S3
    local.retain(|k, _| !known_in_s3.contains(k));

    // Process concurrently
    stream::iter(local.into_iter())
        .map(|(key, path)| {
            let store = store.clone();
            let metrics = metrics.clone();
            async move {
                match process_single_file(store, &key, &path, &metrics).await {
                    Ok(x) => x,
                    Err(e) => {
                        error!(?e, ?key, ?path, "Failed to process file for archive");
                        metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_FAILED_TO_PROCESS);
                        None
                    }
                }
            }
        })
        .buffer_unordered(UPLOAD_CONCURRENCY)
        .for_each(|x| {
            if let Some(key) = x {
                known_in_s3.insert(key);
            }
            futures::future::ready(())
        })
        .await;

    Ok(())
}

async fn process_single_file(
    store: KVStoreErased,
    key: &str,
    path: &PathBuf,
    metrics: &Metrics,
) -> Result<Option<String>> {
    if s3_exists_key(&store, key).await? {
        metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_ALREADY_IN_S3);
        return Ok(Some(key.to_string()));
    }

    let bytes = tokio::fs::read(&path)
        .await
        .wrap_err("Failed to read local file")?;
    store
        .put(&key, bytes)
        .await
        .wrap_err("Failed to upload file to archive store")?;
    metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_UPLOADED);
    info!(key, ?path, "Uploaded file to archive store");
    // Do NOT mark as known here; wait for next tick's exists check
    Ok(None)
}

async fn s3_exists_key(store: &impl KVStore, key: &str) -> Result<bool> {
    let objs = store.scan_prefix(key).await?;
    Ok(objs.iter().any(|k| k == key))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::fs;

    use super::*;
    use crate::kvstore::memory::MemoryStorage;

    #[tokio::test]
    async fn test_generic_archive_uploads_new_files() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let base = tempdir().unwrap();
        let dir_path = base.path().join("my-dir");
        fs::create_dir_all(&dir_path).await.unwrap();

        let test_content = b"hello";
        fs::write(dir_path.join("two.json"), test_content)
            .await
            .unwrap();
        fs::write(dir_path.join("happy.rs"), test_content)
            .await
            .unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        let key1 = "my-dir/two.json";
        let key2 = "my-dir/happy.rs";

        assert_eq!(
            store.get(key1).await.unwrap().unwrap().to_vec().as_slice(),
            test_content.as_slice()
        );
        assert_eq!(
            store.get(key2).await.unwrap().unwrap().to_vec().as_slice(),
            test_content.as_slice()
        );

        // On first upload we should NOT add to known set yet
        assert!(known_in_s3.is_empty());
    }

    #[tokio::test]
    async fn test_generic_archive_discovers_existing_files() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let base = tempdir().unwrap();
        let dir_path = base.path().join("some-data");
        fs::create_dir_all(&dir_path).await.unwrap();

        // Pre-upload a file
        let key = "some-data/item.bin";
        store.put(key, b"remote".to_vec()).await.unwrap();

        // Create local file with same name
        fs::write(dir_path.join("item.bin"), b"local")
            .await
            .unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        assert!(known_in_s3.contains(key));
        // Ensure content not overwritten
        assert_eq!(
            store.get(key).await.unwrap().unwrap().to_vec().as_slice(),
            b"remote".as_slice()
        );
    }

    #[tokio::test]
    async fn test_generic_archive_gc_removes_deleted() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::from(["foo/bar".to_string(), "foo/baz".to_string()]);

        let base = tempdir().unwrap();
        let dir_path = base.path().join("foo");
        fs::create_dir_all(&dir_path).await.unwrap();
        fs::write(dir_path.join("baz"), b"x").await.unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        assert!(!known_in_s3.contains("foo/bar"));
        assert!(known_in_s3.contains("foo/baz"));
    }

    #[tokio::test]
    async fn test_generic_archive_errors_on_bad_dir_name() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        // Root path has no basename
        let folder_path = PathBuf::from("/");
        let err = archive_dir(store, &mut known_in_s3, folder_path, &Metrics::none(), None)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("basename"));
    }
}
