use std::{
    collections::BTreeMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::{Arc, RwLock as StdRwLock},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Mutex,
};

use crate::{
    model::ObjectMetadata,
    util::{hex_lower, now_ms},
};

#[derive(Clone)]
pub struct ObjectStore {
    root: PathBuf,
    write_lock: Arc<Mutex<()>>,
    metadata: Arc<StdRwLock<BTreeMap<String, ObjectMetadata>>>,
    metadata_load_error: Arc<StdRwLock<Option<String>>>,
}

impl ObjectStore {
    pub fn new(root: PathBuf) -> Self {
        let (metadata, metadata_load_error) = load_metadata_index(&root);
        Self {
            root,
            write_lock: Arc::new(Mutex::new(())),
            metadata: Arc::new(StdRwLock::new(metadata)),
            metadata_load_error: Arc::new(StdRwLock::new(metadata_load_error)),
        }
    }

    pub async fn put_with_id(
        &self,
        id: String,
        content_type: String,
        body: Bytes,
    ) -> Result<ObjectMetadata> {
        if !ensure_safe_object_id(&id) {
            anyhow::bail!("invalid object id");
        }
        let _guard = self.write_lock.lock().await;
        fs::create_dir_all(self.blob_dir()).await?;
        fs::create_dir_all(self.metadata_dir()).await?;

        let sha256 = hex_lower(&Sha256::digest(&body));
        let metadata = ObjectMetadata {
            id: id.clone(),
            path: format!("objects/{id}"),
            content_type,
            byte_size: body.len() as u64,
            sha256,
            created_at_ms: now_ms(),
        };

        fs::write(self.blob_path(&id), body).await?;
        fs::write(
            self.metadata_path(&id),
            serde_json::to_vec_pretty(&metadata)?,
        )
        .await?;
        self.insert_metadata(metadata.clone())?;
        Ok(metadata)
    }

    pub async fn put_replicated(&self, metadata: ObjectMetadata, body: Bytes) -> Result<bool> {
        if !ensure_safe_object_id(&metadata.id) {
            anyhow::bail!("invalid replicated object id");
        }
        if metadata.path != format!("objects/{}", metadata.id) {
            anyhow::bail!("replicated object path does not match id");
        }
        if metadata.byte_size != body.len() as u64 {
            anyhow::bail!("replicated object byte size mismatch");
        }
        let sha256 = hex_lower(&Sha256::digest(&body));
        if metadata.sha256 != sha256 {
            anyhow::bail!("replicated object sha256 mismatch");
        }

        let _guard = self.write_lock.lock().await;
        fs::create_dir_all(self.blob_dir()).await?;
        fs::create_dir_all(self.metadata_dir()).await?;

        if self.metadata_path(&metadata.id).exists() && self.blob_path(&metadata.id).exists() {
            let existing = self.metadata(&metadata.id).await?;
            if existing.sha256 == metadata.sha256 && existing.byte_size == metadata.byte_size {
                return Ok(false);
            }
            anyhow::bail!("replicated object id already exists with different metadata");
        }

        fs::write(self.blob_path(&metadata.id), body).await?;
        fs::write(
            self.metadata_path(&metadata.id),
            serde_json::to_vec_pretty(&metadata)?,
        )
        .await?;
        self.insert_metadata(metadata.clone())?;
        Ok(true)
    }

    pub async fn metadata(&self, id: &str) -> Result<ObjectMetadata> {
        if let Some(metadata) = self
            .metadata
            .read()
            .ok()
            .and_then(|index| index.get(id).cloned())
        {
            return Ok(metadata);
        }
        let bytes = fs::read(self.metadata_path(id))
            .await
            .with_context(|| format!("read object metadata for {id}"))?;
        let metadata = serde_json::from_slice::<ObjectMetadata>(&bytes)?;
        self.insert_metadata(metadata.clone())?;
        Ok(metadata)
    }

    pub fn metadata_exists(&self, id: &str) -> bool {
        self.metadata
            .read()
            .ok()
            .is_some_and(|index| index.contains_key(id))
            || self.metadata_path(id).exists()
    }

    pub async fn body(&self, id: &str) -> Result<(ObjectMetadata, Bytes)> {
        let metadata = self.metadata(id).await?;
        let body = fs::read(self.blob_path(id))
            .await
            .with_context(|| format!("read object body for {id}"))?;
        Ok((metadata, Bytes::from(body)))
    }

    pub async fn body_range(
        &self,
        id: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<(ObjectMetadata, Bytes)> {
        let metadata = self.metadata(id).await?;
        let length = end_inclusive
            .checked_sub(start)
            .and_then(|value| value.checked_add(1))
            .context("invalid object byte range")?;
        let length: usize = length
            .try_into()
            .context("object byte range is too large to read")?;
        let mut file = fs::File::open(self.blob_path(id))
            .await
            .with_context(|| format!("open object body for {id}"))?;
        file.seek(SeekFrom::Start(start))
            .await
            .with_context(|| format!("seek object body for {id}"))?;
        let mut body = vec![0; length];
        file.read_exact(&mut body)
            .await
            .with_context(|| format!("read object body range for {id}"))?;
        Ok((metadata, Bytes::from(body)))
    }

    pub async fn list_metadata(&self) -> Result<Vec<ObjectMetadata>> {
        if let Some(error) = self
            .metadata_load_error
            .read()
            .ok()
            .and_then(|error| error.clone())
        {
            anyhow::bail!("object metadata index load failed: {error}");
        }
        let mut objects = self
            .metadata
            .read()
            .map_err(|_| anyhow::anyhow!("object metadata index poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(objects)
    }

    pub async fn delete_object(&self, id: &str) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        for path in [self.blob_path(id), self.metadata_path(id)] {
            match fs::remove_file(path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
        self.remove_metadata(id)?;
        Ok(())
    }

    fn insert_metadata(&self, metadata: ObjectMetadata) -> Result<()> {
        self.metadata
            .write()
            .map_err(|_| anyhow::anyhow!("object metadata index poisoned"))?
            .insert(metadata.id.clone(), metadata);
        Ok(())
    }

    fn remove_metadata(&self, id: &str) -> Result<()> {
        self.metadata
            .write()
            .map_err(|_| anyhow::anyhow!("object metadata index poisoned"))?
            .remove(id);
        Ok(())
    }

    fn blob_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    fn metadata_dir(&self) -> PathBuf {
        self.root.join("metadata")
    }

    fn blob_path(&self, id: &str) -> PathBuf {
        self.blob_dir().join(format!("{id}.bin"))
    }

    fn metadata_path(&self, id: &str) -> PathBuf {
        self.metadata_dir().join(format!("{id}.json"))
    }
}

fn load_metadata_index(root: &Path) -> (BTreeMap<String, ObjectMetadata>, Option<String>) {
    let dir = root.join("metadata");
    if !dir.exists() {
        return (BTreeMap::new(), None);
    }

    let mut objects = BTreeMap::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) => return (objects, Some(format!("read {}: {err}", dir.display()))),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                return (
                    objects,
                    Some(format!("read {} entry: {err}", dir.display())),
                );
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) => return (objects, Some(format!("stat {}: {err}", path.display()))),
        };
        if !file_type.is_file() {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => return (objects, Some(format!("read {}: {err}", path.display()))),
        };
        let metadata = match serde_json::from_slice::<ObjectMetadata>(&bytes) {
            Ok(metadata) => metadata,
            Err(err) => return (objects, Some(format!("parse {}: {err}", path.display()))),
        };
        objects.insert(metadata.id.clone(), metadata);
    }
    (objects, None)
}

pub fn ensure_safe_object_id(id: &str) -> bool {
    Path::new(id).components().count() == 1
        && id
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "nextdb-object-store-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ))
    }

    #[tokio::test]
    async fn delete_object_removes_metadata_and_body() {
        let root = test_root("delete");
        let store = ObjectStore::new(root.clone());
        let metadata = store
            .put_with_id(
                "rollback-object".to_string(),
                "text/plain".to_string(),
                Bytes::from_static(b"rollback"),
            )
            .await
            .expect("put object");

        assert!(store.metadata_exists(&metadata.id));
        assert_eq!(
            store.body(&metadata.id).await.expect("body").1,
            Bytes::from_static(b"rollback")
        );

        store
            .delete_object(&metadata.id)
            .await
            .expect("delete object");

        assert!(!store.metadata_exists(&metadata.id));
        assert!(store.body(&metadata.id).await.is_err());

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn metadata_index_rehydrates_and_updates() {
        let root = test_root("index");
        let store = ObjectStore::new(root.clone());
        let first = store
            .put_with_id(
                "indexed-a".to_string(),
                "text/plain".to_string(),
                Bytes::from_static(b"a"),
            )
            .await
            .expect("put first object");
        let second = store
            .put_with_id(
                "indexed-b".to_string(),
                "text/plain".to_string(),
                Bytes::from_static(b"bb"),
            )
            .await
            .expect("put second object");

        let reloaded = ObjectStore::new(root.clone());
        assert!(reloaded.metadata_exists("indexed-a"));
        assert_eq!(
            reloaded
                .metadata("indexed-a")
                .await
                .expect("indexed metadata")
                .sha256,
            first.sha256
        );
        assert_eq!(
            reloaded
                .list_metadata()
                .await
                .expect("list indexed metadata")
                .iter()
                .map(|metadata| metadata.id.as_str())
                .collect::<Vec<_>>(),
            vec![first.id.as_str(), second.id.as_str()]
        );

        reloaded
            .delete_object("indexed-a")
            .await
            .expect("delete indexed object");
        assert!(!reloaded.metadata_exists("indexed-a"));
        assert_eq!(
            reloaded
                .list_metadata()
                .await
                .expect("list after delete")
                .iter()
                .map(|metadata| metadata.id.as_str())
                .collect::<Vec<_>>(),
            vec![second.id.as_str()]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }
}
