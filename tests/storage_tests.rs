use rust_db_core::Database;
use rust_db_storage::LsmStorage;
use tempfile::TempDir;

fn temp_storage() -> (TempDir, LsmStorage) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let storage = LsmStorage::new(dir.path()).expect("failed to create storage");
    (dir, storage)
}

#[tokio::test]
async fn test_put_and_get_raw() {
    let (_dir, storage) = temp_storage();
    storage.put(b"key1", b"value1").await.unwrap();
    let val = storage.get(b"key1").await.unwrap();
    assert_eq!(val, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_get_nonexistent_key() {
    let (_dir, storage) = temp_storage();
    let val = storage.get(b"missing").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_overwrite_key() {
    let (_dir, storage) = temp_storage();
    storage.put(b"key", b"first").await.unwrap();
    storage.put(b"key", b"second").await.unwrap();
    let val = storage.get(b"key").await.unwrap();
    assert_eq!(val, Some(b"second".to_vec()));
}

#[tokio::test]
async fn test_delete_tombstone() {
    let (_dir, storage) = temp_storage();
    storage.put(b"key", b"value").await.unwrap();
    // Delete via tombstone (empty value)
    storage.put(b"key", &[]).await.unwrap();
    let val = storage.get(b"key").await.unwrap();
    assert_eq!(val, Some(vec![])); // Tombstone returns empty
}

#[tokio::test]
async fn test_scan_prefix() {
    let (_dir, storage) = temp_storage();
    storage.put(b"users:1", b"alice").await.unwrap();
    storage.put(b"users:2", b"bob").await.unwrap();
    storage.put(b"products:1", b"laptop").await.unwrap();

    let results = storage.scan(b"users:").await.unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(k, _)| k.starts_with(b"users:")));
}

#[tokio::test]
async fn test_scan_empty_prefix() {
    let (_dir, storage) = temp_storage();
    storage.put(b"a", b"1").await.unwrap();
    storage.put(b"b", b"2").await.unwrap();
    // Empty prefix should return all
    let results = storage.scan(b"").await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_scan_no_match() {
    let (_dir, storage) = temp_storage();
    storage.put(b"users:1", b"alice").await.unwrap();
    let results = storage.scan(b"products:").await.unwrap();
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_insert_and_get_serialized() {
    let (_dir, storage) = temp_storage();
    let value = "hello world".to_string();
    storage.insert(b"key", &value).await.unwrap();
    let result: Option<String> = Database::get(&storage, b"key").await.unwrap();
    assert_eq!(result, Some("hello world".to_string()));
}

#[tokio::test]
async fn test_delete_via_trait() {
    let (_dir, storage) = temp_storage();
    storage.insert(b"key", &42u32).await.unwrap();
    Database::delete(&storage, b"key").await.unwrap();
    // After delete (tombstone), scan should show empty value
    let raw = storage.get(b"key").await.unwrap();
    assert_eq!(raw, Some(vec![]));
}

#[tokio::test]
async fn test_many_inserts() {
    let (_dir, storage) = temp_storage();
    for i in 0..100u64 {
        let key = format!("key:{i}").into_bytes();
        let val = format!("val:{i}").into_bytes();
        storage.put(&key, &val).await.unwrap();
    }
    // Verify a sample
    let val = storage.get(b"key:50").await.unwrap();
    assert_eq!(val, Some(b"val:50".to_vec()));

    let all = storage.scan(b"key:").await.unwrap();
    assert_eq!(all.len(), 100);
}

#[tokio::test]
async fn test_wal_exists_after_write() {
    let dir = TempDir::new().unwrap();
    let storage = LsmStorage::new(dir.path()).unwrap();
    storage.put(b"k", b"v").await.unwrap();
    let wal_path = dir.path().join("wal.bin");
    assert!(wal_path.exists(), "WAL file should exist after write");
}
