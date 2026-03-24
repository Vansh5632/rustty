use rust_db_core::{Database, MvccDatabase, TransactionContext};
use rust_db_storage::MvccLsmStorage;
use tempfile::TempDir;

fn setup() -> (TempDir, MvccLsmStorage) {
    let dir = TempDir::new().unwrap();
    let storage = MvccLsmStorage::new(dir.path()).unwrap();
    (dir, storage)
}

#[tokio::test]
async fn test_begin_and_commit_transaction() {
    let (_dir, storage) = setup();

    storage.insert(b"accounts:1", &100u64).await.unwrap();

    let mut tx = TransactionContext::new(&storage).await.unwrap();
    // Read within transaction
    let val: Option<u64> = storage
        .get_for_transaction(b"accounts:1", tx.transaction())
        .await
        .unwrap();
    assert_eq!(val, Some(100u64));

    // Write within transaction
    tx.transaction_mut()
        .put(b"accounts:1".to_vec(), bincode::serialize(&200u64).unwrap());
    tx.commit().await.unwrap();

    // Verify committed value
    let val: Option<u64> = Database::get(&storage, b"accounts:1").await.unwrap();
    assert_eq!(val, Some(200u64));
}

#[tokio::test]
async fn test_transaction_rollback() {
    let (_dir, storage) = setup();

    storage.insert(b"key1", &"original").await.unwrap();

    let mut tx = TransactionContext::new(&storage).await.unwrap();
    tx.transaction_mut()
        .put(b"key1".to_vec(), bincode::serialize(&"modified").unwrap());
    tx.rollback().await.unwrap();

    // Value should remain original
    let val: Option<String> = Database::get(&storage, b"key1").await.unwrap();
    assert_eq!(val, Some("original".to_string()));
}

#[tokio::test]
async fn test_transaction_multiple_writes() {
    let (_dir, storage) = setup();

    let mut tx = TransactionContext::new(&storage).await.unwrap();
    for i in 0..5u64 {
        let key = format!("item:{i}").into_bytes();
        tx.transaction_mut()
            .put(key, bincode::serialize(&i).unwrap());
    }
    tx.commit().await.unwrap();

    // All 5 should be persisted
    for i in 0..5u64 {
        let key = format!("item:{i}").into_bytes();
        let val: Option<u64> = Database::get(&storage, &key).await.unwrap();
        assert_eq!(val, Some(i));
    }
}

#[tokio::test]
async fn test_transaction_delete() {
    let (_dir, storage) = setup();

    storage.insert(b"delete_me", &42u32).await.unwrap();

    let mut tx = TransactionContext::new(&storage).await.unwrap();
    tx.transaction_mut().delete(b"delete_me".to_vec());
    tx.commit().await.unwrap();

    // After commit, the key should be tombstoned (empty)
    let raw = storage.scan(b"delete_me").await.unwrap();
    if let Some((_, v)) = raw.first() {
        assert!(v.is_empty(), "Deleted key should have empty tombstone value");
    }
}

#[tokio::test]
async fn test_mvcc_basic_insert_get() {
    let (_dir, storage) = setup();
    storage.insert(b"foo", &"bar".to_string()).await.unwrap();
    let val: Option<String> = Database::get(&storage, b"foo").await.unwrap();
    assert_eq!(val, Some("bar".to_string()));
}

#[tokio::test]
async fn test_mvcc_scan() {
    let (_dir, storage) = setup();
    storage.insert(b"table:1", &"a".to_string()).await.unwrap();
    storage.insert(b"table:2", &"b".to_string()).await.unwrap();
    storage.insert(b"other:1", &"c".to_string()).await.unwrap();

    let results = Database::scan(&storage, b"table:").await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_concurrent_transactions() {
    let storage = {
        let dir = TempDir::new().unwrap();
        // We need to keep TempDir alive, so use a static-ish path
        let path = dir.path().to_path_buf();
        // Leak the dir so it isn't cleaned up during the test
        std::mem::forget(dir);
        MvccLsmStorage::new(&path).unwrap()
    };

    let storage = std::sync::Arc::new(storage);

    storage.insert(b"counter", &0u64).await.unwrap();

    let s1 = storage.clone();
    let s2 = storage.clone();

    let t1 = tokio::spawn(async move {
        let mut tx = TransactionContext::new(&*s1).await.unwrap();
        let val: Option<u64> = s1
            .get_for_transaction(b"counter", tx.transaction())
            .await
            .unwrap();
        let new_val = val.unwrap_or(0) + 10;
        tx.transaction_mut()
            .put(b"counter".to_vec(), bincode::serialize(&new_val).unwrap());
        tx.commit().await.unwrap();
    });

    let t2 = tokio::spawn(async move {
        // Small delay to ensure ordering
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let mut tx = TransactionContext::new(&*s2).await.unwrap();
        let val: Option<u64> = s2
            .get_for_transaction(b"counter", tx.transaction())
            .await
            .unwrap();
        let new_val = val.unwrap_or(0) + 5;
        tx.transaction_mut()
            .put(b"counter".to_vec(), bincode::serialize(&new_val).unwrap());
        tx.commit().await.unwrap();
    });

    t1.await.unwrap();
    t2.await.unwrap();

    // The final value depends on execution order; both writes should succeed
    let val: Option<u64> = Database::get(&*storage, b"counter").await.unwrap();
    assert!(val.is_some());
    // With sequential execution: first +10, then +5 on top = 15
    // OR first +5, then +10 on top = 15 (in simplified MVCC, last writer wins)
    let v = val.unwrap();
    assert!(v > 0, "Counter should have been incremented, got {v}");
}
