use rust_db_core::{Database, Operator, Value, FieldAccess, Schema};
use rust_db_query::QueryExt;
use rust_db_storage::LsmStorage;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

// A test schema that implements Schema + FieldAccess manually
// (We can't use the derive macro in integration tests easily, so do it by hand)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestUser {
    id: u64,
    name: String,
    age: u32,
    active: bool,
}

impl Schema for TestUser {
    fn validate(&self) -> rust_db_core::Result<()> {
        Ok(())
    }
    fn table_name() -> &'static str {
        "TestUser"
    }
    fn indexes(&self) -> std::collections::HashMap<String, Vec<u8>> {
        std::collections::HashMap::new()
    }
}

impl FieldAccess for TestUser {
    fn get_field(&self, field_name: &str) -> Option<Value> {
        match field_name {
            "id" => Some(Value::Int(self.id as i64)),
            "name" => Some(Value::String(self.name.clone())),
            "age" => Some(Value::Int(self.age as i64)),
            "active" => Some(Value::Bool(self.active)),
            _ => None,
        }
    }
}

fn setup() -> (TempDir, LsmStorage) {
    let dir = TempDir::new().unwrap();
    let storage = LsmStorage::new(dir.path()).unwrap();
    (dir, storage)
}

async fn seed_users(storage: &LsmStorage) {
    let users = vec![
        TestUser { id: 1, name: "Alice".to_string(), age: 30, active: true },
        TestUser { id: 2, name: "Bob".to_string(), age: 25, active: false },
        TestUser { id: 3, name: "Charlie".to_string(), age: 35, active: true },
        TestUser { id: 4, name: "Diana".to_string(), age: 28, active: true },
    ];
    for user in users {
        let key = format!("TestUser:{}", user.id).into_bytes();
        storage.insert(&key, &user).await.unwrap();
    }
}

#[tokio::test]
async fn test_query_all() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage.query::<TestUser>().execute().await.unwrap();
    assert_eq!(results.len(), 4);
}

#[tokio::test]
async fn test_query_filter_gt() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("age", Operator::Gt, Value::Int(28))
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 2); // Alice(30), Charlie(35)
    assert!(results.iter().all(|u| u.age > 28));
}

#[tokio::test]
async fn test_query_filter_eq() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("name", Operator::Eq, Value::String("Bob".to_string()))
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Bob");
}

#[tokio::test]
async fn test_query_filter_bool() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("active", Operator::Eq, Value::Bool(false))
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Bob");
}

#[tokio::test]
async fn test_query_filter_contains() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("name", Operator::Contains, Value::String("li".to_string()))
        .execute()
        .await
        .unwrap();

    // Alice and Charlie both contain "li"
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_query_limit() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .limit(2)
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_query_combined_filters() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("age", Operator::Gte, Value::Int(28))
        .filter("active", Operator::Eq, Value::Bool(true))
        .execute()
        .await
        .unwrap();

    // Alice(30, true), Charlie(35, true), Diana(28, true)
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|u| u.age >= 28 && u.active));
}

#[tokio::test]
async fn test_query_no_results() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("age", Operator::Gt, Value::Int(100))
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_query_startswith() {
    let (_dir, storage) = setup();
    seed_users(&storage).await;

    let results = storage
        .query::<TestUser>()
        .filter("name", Operator::StartsWith, Value::String("Ch".to_string()))
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Charlie");
}

#[tokio::test]
async fn test_query_empty_table() {
    let (_dir, storage) = setup();
    // No data seeded
    let results = storage.query::<TestUser>().execute().await.unwrap();
    assert_eq!(results.len(), 0);
}
