use rust_db_core::{Database, Value, Operator};
use rust_db_schema::Schema;
use rust_db_storage::{LsmStorage, IndexDescriptor, IndexType};
use rust_db_query::QueryExt;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Schema)]
struct User {
    id: u64,
    name: String,
    #[index]
    email: String,
    age: u32,
    active: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize storage
    let storage = LsmStorage::new(std::path::Path::new("./data"))?;
    
    // Create email index
    storage.create_index(IndexDescriptor {
        name: "idx_user_email".to_string(),
        field: "email".to_string(),
        index_type: IndexType::Hash,
    }).await?;
    
    // Insert users
    let users = vec![
        User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), age: 30, active: true },
        User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), age: 25, active: true },
        User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), age: 35, active: false },
        User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), age: 28, active: true },
    ];
    
    for user in users {
        let key = format!("User:{}", user.id).into_bytes();
        storage.insert(&key, &user).await?;
    }
    
    // Query with real filters
    println!("Active users over 25:");
    let active_users = storage.query::<User>()
        .filter("active", Operator::Eq, Value::Bool(true))
        .filter("age", Operator::Gt, Value::Int(25))
        .execute()
        .await?;
    
    for user in active_users {
        println!("  - {} ({}): {}", user.name, user.age, user.email);
    }
    
    // Query with different operators
    println!("\nUsers with 'example.com' emails:");
    let example_users = storage.query::<User>()
        .filter("email", Operator::Contains, Value::String("example.com".to_string()))
        .execute()
        .await?;
    
    for user in example_users {
        println!("  - {}", user.email);
    }
    
    // Index-based lookup
    println!("\nLooking up user by email (index-based):");
    let specific_user = storage.get_by_index::<User>(
        "idx_user_email", 
        &Value::String("bob@example.com".to_string())
    ).await?;
    
    for user in specific_user {
        println!("  - Found: {} (ID: {})", user.name, user.id);
    }
    
    // Complex query with limits
    println!("\nFirst 2 users:");
    let first_two = storage.query::<User>()
        .limit(2)
        .execute()
        .await?;
    
    for user in first_two {
        println!("  - {} (#{})", user.name, user.id);
    }
    
    Ok(())
}