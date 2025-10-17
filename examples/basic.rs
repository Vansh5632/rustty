use rust_db_core::{Database, Value, Operator};
use rust_db_schema::Schema;
use rust_db_storage::LsmStorage;
use rust_db_query::QueryExt;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Schema)]
struct User {
    id: u64,
    name: String,
    #[index]
    email: String,
    age: u32,
}

#[derive(Debug, Serialize, Deserialize, Schema)]
struct Product {
    id: u64,
    name: String,
    price: f64,
    #[index]
    category: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize storage
    let storage = LsmStorage::new(std::path::Path::new("./data"))?;
    
    // Insert users
    let users = vec![
        User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), age: 30 },
        User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), age: 25 },
        User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), age: 35 },
    ];
    
    for user in users {
        let key = format!("users:{}", user.id).into_bytes();
        storage.insert(&key, &user).await?;
    }
    
    // Insert products
    let products = vec![
        Product { id: 1, name: "Laptop".to_string(), price: 999.99, category: "electronics".to_string() },
        Product { id: 2, name: "Book".to_string(), price: 29.99, category: "education".to_string() },
    ];
    
    for product in products {
        let key = format!("products:{}", product.id).into_bytes();
        storage.insert(&key, &product).await?;
    }
    
    // Query users
    println!("All users:");
    let all_users = storage.query::<User>()
        .execute()
        .await?;
    
    for user in all_users {
        println!("  - {} ({}) - {}", user.name, user.age, user.email);
    }
    
    // Query with filters (placeholder - real filtering coming next)
    println!("\nUsers with age > 25:");
    let older_users = storage.query::<User>()
        .filter("age", Operator::Gt, Value::Int(25))
        .execute()
        .await?;
    
    for user in older_users {
        println!("  - {} ({})", user.name, user.age);
    }
    
    // Get single user
    println!("\nGetting user with ID 2:");
    let user_key = b"users:2";
    if let Some(user) = Database::get::<User>(&storage, user_key).await? {
        println!("  Found: {} - {}", user.name, user.email);
    }
    
    Ok(())
}