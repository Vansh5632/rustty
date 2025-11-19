use rust_db_core::{Database, MvccDatabase, TransactionContext, Value, FilterOperator};
use rust_db_schema::Schema;
use rust_db_storage::MvccLsmStorage;
use rust_db_query::{QueryExt, TransactionalQueryExt};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
struct Account {
    id: u64,
    name: String,
    balance: f64,
    #[index]
    email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
struct Transfer {
    from_account: u64,
    to_account: u64,
    amount: f64,
    status: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize MVCC storage
    let storage = MvccLsmStorage::new(std::path::Path::new("./data"))?;
    
    // Create some test accounts
    let accounts = vec![
        Account { id: 1, name: "Alice".to_string(), balance: 1000.0, email: "alice@example.com".to_string() },
        Account { id: 2, name: "Bob".to_string(), balance: 500.0, email: "bob@example.com".to_string() },
        Account { id: 3, name: "Charlie".to_string(), balance: 750.0, email: "charlie@example.com".to_string() },
    ];
    
    for account in accounts {
        let key = format!("accounts:{}", account.id).into_bytes();
        storage.insert(&key, &account).await?;
    }
    
    println!("=== Initial State ===");
    print_accounts(&storage).await?;
    
    // Example 1: Simple transaction
    println!("\n=== Example 1: Simple Transaction ===");
    let mut tx_ctx = TransactionContext::new(&storage).await?;
    
    // Update Alice's balance within transaction
    let alice_key = b"accounts:1";
    if let Some(mut alice) = storage.get_for_transaction::<Account>(alice_key, tx_ctx.transaction()).await? {
        alice.balance += 100.0;
        tx_ctx.transaction_mut().put(alice_key.to_vec(), bincode::serialize(&alice)?);
        println!("Updated Alice's balance to: {}", alice.balance);
    }
    
    // Query within transaction (sees the updated balance)
    let accounts_in_tx = storage.query_within_transaction::<Account>(tx_ctx.transaction())
        .execute()
        .await?;
    
    println!("Accounts visible in transaction:");
    for account in accounts_in_tx {
        println!("  - {}: ${}", account.name, account.balance);
    }
    
    // Commit the transaction
    tx_ctx.commit().await?;
    println!("Transaction committed!");
    
    // Example 2: Transfer between accounts (ACID)
    println!("\n=== Example 2: Money Transfer ===");
    transfer_money(&storage, 1, 2, 200.0).await?;
    
    // Example 3: Failed transaction (rollback)
    println!("\n=== Example 3: Failed Transfer (Insufficient Funds) ===");
    let result = transfer_money(&storage, 3, 1, 2000.0).await;
    match result {
        Ok(_) => println!("Transfer succeeded"),
        Err(e) => println!("Transfer failed: {}", e),
    }
    
    // Example 4: Concurrent transactions
    println!("\n=== Example 4: Concurrent Transactions ===");
    let storage_arc = std::sync::Arc::new(storage);
    
    let storage1 = storage_arc.clone();
    let storage2 = storage_arc.clone();
    
    let task1 = tokio::spawn(async move {
        let mut tx_ctx = TransactionContext::new(&*storage1).await.unwrap();
        println!("Task 1: Started transaction");
        
        // Simulate some work
        sleep(Duration::from_millis(100)).await;
        
        // Update account
        let key = b"accounts:1";
        if let Some(mut account) = storage1.get_for_transaction::<Account>(key, tx_ctx.transaction()).await.unwrap() {
            account.balance -= 50.0;
            tx_ctx.transaction_mut().put(key.to_vec(), bincode::serialize(&account).unwrap());
            println!("Task 1: Deducted $50 from account 1");
        }
        
        tx_ctx.commit().await.unwrap();
        println!("Task 1: Transaction committed");
    });
    
    let task2 = tokio::spawn(async move {
        // Start slightly later to simulate concurrency
        sleep(Duration::from_millis(50)).await;
        
        let tx_ctx = TransactionContext::new(&*storage2).await.unwrap();
        println!("Task 2: Started transaction");
        
        // This transaction sees a consistent snapshot
        let accounts = storage2.query_within_transaction::<Account>(tx_ctx.transaction())
            .execute()
            .await
            .unwrap();
            
        println!("Task 2: Can see {} accounts", accounts.len());
        
        tx_ctx.commit().await.unwrap();
        println!("Task 2: Transaction committed");
    });
    
    let _ = tokio::join!(task1, task2);
    
    println!("\n=== Final State ===");
    print_accounts(&storage_arc).await?;
    
    Ok(())
}

async fn transfer_money(
    storage: &impl MvccDatabase,
    from_id: u64,
    to_id: u64,
    amount: f64,
) -> anyhow::Result<()> {
    let mut tx_ctx = TransactionContext::new(storage).await?;
    
    let from_key = format!("accounts:{}", from_id).into_bytes();
    let to_key = format!("accounts:{}", to_id).into_bytes();
    
    // Get both accounts within transaction
    let from_account = storage.get_for_transaction::<Account>(&from_key, tx_ctx.transaction()).await?
        .ok_or_else(|| anyhow::anyhow!("From account not found"))?;
        
    let to_account = storage.get_for_transaction::<Account>(&to_key, tx_ctx.transaction()).await?
        .ok_or_else(|| anyhow::anyhow!("To account not found"))?;
    
    // Check sufficient funds
    if from_account.balance < amount {
        tx_ctx.rollback().await?;
        return Err(anyhow::anyhow!("Insufficient funds in account {}", from_id));
    }
    
    // Update balances
    let mut updated_from = from_account.clone();
    updated_from.balance -= amount;
    
    let mut updated_to = to_account.clone();
    updated_to.balance += amount;
    
    // Apply updates
    tx_ctx.transaction_mut().put(from_key, bincode::serialize(&updated_from)?);
    tx_ctx.transaction_mut().put(to_key, bincode::serialize(&updated_to)?);
    
    // Create transfer record
    let transfer = Transfer {
        from_account: from_id,
        to_account: to_id,
        amount,
        status: "completed".to_string(),
    };
    let transfer_key = format!("transfers:{}", rust_db_core::VersionTimestamp::now().as_u64()).into_bytes();
    tx_ctx.transaction_mut().put(transfer_key, bincode::serialize(&transfer)?);
    
    println!("Transfer: ${} from {} to {}", amount, from_account.name, to_account.name);
    
    // Commit the transaction
    tx_ctx.commit().await?;
    
    Ok(())
}

async fn print_accounts(storage: &impl Database) -> anyhow::Result<()> {
    let accounts = storage.query::<Account>()
        .execute()
        .await?;
    
    for account in accounts {
        println!("  - {}: ${} ({})", account.name, account.balance, account.email);
    }
    
    Ok(())
}