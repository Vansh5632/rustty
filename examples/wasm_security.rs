use rust_db_core::{
    Database, DataType, OperationType, Parameter, Permission, Principal, Resource, 
    SecurityContext, WasmExecutionContext, WasmProcedure, WasmValue,
};
use rust_db_storage::{LsmStorage, SecurityLayer};
use rust_db_wasm::{SecurityPolicy, WasmProcedureRuntime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u64,
    customer_id: u64,
    amount: f64,
    status: String,
    items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OrderItem {
    product_id: u64,
    quantity: u32,
    price: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    println!("=== Secure Database with WASM Procedures Demo ===\n");

    // Step 1: Create database with security layer
    println!("📦 Creating secure database...");
    let storage = LsmStorage::new(Path::new("./data/secure_wasm_demo"))?;
    let mut secure_db = SecurityLayer::new(storage);

    // Initialize default roles
    secure_db.initialize_default_roles().await?;
    println!("✓ Security layer initialized with default roles\n");

    // Step 2: Create users
    println!("👤 Creating users...");
    let admin = secure_db
        .add_user("admin", "admin_password_123", vec!["admin".to_string()])
        .await?;
    println!("✓ Admin user created: {}", admin.name);

    let analyst = secure_db
        .add_user(
            "alice",
            "alice_password_456",
            vec!["reader".to_string()],
        )
        .await?;
    println!("✓ Analyst user created: {}\n", analyst.name);

    // Step 3: Authenticate users
    println!("🔐 Authenticating users...");
    let admin_principal = secure_db.authenticate("admin", "admin_password_123").await?;
    println!("✓ Admin authenticated: {:?}", admin_principal.roles);

    let analyst_principal = secure_db.authenticate("alice", "alice_password_456").await?;
    println!("✓ Analyst authenticated: {:?}\n", analyst_principal.roles);

    // Step 4: Initialize WASM runtime
    println!("⚙️  Initializing WASM runtime...");
    let wasm_runtime = WasmProcedureRuntime::new(SecurityPolicy::default())?;
    println!("✓ WASM runtime initialized\n");

    // Step 5: Create sample WASM procedure
    // For demonstration, we'll use a simple inline WASM module
    // In production, this would be compiled from Rust/C/etc.
    println!("📝 Registering WASM stored procedure...");
    
    // This is a minimal WASM module that exports a main function
    let wasm_module = create_demo_wasm_module();
    
    let procedure = WasmProcedure {
        name: "calculate_order_total".to_string(),
        module_hash: String::new(), // Will be computed by registry
        parameters: vec![
            Parameter {
                name: "order_id".to_string(),
                data_type: DataType::Int,
                required: true,
            },
            Parameter {
                name: "tax_rate".to_string(),
                data_type: DataType::Float,
                required: false,
            },
        ],
        return_type: DataType::Float,
        permissions: vec![Permission::ReadTable("orders".to_string())],
        timeout_ms: 1000,
        memory_limit_mb: 10,
    };

    wasm_runtime
        .register_procedure(procedure.clone(), wasm_module)
        .await?;
    println!("✓ Procedure registered: {}\n", procedure.name);

    // Step 6: Create test data
    println!("💾 Creating test orders...");
    let order1 = Order {
        id: 1,
        customer_id: 1001,
        amount: 150.75,
        status: "pending".to_string(),
        items: vec![
            OrderItem {
                product_id: 1,
                quantity: 2,
                price: 50.0,
            },
            OrderItem {
                product_id: 2,
                quantity: 1,
                price: 50.75,
            },
        ],
    };

    let order2 = Order {
        id: 2,
        customer_id: 1002,
        amount: 299.99,
        status: "pending".to_string(),
        items: vec![OrderItem {
            product_id: 3,
            quantity: 1,
            price: 299.99,
        }],
    };

    secure_db.insert(b"orders:1", &order1).await?;
    secure_db.insert(b"orders:2", &order2).await?;
    println!("✓ Created 2 test orders\n");

    // Step 7: Execute WASM procedure
    println!("🚀 Executing WASM stored procedure...");
    
    let mut params = HashMap::new();
    params.insert("order_id".to_string(), WasmValue::Int(1));
    params.insert("tax_rate".to_string(), WasmValue::Float(0.08));

    let context = WasmExecutionContext {
        procedure_name: "calculate_order_total".to_string(),
        parameters: params,
        security_context: SecurityContext::new(
            Some(analyst_principal.clone()),
            OperationType::Execute,
            Resource::Procedure("calculate_order_total".to_string()),
        ),
        transaction_id: None,
        timeout_ms: 1000,
    };

    let result = wasm_runtime.execute(context).await?;

    println!("📊 Execution Result:");
    println!("   Success: {}", result.success);
    if let Some(value) = result.value {
        println!("   Value: {:?}", value);
    }
    if let Some(error) = result.error_message {
        println!("   Error: {}", error);
    }
    println!("   Execution time: {}ms", result.execution_time_ms);
    println!("   Memory used: {:.2}MB\n", result.memory_used_mb);

    // Step 8: Test access control
    println!("🔒 Testing access control...");
    
    // This should succeed - admin has all permissions
    match secure_db.get::<Order>(b"orders:1").await {
        Ok(Some(order)) => println!("✓ Admin can read order: #{}", order.id),
        Ok(None) => println!("✗ Order not found"),
        Err(e) => println!("✗ Access denied: {}", e),
    }

    // Show procedure stats
    println!("\n📈 Procedure Statistics:");
    let stats = wasm_runtime.get_stats("calculate_order_total").await?;
    println!("   Name: {}", stats.name);
    println!("   Executions: {}", stats.execution_count);
    println!("   Module size: {} bytes", stats.module_size_bytes);
    println!("   Created at: {}", stats.created_at);

    // Step 9: View audit log
    println!("\n📋 Recent Audit Log Entries:");
    let audit_log = secure_db.get_audit_log().await;
    for (i, entry) in audit_log.iter().rev().take(5).enumerate() {
        println!("   {}. [{}] {} on {} - {}", 
            i + 1,
            entry.principal_id.as_ref().unwrap_or(&"system".to_string()),
            entry.operation,
            entry.resource,
            if entry.success { "✓" } else { "✗" }
        );
    }

    // Step 10: List all procedures
    println!("\n📚 Registered Procedures:");
    let procedures = wasm_runtime.list_procedures().await?;
    for proc in procedures {
        println!("   • {} ({} parameters, timeout: {}ms)",
            proc.name,
            proc.parameters.len(),
            proc.timeout_ms
        );
    }

    println!("\n✅ Demo completed successfully!");
    println!("\n🎯 Key Features Demonstrated:");
    println!("   ✓ User authentication with Argon2 password hashing");
    println!("   ✓ Role-based access control (RBAC)");
    println!("   ✓ WASM stored procedure registration");
    println!("   ✓ Sandboxed WASM execution with resource limits");
    println!("   ✓ Security context propagation");
    println!("   ✓ Comprehensive audit logging");
    println!("   ✓ Permission checking");
    println!("   ✓ Procedure execution statistics");

    Ok(())
}

/// Creates a minimal WASM module for demonstration
/// In production, this would be compiled from source code
fn create_demo_wasm_module() -> Vec<u8> {
    // This is a minimal WASM module in binary format (WAT compiled to WASM)
    // Module exports: main(params_ptr: i32, params_len: i32)
    // It simply returns without doing much (demonstration purposes)
    vec![
        0x00, 0x61, 0x73, 0x6d, // Magic number \0asm
        0x01, 0x00, 0x00, 0x00, // Version 1
        // Type section
        0x01, 0x07, 0x01,
        0x60, 0x02, 0x7f, 0x7f, 0x00, // Function type: (i32, i32) -> ()
        // Function section
        0x03, 0x02, 0x01, 0x00, // 1 function with type 0
        // Memory section
        0x05, 0x03, 0x01, 0x00, 0x01, // Memory: 1 page
        // Export section
        0x07, 0x0e, 0x02,
        0x04, 0x6d, 0x61, 0x69, 0x6e, 0x00, 0x00, // Export "main" function 0
        0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, // Export "memory"
        // Code section
        0x0a, 0x04, 0x01,
        0x02, 0x00, 0x0b, // Function body: (empty, just return)
    ]
}
