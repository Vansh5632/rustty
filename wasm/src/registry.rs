use ring::digest;
use rust_db_core::{DbError, Result, WasmProcedure};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Registry for storing and managing WASM procedures
pub struct ProcedureRegistry {
    procedures: Arc<RwLock<HashMap<String, StoredProcedure>>>,
}

/// A compiled and stored WASM procedure
#[derive(Clone)]
pub struct StoredProcedure {
    pub metadata: WasmProcedure,
    pub wasm_bytes: Vec<u8>,
    pub compiled_hash: String,
    pub created_at: u64,
    pub last_executed: Option<u64>,
    pub execution_count: u64,
}

impl ProcedureRegistry {
    pub fn new() -> Self {
        Self {
            procedures: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new WASM procedure
    pub async fn register(
        &self,
        procedure: WasmProcedure,
        wasm_bytes: Vec<u8>,
    ) -> Result<()> {
        // Compute hash of the WASM module
        let hash = Self::compute_hash(&wasm_bytes);

        // Verify hash matches if provided
        if !procedure.module_hash.is_empty() && procedure.module_hash != hash {
            return Err(DbError::Security(format!(
                "Module hash mismatch: expected {}, got {}",
                procedure.module_hash, hash
            )));
        }

        let stored = StoredProcedure {
            metadata: procedure.clone(),
            wasm_bytes,
            compiled_hash: hash,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_executed: None,
            execution_count: 0,
        };

        let mut registry = self.procedures.write().await;
        registry.insert(procedure.name.clone(), stored);

        log::info!("Registered WASM procedure: {}", procedure.name);
        Ok(())
    }

    /// Get a procedure by name
    pub async fn get(&self, name: &str) -> Result<Option<StoredProcedure>> {
        let registry = self.procedures.read().await;
        Ok(registry.get(name).cloned())
    }

    /// List all registered procedures
    pub async fn list(&self) -> Result<Vec<WasmProcedure>> {
        let registry = self.procedures.read().await;
        Ok(registry.values().map(|p| p.metadata.clone()).collect())
    }

    /// Drop a procedure
    pub async fn remove_procedure(&self, name: &str) -> Result<()> {
        let mut registry = self.procedures.write().await;
        registry
            .remove(name)
            .ok_or_else(|| DbError::Query(format!("Procedure not found: {}", name)))?;

        log::info!("Dropped WASM procedure: {}", name);
        Ok(())
    }

    /// Update execution statistics
    pub async fn record_execution(&self, name: &str) -> Result<()> {
        let mut registry = self.procedures.write().await;
        if let Some(procedure) = registry.get_mut(name) {
            procedure.execution_count += 1;
            procedure.last_executed = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }
        Ok(())
    }

    /// Verify a procedure's hash matches its stored bytes
    pub async fn verify_integrity(&self, name: &str) -> Result<bool> {
        let registry = self.procedures.read().await;
        if let Some(procedure) = registry.get(name) {
            let current_hash = Self::compute_hash(&procedure.wasm_bytes);
            Ok(current_hash == procedure.compiled_hash)
        } else {
            Err(DbError::Query(format!("Procedure not found: {}", name)))
        }
    }

    /// Compute SHA-256 hash of WASM module
    fn compute_hash(wasm_bytes: &[u8]) -> String {
        let hash = digest::digest(&digest::SHA256, wasm_bytes);
        hex::encode(hash.as_ref())
    }

    /// Get statistics for a procedure
    pub async fn get_stats(&self, name: &str) -> Result<ProcedureStats> {
        let registry = self.procedures.read().await;
        if let Some(procedure) = registry.get(name) {
            Ok(ProcedureStats {
                name: name.to_string(),
                execution_count: procedure.execution_count,
                last_executed: procedure.last_executed,
                created_at: procedure.created_at,
                module_size_bytes: procedure.wasm_bytes.len(),
            })
        } else {
            Err(DbError::Query(format!("Procedure not found: {}", name)))
        }
    }
}

impl Default for ProcedureRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ProcedureStats {
    pub name: String,
    pub execution_count: u64,
    pub last_executed: Option<u64>,
    pub created_at: u64,
    pub module_size_bytes: usize,
}
