use rust_db_core::{DbError, Result, WasmValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Host functions that WASM modules can call to interact with the database
pub struct HostFunctions {
    // Storage for shared state between host and WASM
    pub state: Arc<RwLock<HostState>>,
}

pub struct HostState {
    // Simple in-memory storage for demonstration
    // In production, this would interface with the actual database
    pub data: HashMap<String, WasmValue>,
    pub logs: Vec<String>,
}

impl HostFunctions {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HostState {
                data: HashMap::new(),
                logs: Vec::new(),
            })),
        }
    }

    /// Get a value from the database
    pub async fn db_get(&self, key: &str) -> Result<Option<WasmValue>> {
        let state = self.state.read().await;
        Ok(state.data.get(key).cloned())
    }

    /// Put a value into the database
    pub async fn db_put(&self, key: String, value: WasmValue) -> Result<()> {
        let mut state = self.state.write().await;
        state.data.insert(key, value);
        Ok(())
    }

    /// Query the database with filters
    pub async fn db_query(
        &self,
        _table: &str,
        _filters: &[(&str, WasmValue)],
    ) -> Result<Vec<WasmValue>> {
        // Simplified query - in production would use actual query engine
        let state = self.state.read().await;
        Ok(state.data.values().cloned().collect())
    }

    /// Log a message from WASM
    pub async fn log(&self, level: &str, message: &str) {
        let mut state = self.state.write().await;
        let log_entry = format!("[{}] {}", level, message);
        state.logs.push(log_entry.clone());
        log::info!("{}", log_entry);
    }

    /// Get all logs
    pub async fn get_logs(&self) -> Vec<String> {
        let state = self.state.read().await;
        state.logs.clone()
    }

    /// Clear all logs
    pub async fn clear_logs(&self) {
        let mut state = self.state.write().await;
        state.logs.clear();
    }
}

impl Default for HostFunctions {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions for WASM memory management

/// Read a string from WASM memory
pub fn read_string_from_wasm(memory: &[u8], ptr: usize, len: usize) -> Result<String> {
    if ptr + len > memory.len() {
        return Err(DbError::Wasm("Memory access out of bounds".to_string()));
    }

    String::from_utf8(memory[ptr..ptr + len].to_vec())
        .map_err(|e| DbError::Wasm(format!("Invalid UTF-8: {}", e)))
}

/// Write a string to WASM memory
pub fn write_string_to_wasm(memory: &mut [u8], ptr: usize, data: &str) -> Result<()> {
    let bytes = data.as_bytes();
    if ptr + bytes.len() > memory.len() {
        return Err(DbError::Wasm("Memory access out of bounds".to_string()));
    }

    memory[ptr..ptr + bytes.len()].copy_from_slice(bytes);
    Ok(())
}

/// Read bytes from WASM memory
pub fn read_bytes_from_wasm(memory: &[u8], ptr: usize, len: usize) -> Result<Vec<u8>> {
    if ptr + len > memory.len() {
        return Err(DbError::Wasm("Memory access out of bounds".to_string()));
    }

    Ok(memory[ptr..ptr + len].to_vec())
}

/// Write bytes to WASM memory
pub fn write_bytes_to_wasm(memory: &mut [u8], ptr: usize, data: &[u8]) -> Result<()> {
    if ptr + data.len() > memory.len() {
        return Err(DbError::Wasm("Memory access out of bounds".to_string()));
    }

    memory[ptr..ptr + data.len()].copy_from_slice(data);
    Ok(())
}
