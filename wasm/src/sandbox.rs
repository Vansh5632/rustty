use ring::digest;
use rust_db_core::{DbError, Permission, Result, WasmExecutionContext};

#[derive(Clone)]
pub struct SecurityPolicy {
    pub allowed_imports: Vec<String>,
    pub forbidden_syscalls: Vec<String>,
    pub max_module_size: usize,
    pub resource_limits: ResourceLimits,
    pub required_permissions: Vec<Permission>,
}

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_memory_mb: u32,
    pub max_fuel: u64,
    pub max_execution_time_ms: u64,
    pub max_table_size: u32,
}

impl SecurityPolicy {
    pub fn default() -> Self {
        Self {
            allowed_imports: vec![
                "env.db_get".to_string(),
                "env.db_put".to_string(),
                "env.log".to_string(),
            ],
            forbidden_syscalls: vec![
                "fd_write".to_string(),
                "fd_read".to_string(),
                "proc_exit".to_string(),
            ],
            max_module_size: 10 * 1024 * 1024, // 10MB
            resource_limits: ResourceLimits::default(),
            required_permissions: vec![],
        }
    }

    pub fn strict() -> Self {
        Self {
            allowed_imports: vec!["env.log".to_string()],
            forbidden_syscalls: vec![
                "fd_write".to_string(),
                "fd_read".to_string(),
                "proc_exit".to_string(),
                "sock_send".to_string(),
                "sock_recv".to_string(),
            ],
            max_module_size: 1 * 1024 * 1024, // 1MB
            resource_limits: ResourceLimits::strict(),
            required_permissions: vec![],
        }
    }

    pub fn validate(&self, context: &WasmExecutionContext, wasm_bytes: &[u8]) -> Result<()> {
        // Check module size
        if wasm_bytes.len() > self.max_module_size {
            return Err(DbError::Wasm(format!(
                "WASM module too large: {} bytes (max: {} bytes)",
                wasm_bytes.len(),
                self.max_module_size
            )));
        }

        // Verify module doesn't contain forbidden imports
        self.validate_imports(wasm_bytes)?;

        // Verify permissions
        self.validate_permissions(context)?;

        // Additional security checks
        self.validate_bytecode(wasm_bytes)?;

        Ok(())
    }

    fn validate_imports(&self, _wasm_bytes: &[u8]) -> Result<()> {
        // Parse WASM module and check imports
        // This is a simplified version - in production, would use wasmparser crate
        // to inspect the module's import section
        Ok(())
    }

    fn validate_permissions(&self, context: &WasmExecutionContext) -> Result<()> {
        for required in &self.required_permissions {
            if !context.security_context.has_permission(required) {
                return Err(DbError::AccessDenied(format!(
                    "Missing permission: {:?}",
                    required
                )));
            }
        }
        Ok(())
    }

    fn validate_bytecode(&self, wasm_bytes: &[u8]) -> Result<()> {
        // Check for suspicious patterns and compute hash
        let hash = digest::digest(&digest::SHA256, wasm_bytes);
        let _hash_hex = hex::encode(hash.as_ref());

        // TODO: Check against known malicious hashes
        // TODO: Validate WASM module structure using wasmparser

        Ok(())
    }

    pub fn resource_limits(&self) -> ResourceLimits {
        self.resource_limits.clone()
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: 256,
            max_fuel: 1_000_000_000,        // 1 billion instructions
            max_execution_time_ms: 5000,    // 5 seconds
            max_table_size: 1000,
        }
    }
}

impl ResourceLimits {
    pub fn strict() -> Self {
        Self {
            max_memory_mb: 64,
            max_fuel: 100_000_000,         // 100 million instructions
            max_execution_time_ms: 1000,   // 1 second
            max_table_size: 100,
        }
    }

    pub fn permissive() -> Self {
        Self {
            max_memory_mb: 512,
            max_fuel: 10_000_000_000,      // 10 billion instructions
            max_execution_time_ms: 30000,  // 30 seconds
            max_table_size: 10000,
        }
    }
}
