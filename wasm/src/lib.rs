mod host_functions;
mod registry;
mod runtime;
mod sandbox;

pub use host_functions::{HostFunctions, HostState};
pub use registry::{ProcedureRegistry, ProcedureStats, StoredProcedure};
pub use runtime::{ExecutionResult, WasmRuntime};
pub use sandbox::{ResourceLimits, SecurityPolicy};

use rust_db_core::{DbError, Result, WasmExecutionContext, WasmExecutionResult, WasmProcedure};
use std::sync::Arc;

/// Main WASM procedure runtime that orchestrates module execution
#[derive(Clone)]
pub struct WasmProcedureRuntime {
    runtime: Arc<WasmRuntime>,
    registry: Arc<ProcedureRegistry>,
    security_policy: SecurityPolicy,
    host_functions: Arc<HostFunctions>,
}

impl WasmProcedureRuntime {
    pub fn new(security_policy: SecurityPolicy) -> Result<Self> {
        let runtime = WasmRuntime::new()?;
        Ok(Self {
            runtime: Arc::new(runtime),
            registry: Arc::new(ProcedureRegistry::new()),
            security_policy,
            host_functions: Arc::new(HostFunctions::new()),
        })
    }

    /// Register a new WASM procedure
    pub async fn register_procedure(
        &self,
        procedure: WasmProcedure,
        wasm_bytes: Vec<u8>,
    ) -> Result<()> {
        // Validate the module against security policy
        let dummy_context = WasmExecutionContext {
            procedure_name: procedure.name.clone(),
            parameters: Default::default(),
            security_context: rust_db_core::SecurityContext::new(
                None,
                rust_db_core::OperationType::Create,
                rust_db_core::Resource::Procedure(procedure.name.clone()),
            ),
            transaction_id: None,
            timeout_ms: procedure.timeout_ms,
        };

        self.security_policy.validate(&dummy_context, &wasm_bytes)?;

        // Register in the registry
        self.registry.register(procedure, wasm_bytes).await?;

        Ok(())
    }

    /// Execute a registered WASM procedure
    pub async fn execute(
        &self,
        context: WasmExecutionContext,
    ) -> Result<WasmExecutionResult> {
        let start_time = std::time::Instant::now();

        // Get the procedure from registry
        let procedure = self
            .registry
            .get(&context.procedure_name)
            .await?
            .ok_or_else(|| {
                DbError::Query(format!("Procedure not found: {}", context.procedure_name))
            })?;

        // Verify module integrity
        self.verify_module(&procedure.wasm_bytes, &context.procedure_name)
            .await?;

        // Apply security policy
        self.security_policy.validate(&context, &procedure.wasm_bytes)?;

        // Execute in sandbox
        let result = self
            .runtime
            .execute(
                &procedure.wasm_bytes,
                context.clone(),
                self.security_policy.resource_limits(),
            )
            .await?;

        // Record execution statistics
        self.registry
            .record_execution(&context.procedure_name)
            .await?;

        let execution_time = start_time.elapsed();

        Ok(WasmExecutionResult {
            success: result.is_success,
            value: result.value,
            error_message: result.error,
            execution_time_ms: execution_time.as_millis() as u64,
            memory_used_mb: result.memory_used_mb,
        })
    }

    /// List all registered procedures
    pub async fn list_procedures(&self) -> Result<Vec<WasmProcedure>> {
        self.registry.list().await
    }

    /// Drop a procedure
    pub async fn drop_procedure(&self, name: &str) -> Result<()> {
        self.registry.remove_procedure(name).await
    }

    /// Get procedure statistics
    pub async fn get_stats(&self, name: &str) -> Result<ProcedureStats> {
        self.registry.get_stats(name).await
    }

    /// Verify module integrity
    async fn verify_module(&self, wasm_bytes: &[u8], procedure_name: &str) -> Result<()> {
        // Verify hash matches registered procedure
        let is_valid = self.registry.verify_integrity(procedure_name).await?;
        if !is_valid {
            return Err(DbError::Security(format!(
                "Module integrity check failed for procedure: {}",
                procedure_name
            )));
        }
        Ok(())
    }

    /// Get host functions for testing
    pub fn host_functions(&self) -> Arc<HostFunctions> {
        self.host_functions.clone()
    }
}
