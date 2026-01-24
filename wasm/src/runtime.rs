use crate::sandbox::{ResourceLimits, SecurityPolicy};
use ring::digest;
use rust_db_core::{DbError, Result, WasmExecutionContext, WasmValue};
use std::collections::HashMap;
use std::time::Duration;
use wasmtime::*;

pub struct WasmRuntime {
    engine: Engine,
}

impl WasmRuntime {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true); // Enable fuel for computation limits
        config.max_wasm_stack(256 * 1024); // 256KB stack
        config.wasm_multi_memory(false);
        config.wasm_threads(false);

        let engine = Engine::new(&config)
            .map_err(|e| DbError::Wasm(format!("Failed to create WASM engine: {}", e)))?;

        Ok(Self { engine })
    }

    pub async fn execute(
        &self,
        wasm_bytes: &[u8],
        context: WasmExecutionContext,
        resource_limits: ResourceLimits,
    ) -> Result<ExecutionResult> {
        let _start_time = std::time::Instant::now();

        // Create store with resource limiter
        let timeout_ms = context.timeout_ms;
        let mut store = Store::new(&self.engine, StoreData::new(context));

        // Configure resource limits
        store.limiter(|data| data);

        // Allocate fuel for computation
        let fuel_amount = resource_limits.max_fuel;
        store
            .add_fuel(fuel_amount)
            .map_err(|e| DbError::Wasm(format!("Failed to add fuel: {}", e)))?;

        // Compile module
        let module = Module::from_binary(&self.engine, wasm_bytes)
            .map_err(|e| DbError::Wasm(format!("Failed to compile WASM: {}", e)))?;

        // Create linker
        let mut linker = Linker::new(&self.engine);

        // Register host functions
        self.register_host_functions(&mut linker)?;

        // Instantiate module
        let instance = linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|e| DbError::Wasm(format!("Failed to instantiate WASM: {}", e)))?;

        // Get the main function
        let main_func = instance
            .get_func(&mut store, "main")
            .ok_or_else(|| DbError::Wasm("No main function exported".to_string()))?;

        // Get memory for parameter passing
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| DbError::Wasm("No memory exported".to_string()))?;

        // Encode parameters
        let params_encoded = self.encode_parameters(&store.data().context)?;

        // Allocate memory for parameters
        let params_ptr = self.allocate_in_wasm(&mut store, &memory, params_encoded.len())?;

        // Write parameters to memory
        memory
            .write(&mut store, params_ptr, &params_encoded)
            .map_err(|e| DbError::Wasm(format!("Failed to write parameters: {}", e)))?;

        // Execute with timeout
        let timeout_duration = Duration::from_millis(timeout_ms);
        let params = [Val::I32(params_ptr as i32), Val::I32(params_encoded.len() as i32)];
        let mut results = [];
        let call_future = main_func.call_async(
            &mut store,
            &params,
            &mut results,
        );

        let execution_result = match tokio::time::timeout(timeout_duration, call_future).await {
            Ok(Ok(_)) => {
                // Read result from memory (assuming result is written to a known location)
                // For now, return success
                ExecutionResult {
                    is_success: true,
                    value: Some(WasmValue::Null),
                    error: None,
                    memory_used_mb: store.data().memory_used as f64 / 1024.0 / 1024.0,
                }
            }
            Ok(Err(e)) => ExecutionResult {
                is_success: false,
                value: None,
                error: Some(format!("WASM execution error: {}", e)),
                memory_used_mb: store.data().memory_used as f64 / 1024.0 / 1024.0,
            },
            Err(_) => ExecutionResult {
                is_success: false,
                value: None,
                error: Some("Execution timeout".to_string()),
                memory_used_mb: store.data().memory_used as f64 / 1024.0 / 1024.0,
            },
        };

        Ok(execution_result)
    }

    fn register_host_functions(&self, linker: &mut Linker<StoreData>) -> Result<()> {
        // Register logging function
        linker
            .func_wrap(
                "env",
                "log",
                |mut caller: Caller<'_, StoreData>, level: i32, msg_ptr: i32, msg_len: i32| {
                    let memory = match caller.get_export("memory") {
                        Some(Extern::Memory(mem)) => mem,
                        _ => return,
                    };

                    let mut buffer = vec![0u8; msg_len as usize];
                    if memory.read(&caller, msg_ptr as usize, &mut buffer).is_ok() {
                        if let Ok(msg) = String::from_utf8(buffer) {
                            log::info!("[WASM:{}] {}", level, msg);
                        }
                    }
                },
            )
            .map_err(|e| DbError::Wasm(format!("Failed to register log function: {}", e)))?;

        // More host functions will be added in host_functions.rs
        Ok(())
    }

    fn encode_parameters(&self, context: &WasmExecutionContext) -> Result<Vec<u8>> {
        // Encode parameters as JSON for simplicity
        serde_json::to_vec(&context.parameters)
            .map_err(|e| DbError::Serialization(format!("Failed to encode parameters: {}", e)))
    }

    fn allocate_in_wasm(
        &self,
        store: &mut Store<StoreData>,
        memory: &Memory,
        size: usize,
    ) -> Result<usize> {
        // Simple bump allocator - in production, would call WASM's allocator
        let current_offset = store.data().alloc_offset;
        store.data_mut().alloc_offset += size;

        // Ensure memory is large enough
        let pages_needed = (store.data().alloc_offset + 65535) / 65536;
        let current_pages = memory.size(&*store) as usize;

        if pages_needed > current_pages {
            memory
                .grow(store, (pages_needed - current_pages) as u64)
                .map_err(|e| DbError::Wasm(format!("Failed to grow memory: {}", e)))?;
        }

        Ok(current_offset)
    }
}

pub struct StoreData {
    context: WasmExecutionContext,
    memory_used: usize,
    fuel_consumed: u64,
    alloc_offset: usize,
}

impl StoreData {
    fn new(context: WasmExecutionContext) -> Self {
        Self {
            context,
            memory_used: 0,
            fuel_consumed: 0,
            alloc_offset: 1024, // Start allocations after first KB
        }
    }
}

impl ResourceLimiter for StoreData {
    fn memory_growing(&mut self, current: usize, desired: usize, maximum: Option<usize>) -> std::result::Result<bool, wasmtime::Error> {
        let new_used = self.memory_used.saturating_add(desired.saturating_sub(current));

        if let Some(max) = maximum {
            if new_used > max {
                return Ok(false);
            }
        }

        // Check against our security policy (256MB max)
        if new_used > 256 * 1024 * 1024 {
            return Ok(false);
        }

        self.memory_used = new_used;
        Ok(true)
    }

    fn table_growing(&mut self, _current: u32, _desired: u32, _maximum: Option<u32>) -> std::result::Result<bool, wasmtime::Error> {
        Ok(true) // Allow table growth with default limits
    }
}

pub struct ExecutionResult {
    pub is_success: bool,
    pub value: Option<WasmValue>,
    pub error: Option<String>,
    pub memory_used_mb: f64,
}

impl ExecutionResult {
    pub fn success(value: WasmValue, memory_used_mb: f64) -> Self {
        Self {
            is_success: true,
            value: Some(value),
            error: None,
            memory_used_mb,
        }
    }

    pub fn error(error: String, memory_used_mb: f64) -> Self {
        Self {
            is_success: false,
            value: None,
            error: Some(error),
            memory_used_mb,
        }
    }
}
