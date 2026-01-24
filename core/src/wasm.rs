use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::security::{SecurityContext, Permission};

// WASM Procedure definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmProcedure {
    pub name: String,
    pub module_hash: String,
    pub parameters: Vec<Parameter>,
    pub return_type: DataType,
    pub permissions: Vec<Permission>,
    pub timeout_ms: u64,
    pub memory_limit_mb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parameter {
    pub name: String,
    pub data_type: DataType,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Float,
    String,
    Bool,
    Bytes,
    Json,
    Record(String), // Table name
}

// WASM execution context
#[derive(Debug, Clone)]
pub struct WasmExecutionContext {
    pub procedure_name: String,
    pub parameters: HashMap<String, WasmValue>,
    pub security_context: SecurityContext,
    pub transaction_id: Option<String>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WasmValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Null,
}

impl WasmValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            WasmValue::Int(_) => "int",
            WasmValue::Float(_) => "float",
            WasmValue::String(_) => "string",
            WasmValue::Bool(_) => "bool",
            WasmValue::Bytes(_) => "bytes",
            WasmValue::Json(_) => "json",
            WasmValue::Null => "null",
        }
    }
}

// WASM execution result
#[derive(Debug, Clone)]
pub struct WasmExecutionResult {
    pub success: bool,
    pub value: Option<WasmValue>,
    pub error_message: Option<String>,
    pub execution_time_ms: u64,
    pub memory_used_mb: f64,
}

impl WasmExecutionResult {
    pub fn success(value: WasmValue, execution_time_ms: u64, memory_used_mb: f64) -> Self {
        Self {
            success: true,
            value: Some(value),
            error_message: None,
            execution_time_ms,
            memory_used_mb,
        }
    }

    pub fn error(error_message: String, execution_time_ms: u64, memory_used_mb: f64) -> Self {
        Self {
            success: false,
            value: None,
            error_message: Some(error_message),
            execution_time_ms,
            memory_used_mb,
        }
    }
}
