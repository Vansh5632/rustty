use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize, Deserialize};
use thiserror::Error;
use std::collections::HashMap;

#[derive(Error,Debug)]
pub enum DbError {
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Query error: {0}")]
    Query(String),
    
    #[error("Schema error: {0}")]
    Schema(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),
}

//type alias 
pub type Result<T> = std::result::Result<T,DbError>;

#[async_trait]
pub trait Database:Send+Sync{
    async fn insert<T:Serialize+ Send + Sync>(&self,key:&[u8],value:&T)->Result<()>;
    async fn get<T:DeserializeOwned>(&self,key:&[u8])->Result<Option<T>>;
    async fn delete(&self,key:&[u8]) -> Result<()>;
    async fn scan(&self,prefic:&[u8])-> Result<Vec<(Vec<u8>,Vec<u8>)>>;
}

pub trait Schema:Send+Sync {
    fn validate(&self) -> Result<()>;
    fn table_name()-> &'static str;
    fn indexes(&self)-> HashMap<String,Vec<u8>>;
}

#[derive(Debug,Clone)]
pub struct Query{
    pub filters:Vec<Filter>,
    pub limit:Option<usize>,
    pub order_by:Option<String>,
}

#[derive(Debug,Clone)]
pub struct Filter{
    pub field:String,
    pub operator:Operator,
    pub value:Value,
}

// Alias for backward compatibility
pub type FieldFilter = Filter;

#[derive(Debug,Clone)]
pub enum Operator{
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    Contains,
    StartsWith,
    EndsWith,
}

// Alias for backward compatibility
pub type FilterOperator = Operator;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value{
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

impl Value{
    pub fn type_matches(&self,other:&Value)->bool{
        matches!(
            (self,other),
            (Value::Int(_),Value::Int(_))|
            (Value::Float(_),Value::Float(_))|
            (Value::String(_),Value::String(_))|
            (Value::Bool(_),Value::Bool(_))|
            (Value::Null,Value::Null)
        )
    }
}

pub struct Transaction{
    pub id:u64,
    pub snapshot_ts:u64,
}

impl Transaction{
    pub async fn commit(self)->Result<()>{
        Ok(())
    }

    pub async fn rollback(self)->Result<()>{
        Ok(())
    }
}

pub trait FieldAccess{
    fn get_field(&self,field_name:&str) -> Option<Value>;
}

// From implementations for Value conversion
impl From<&u64> for Value {
    fn from(val: &u64) -> Self {
        Value::Int(*val as i64)
    }
}

impl From<&u32> for Value {
    fn from(val: &u32) -> Self {
        Value::Int(*val as i64)
    }
}

impl From<&i64> for Value {
    fn from(val: &i64) -> Self {
        Value::Int(*val)
    }
}

impl From<&f64> for Value {
    fn from(val: &f64) -> Self {
        Value::Float(*val)
    }
}

impl From<&bool> for Value {
    fn from(val: &bool) -> Self {
        Value::Bool(*val)
    }
}

impl From<&String> for Value {
    fn from(val: &String) -> Self {
        Value::String(val.clone())
    }
}

impl From<&str> for Value {
    fn from(val: &str) -> Self {
        Value::String(val.to_string())
    }
}