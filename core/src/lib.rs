use async_trait::async_trait;
use serde::{de::{value, DeserializeOwned}, Deserialize, Serialize};
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
    async fn insert<T:Serialize+ Send>(&self,key:&[u8],value:&T)->Result<()>;
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

#[derive(Debug,Clone)]
pub enum Operator{
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
}

#[derive(Debug,Clone)]
pub enum Value{
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
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