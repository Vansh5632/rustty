use async_trait::async_trait;
use serde::de::value;
use serde::{de::DeserializeOwned, Serialize, Deserialize};
use thiserror::Error;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64,Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod compaction;
pub use compaction::{CompactionStats,CompactionConfig,CompactionStrategy,GcConfig,GcStats};

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

    #[error("Transaction error: {0}")]
    Transaction(String),
    #[error("Deadlock detected: {0}")]
    Deadlock(String),

    #[error("Compaction error: {0}")]
    Compaction(String),

    #[error("Garbage collection error:{0}")]
    GarbageCollection(String),
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

// pub struct Transaction{
//     pub id:u64,
//     pub snapshot_ts:u64,
// }

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

//MVCC types 
pub struct TransactionId(u64);

impl TransactionId{
    pub fn new()->Self{
        static COUNTER:AtomicU64 = AtomicU64::new(1);
        TransactionId(COUNTER.fetch_add(1,Ordering::SeqCst))
    }

    pub fn as_u64(&self)->u64{
        self.0
    }
}

impl std::hash::Hash for TransactionId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for TransactionId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for TransactionId {}

impl Clone for TransactionId {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for TransactionId {}

impl std::fmt::Debug for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TransactionId").field(&self.0).finish()
    }
}

impl Serialize for TransactionId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TransactionId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(TransactionId)
    }
}

#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize,PartialOrd,Ord)]
pub struct VersionTimestamp(u64);

impl VersionTimestamp{
    pub fn now()-> Self{
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
        VersionTimestamp(now)
    }
    
    pub fn as_u64(&self) -> u64{
        self.0
    }
}

// Transaction states
#[derive(Debug,Clone)]
pub enum TransactionState{
    Active,
    Committed,
    Aborted,
}

// MVCC Record with version info
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct VersionedRecord{
    pub value:Vec<u8>,
    pub created_tx:TransactionId,
    pub expired_tx:TransactionId,
    pub created_ts:VersionTimestamp,
    pub expired_ts:VersionTimestamp,
}

impl VersionedRecord{
    pub fn new(value:Vec<u8>,tx_id:TransactionId)-> Self{
        let now = VersionTimestamp::now();
        Self { value, created_tx: tx_id, expired_tx: TransactionId(0), created_ts: now, expired_ts: VersionTimestamp(0) }
    }

    pub fn is_visible(&self,tx_id:TransactionId,snapshot_ts:VersionTimestamp)->bool{
        self.created_ts <= snapshot_ts && (self.expired_tx.0 == 0 || self.expired_ts > snapshot_ts) && self.created_tx != tx_id
    }

    pub fn mark_expired(&mut self,tx_id:TransactionId){
        self.expired_tx = tx_id;
        self.expired_ts = VersionTimestamp::now();
    }
}


pub struct Transaction{
    pub id:TransactionId,
    pub snapshot_ts:VersionTimestamp,
    pub state:TransactionState,
    pub writes:HashMap<Vec<u8>,Option<Vec<u8>>>,
}

impl Transaction{
    pub fn new()->Self{
        Self { id: TransactionId::new(), snapshot_ts: VersionTimestamp::now(), state: TransactionState::Active, writes: HashMap::new() }
    }

    pub fn put(&mut self,key:Vec<u8>,value:Vec<u8>){
        self.writes.insert(key, Some(value));
    }

    pub fn delete(&mut self,key:Vec<u8>){
        self.writes.insert(key, None);
    }
}

#[async_trait]

pub trait MvccDatabase:Database {
    async fn begin_transaction(&self)-> Result<Transaction>;
    async fn commit_transaction(&self,transaction:Transaction) ->Result<()>;
    async fn rollback_transaction(&self,transaction:Transaction) -> Result<()>;

    async fn get_for_transaction<T:DeserializeOwned>(
        &self,
        key:&[u8],
        transaction:&Transaction,
    )->Result<Option<T>>;

    async fn scan_for_transaction(
        &self,
        prefix:&[u8],
        transaction:&Transaction,
    )->Result<Vec<(Vec<u8>,Vec<u8>)>>;
}

pub struct TransactionContext<'a,D:MvccDatabase>{
    db:&'a D,
    transaction:Option<Transaction>,
}

impl<'a,D:MvccDatabase> TransactionContext<'a,D>{
    pub async fn new(db:&'a D)->Result<Self>{
        let transaction = db.begin_transaction().await?;
        Ok(Self { db, transaction: Some(transaction) })
    }

    pub async fn commit(mut self) -> Result<()>{
        if let Some(transaction) = self.transaction.take(){
            self.db.commit_transaction(transaction).await
        }else{
            Err(DbError::Transaction("Transaction already completed".to_string()))
        }
    }

    pub async fn rollback(mut self) -> Result<()>{
        if let Some(transaction) = self.transaction.take(){
            self.db.rollback_transaction(transaction).await
        }else{
            Err(DbError::Transaction("Transaction already completed".to_string()))
        }
    }

    pub fn transaction(&self) -> &Transaction{
        self.transaction.as_ref().unwrap()
    }

    pub fn transaction_mut(&mut self) -> &mut Transaction{
        self.transaction.as_mut().unwrap()
    }
}

impl<'a ,D:MvccDatabase> Drop for TransactionContext<'a,D>{
    fn drop(&mut self){
        if self.transaction.is_some(){

        }
    }
}

// Implement Database trait for Arc<T> where T: Database
#[async_trait]
impl<T: Database> Database for std::sync::Arc<T> {
    async fn insert<V: Serialize + Send + Sync>(&self, key: &[u8], value: &V) -> Result<()> {
        (**self).insert(key, value).await
    }

    async fn get<V: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<V>> {
        (**self).get(key).await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        (**self).delete(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        (**self).scan(prefix).await
    }
}

// Implement MvccDatabase trait for Arc<T> where T: MvccDatabase
#[async_trait]
impl<T: MvccDatabase> MvccDatabase for std::sync::Arc<T> {
    async fn begin_transaction(&self) -> Result<Transaction> {
        (**self).begin_transaction().await
    }

    async fn commit_transaction(&self, transaction: Transaction) -> Result<()> {
        (**self).commit_transaction(transaction).await
    }

    async fn rollback_transaction(&self, transaction: Transaction) -> Result<()> {
        (**self).rollback_transaction(transaction).await
    }

    async fn get_for_transaction<V: DeserializeOwned>(
        &self,
        key: &[u8],
        transaction: &Transaction,
    ) -> Result<Option<V>> {
        (**self).get_for_transaction(key, transaction).await
    }

    async fn scan_for_transaction(
        &self,
        prefix: &[u8],
        transaction: &Transaction,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        (**self).scan_for_transaction(prefix, transaction).await
    }
}