use rust_db_core::{Database, DbError, MvccDatabase, Result, Transaction, TransactionState, CompactionConfig, CompactionStats, GcConfig, GcStats};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use memmap::Mmap;
use serde::{Serialize, Deserialize};
use lazy_static::lazy_static;
use std::time::{SystemTime, UNIX_EPOCH};

mod mvcc;
pub use mvcc::{MvccStorage, TransactionManager};

mod index;
pub use index::{IndexDescriptor, IndexManager, IndexType};

mod compaction;
mod garbage_collector;

pub use compaction::{CompactionManager,BackgroundCompactor};
pub use garbage_collector::{GarbageCollector,BackgroundGc};

lazy_static! {
    static ref FLUSH_THRESHOLD: usize = 1024 * 1024; // 1MB
}

// Write-Ahead Log for durability
pub struct WriteAheadLog {
    file: BufWriter<File>,
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        Ok(WriteAheadLog {
            file: BufWriter::new(file),
            path: path.to_path_buf(),
        })
    }
    
    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = WalEntry {
            key: key.to_vec(),
            value: value.to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        };
        
        bincode::serialize_into(&mut self.file, &entry)
            .map_err(|e| DbError::Serialization(e.to_string()))?;
            
        self.file.flush()
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct WalEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
}

// MemTable for in-memory storage
pub struct MemTable {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
            size: 0,
        }
    }
    
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.size += key.len() + value.len();
        self.data.insert(key, value);
    }
    
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }
    
    pub fn scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    
    pub fn should_flush(&self) -> bool {
        self.size > *FLUSH_THRESHOLD
    }
    
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

// SSTable (Sorted String Table) for disk storage
#[derive(Clone)]
pub struct SSTable {
    pub path: PathBuf,
    data: Arc<Mmap>,
    pub file_size: u64,
    pub level: u32,
}

impl SSTable {
    pub fn from_memtable(path: &Path, memtable: &MemTable) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        // Serialize memtable to file
        for (key, value) in &memtable.data {
            let entry = (key, value);
            bincode::serialize_into(&mut file, &entry)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
        }
        
        file.flush()
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        // Get file size
        let metadata = std::fs::metadata(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
        let file_size = metadata.len();
            
        // Memory map the file for fast reads
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        unsafe {
            let data = Mmap::map(&file)
                .map_err(|e| DbError::Storage(e.to_string()))?;
                
            Ok(SSTable {
                path: path.to_path_buf(),
                data: Arc::new(data),
                file_size,
                level: 0,
            })
        }
    }
    
    pub fn get(&self, _key: &[u8]) -> Option<Vec<u8>> {
        // Simplified - in real implementation would use bloom filter and sparse index
        // For now, we'll implement proper scanning in the next iteration
        None
    }
    
    pub async fn iter(&self) -> Result<Vec<(Vec<u8>, ValueWithTimestamp)>> {
        // Simplified implementation - returns all key-value pairs
        // In production, this would be a streaming iterator
        let mut entries = Vec::new();
        let cursor = 0;
        
        while cursor < self.data.len() {
            if let Ok((key, value)) = bincode::deserialize_from(&self.data[cursor..]as &[u8]) {
                entries.push((key, ValueWithTimestamp { value, timestamp: 0 }));
                // Move cursor - simplified, in production would track exact position
                break;
            } else {
                break;
            }
        }
        
        Ok(entries)
    }
    
    pub async fn create(path: &Path, data: BTreeMap<Vec<u8>, ValueWithTimestamp>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        // Serialize data to file
        for (key, value_with_ts) in &data {
            let entry = (key, &value_with_ts.value);
            bincode::serialize_into(&mut file, &entry)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
        }
        
        file.flush()
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        // Get file size
        let metadata = std::fs::metadata(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
        let file_size = metadata.len();
            
        // Memory map the file
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        unsafe {
            let mmap_data = Mmap::map(&file)
                .map_err(|e| DbError::Storage(e.to_string()))?;
                
            Ok(SSTable {
                path: path.to_path_buf(),
                data: Arc::new(mmap_data),
                file_size,
                level: 0,
            })
        }
    }
}

#[derive(Clone, Debug)]
pub struct ValueWithTimestamp {
    pub value: Vec<u8>,
    pub timestamp: u64,
}

// Main LSM Storage Engine
#[derive(Clone)]
pub struct LsmStorage {
    memtable: Arc<RwLock<MemTable>>,
    wal: Arc<RwLock<WriteAheadLog>>,
    sstables: Arc<RwLock<Vec<SSTable>>>,
    sstable_levels: Arc<RwLock<HashMap<u32, Vec<SSTable>>>>,
    base_path: PathBuf,
    index_manager: Arc<RwLock<IndexManager>>,
    compaction_manager: Option<Arc<CompactionManager>>,
}

impl LsmStorage {
    pub fn new(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        let wal_path = path.join("wal.bin");
        let wal = WriteAheadLog::new(&wal_path)?;
        let storage = LsmStorage {
            memtable: Arc::new(RwLock::new(MemTable::new())),
            wal: Arc::new(RwLock::new(wal)),
            sstables: Arc::new(RwLock::new(Vec::new())),
            sstable_levels: Arc::new(RwLock::new(HashMap::new())),
            base_path: path.to_path_buf(),
            index_manager: Arc::new(RwLock::new(IndexManager::new())),
            compaction_manager: None,
        };
        Ok(storage)
    }
    
    pub fn with_compaction(mut self, config: CompactionConfig) -> Self {
        let storage_arc = Arc::new(self.clone());
        self.compaction_manager = Some(Arc::new(CompactionManager::new(storage_arc, config)));
        self
    }
    
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
    
    pub async fn trigger_compaction(&self) -> Result<CompactionStats> {
        if let Some(ref manager) = self.compaction_manager {
            manager.trigger_compaction().await
        } else {
            Err(DbError::Storage("Compaction manager not initialized".to_string()))
        }
    }
    
    pub async fn add_sstable(&self, sstable: SSTable, level: u32) -> Result<()> {
        let mut levels = self.sstable_levels.write().unwrap();
        levels.entry(level).or_insert_with(Vec::new).push(sstable);
        Ok(())
    }
    
    pub fn get_sstables_at_level(&self, level: u32) -> Vec<SSTable> {
        let levels = self.sstable_levels.read().unwrap();
        levels.get(&level).cloned().unwrap_or_default()
    }
    
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Write to WAL first (for durability)
        self.wal.write()
            .map_err(|e| DbError::Storage(format!("WAL lock error: {}", e)))?
            .write_entry(key, value)?;
        
        // Write to memtable
        {
            let mut memtable = self.memtable.write().unwrap();
            memtable.insert(key.to_vec(), value.to_vec());
            
            // Flush to SSTable if threshold reached
            if memtable.should_flush() {
                self.flush_memtable()?;
            }
        }
        
        Ok(())
    }
    
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check memtable first
        {
            let memtable = self.memtable.read().unwrap();
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value));
            }
        }
        
        // Check SSTables (from newest to oldest)
        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter().rev() {
            if let Some(value) = sstable.get(key) {
                return Ok(Some(value));
            }
        }
        
        Ok(None)
    }
    
    pub async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        
        // Scan memtable
        {
            let memtable = self.memtable.read().unwrap();
            results.extend(memtable.scan(prefix));
        }
        
        // Scan SSTables
        let sstables = self.sstables.read().unwrap();
        for _sstable in sstables.iter() {
            // Simplified - in real implementation would properly scan SSTable
        }
        
        // Deduplicate (newer values override older ones)
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results.dedup_by(|a, b| a.0 == b.0);
        
        Ok(results)
    }
    
    fn flush_memtable(&self) -> Result<()> {
        let mut memtable = self.memtable.write().unwrap();
        
        if memtable.len() == 0 {
            return Ok(());
        }
        
        // Create new SSTable from current memtable
        let sstable_path = self.base_path.join(format!(
            "sst_{}.bin",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros()
        ));
        
        let sstable = SSTable::from_memtable(&sstable_path, &memtable)?;
        
        // Add to SSTable list
        self.sstables.write().unwrap().push(sstable);
        
        // Clear memtable
        *memtable = MemTable::new();
        
        // Clear WAL (in production, we'd use segment-based WAL)
        let wal_path = self.base_path.join("wal.bin");
        let new_wal = WriteAheadLog::new(&wal_path)?;
        *self.wal.write().unwrap() = new_wal;
        
        Ok(())
    }

    // Index management methods
    pub async fn create_index(&self, descriptor: IndexDescriptor) -> Result<()> {
        let mut index_mgr = self.index_manager.write().unwrap();
        index_mgr.create_index(descriptor).await
    }

    pub async fn get_by_index<T: serde::de::DeserializeOwned>(
        &self,
        index_name: &str,
        value: &rust_db_core::Value,
    ) -> Result<Vec<T>> {
        let index_mgr = self.index_manager.read().unwrap();
        let record_keys = index_mgr.lookup_index(self, index_name, value).await?;

        let mut results = Vec::new();
        for key in record_keys {
            if let Some(data) = self.get(&key).await? {
                let item: T = bincode::deserialize(&data)
                    .map_err(|e| DbError::Serialization(e.to_string()))?;
                results.push(item);
            }
        }

        Ok(results)
    }
}

// Implement core Database trait for LsmStorage
#[async_trait::async_trait]
impl Database for LsmStorage {
    async fn insert<T: Serialize + Send + Sync>(&self, key: &[u8], value: &T) -> Result<()> {
        let serialized = bincode::serialize(value)
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        self.put(key, &serialized).await
    }
    
    async fn get<T: serde::de::DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(data) => {
                let value = bincode::deserialize(&data)
                    .map_err(|e| DbError::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
    
    async fn delete(&self, key: &[u8]) -> Result<()> {
        // Tombstone marker for deletion
        self.put(key, &[]).await
    }
    
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(prefix).await
    }
}

// Update LsmStorage to implement MvccDatabase
#[async_trait::async_trait]
impl MvccDatabase for LsmStorage {
    async fn begin_transaction(&self) -> Result<Transaction> {
        // For now, return a simple transaction
        // In a real implementation, this would integrate with the transaction manager
        Ok(Transaction::new())
    }
    
    async fn commit_transaction(&self, mut transaction: Transaction) -> Result<()> {
        // Apply all writes from the transaction
        for (key, value_opt) in &transaction.writes {
            match value_opt {
                Some(value) => {
                    self.put(key, value).await?;
                }
                None => {
                    self.delete(key).await?;
                }
            }
        }
        
        transaction.state = TransactionState::Committed;
        Ok(())
    }
    
    async fn rollback_transaction(&self, mut transaction: Transaction) -> Result<()> {
        // Simply mark as aborted - writes are not applied
        transaction.state = TransactionState::Aborted;
        Ok(())
    }
    
    async fn get_for_transaction<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
        _transaction: &Transaction,
    ) -> Result<Option<T>> {
        // For now, use regular get - in full MVCC this would check transaction visibility
        <Self as Database>::get(self, key).await
    }
    
    async fn scan_for_transaction(
        &self,
        prefix: &[u8],
        _transaction: &Transaction,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // For now, use regular scan
        <Self as Database>::scan(self, prefix).await
    }
}
// Enhanced LsmStorage with MVCC support
pub struct MvccLsmStorage {
    base_storage: LsmStorage,
    transaction_manager: Arc<TransactionManager>,
    mvcc_storage: Option<Arc<MvccStorage>>,
    garbage_collector: Option<Arc<GarbageCollector>>,
}

impl MvccLsmStorage {
    pub fn new(path: &std::path::Path) -> Result<Self> {
        let base_storage = LsmStorage::new(path)?;
        let transaction_manager = Arc::new(TransactionManager::new());
        
        Ok(Self {
            base_storage,
            transaction_manager,
            mvcc_storage: None,
            garbage_collector: None,
        })
    }
    
    pub fn with_garbage_collection(mut self, config: GcConfig) -> Result<Self> {
        let base_storage_clone = self.base_storage.clone();
        let mvcc_storage = Arc::new(MvccStorage::new(base_storage_clone));
        let gc = Arc::new(GarbageCollector::new(Arc::clone(&mvcc_storage), config));
        self.mvcc_storage = Some(mvcc_storage);
        self.garbage_collector = Some(gc);
        Ok(self)
    }
    
    pub async fn run_garbage_collection(&self) -> Result<GcStats> {
        if let Some(ref gc) = self.garbage_collector {
            gc.run_garbage_collection().await
        } else {
            Err(DbError::Storage("Garbage collector not initialized".to_string()))
        }
    }
}

#[async_trait::async_trait]
impl Database for MvccLsmStorage {
    async fn insert<T: Serialize + Send + Sync>(&self, key: &[u8], value: &T) -> Result<()> {
        self.base_storage.insert(key, value).await
    }
    
    async fn get<T: serde::de::DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        <LsmStorage as Database>::get(&self.base_storage, key).await
    }
    
    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.base_storage.delete(key).await
    }
    
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.base_storage.scan(prefix).await
    }
}

#[async_trait::async_trait]
impl MvccDatabase for MvccLsmStorage {
    async fn begin_transaction(&self) -> Result<Transaction> {
        Ok(self.transaction_manager.begin_transaction())
    }
    
    async fn commit_transaction(&self, mut transaction: Transaction) -> Result<()> {
        // Apply all writes from the transaction to the base storage
        for (key, value_opt) in &transaction.writes {
            match value_opt {
                Some(value) => {
                    self.base_storage.put(key, value).await?;
                }
                None => {
                    self.base_storage.delete(key).await?;
                }
            }
        }
        
        // Mark transaction as committed
        self.transaction_manager.commit_transaction(&mut transaction)?;
        Ok(())
    }
    
    async fn rollback_transaction(&self, mut transaction: Transaction) -> Result<()> {
        self.transaction_manager.rollback_transaction(&mut transaction)?;
        Ok(())
    }
    
    async fn get_for_transaction<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
        _transaction: &Transaction,
    ) -> Result<Option<T>> {
        // For simplified MVCC, just use base storage
        // In full implementation, this would check version visibility
        <LsmStorage as Database>::get(&self.base_storage, key).await
    }
    
    async fn scan_for_transaction(
        &self,
        prefix: &[u8],
        _transaction: &Transaction,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // For simplified MVCC, just use base storage
        self.base_storage.scan(prefix).await
    }
}