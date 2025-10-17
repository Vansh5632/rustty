use rust_db_core::{DbError, Database, Result};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use memmap::Mmap;
use serde::{Serialize, Deserialize};
use lazy_static::lazy_static;
use std::time::{SystemTime, UNIX_EPOCH};

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
pub struct SSTable {
    path: PathBuf,
    data: Mmap,
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
                data,
            })
        }
    }
    
    pub fn get(&self, _key: &[u8]) -> Option<Vec<u8>> {
        // Simplified - in real implementation would use bloom filter and sparse index
        // For now, we'll implement proper scanning in the next iteration
        None
    }
}

// Main LSM Storage Engine
pub struct LsmStorage {
    memtable: Arc<RwLock<MemTable>>,
    wal: RwLock<WriteAheadLog>,
    sstables: RwLock<Vec<SSTable>>,
    base_path: PathBuf,
}

impl LsmStorage {
    pub fn new(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| DbError::Storage(e.to_string()))?;
            
        let wal_path = path.join("wal.bin");
        let wal = WriteAheadLog::new(&wal_path)?;
        
        Ok(LsmStorage {
            memtable: Arc::new(RwLock::new(MemTable::new())),
            wal: RwLock::new(wal),
            sstables: RwLock::new(Vec::new()),
            base_path: path.to_path_buf(),
        })
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