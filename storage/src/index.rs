use std::collections::HashMap;
use crate::LsmStorage;
use rust_db_core::{Result, Value};

#[derive(Debug, Clone)]
pub struct IndexDescriptor {
    pub name: String,
    pub field: String,
    pub index_type: IndexType,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    Hash,
    BTree,
}

pub struct IndexManager {
    indexes: HashMap<String, IndexDescriptor>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    pub async fn create_index(&mut self, descriptor: IndexDescriptor) -> Result<()> {
        self.indexes.insert(descriptor.name.clone(), descriptor);
        Ok(())
    }

    pub async fn drop_index(&mut self, index_name: &str) -> Result<()> {
        self.indexes.remove(index_name);
        Ok(())
    }

    pub async fn update_index(
        &self,
        storage: &LsmStorage,
        index_name: &str,
        record_key: &[u8],
        field_value: &Value,
    ) -> Result<()> {
        if let Some(descriptor) = self.indexes.get(index_name) {
            let index_key = self.build_index_key(&descriptor.name, field_value, record_key);
            storage.put(&index_key, &[]).await?;
        }
        Ok(())
    }

    pub async fn lookup_index(
        &self,
        storage: &LsmStorage,
        index_name: &str,
        value: &Value,
    ) -> Result<Vec<Vec<u8>>> {
        let prefix = self.build_index_prefix(index_name, value);
        let records = storage.scan(&prefix).await?;

        let record_keys = records
            .into_iter()
            .map(|(key, _)| self.extract_record_key(&key))
            .collect();

        Ok(record_keys)
    }

    fn build_index_key(&self, index_name: &str, field_value: &Value, record_key: &[u8]) -> Vec<u8> {
        let value_bytes = bincode::serialize(field_value).unwrap();
        let mut key = Vec::new();
        key.extend(b"index:");
        key.extend(index_name.as_bytes());
        key.extend(b":");
        key.extend(&value_bytes);
        key.extend(b":");
        key.extend(record_key);
        key
    }

    fn build_index_prefix(&self, index_name: &str, value: &Value) -> Vec<u8> {
        let value_bytes = bincode::serialize(value).unwrap();
        let mut prefix = Vec::new();
        prefix.extend(b"index:");
        prefix.extend(index_name.as_bytes());
        prefix.extend(b":");
        prefix.extend(&value_bytes);
        prefix.extend(b":");
        prefix
    }

    fn extract_record_key(&self, index_key: &[u8]) -> Vec<u8> {
        // Extract the record key part from the index key
        // Format: "index:{name}:{value}:{record_key}"
        let parts: Vec<&[u8]> = index_key.splitn(4, |&b| b == b':').collect();
        if parts.len() == 4 {
            parts[3].to_vec()
        } else {
            Vec::new()
        }
    }
}