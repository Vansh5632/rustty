use std::sync::{Arc,RwLock};
use rust_db_core::{
    DbError,Result,Transaction,TransactionId,VersionTimestamp,VersionedRecord,TransactionState,MvccDatabase
};




pub struct TransactionManager{
    active_transactions: RwLock<Hashset<TransactionId>>,
    committed_transactions: RwLock<HashMap<TransactionId,VersionTimestamp>>,
    next_tx_id:Arc<AtomicU64>,
}

impl TransactionManager{
    pub fn new()->Self{
        Self{
            active_transactions:RwLock::new(Hashset::new()),
            committed_transactions:RwLock::new(HashMap::new()),
            next_tx_id:Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn begin_transaction(&self)->Transaction{
        let tx_id = Transactionid::new();
        self.active_transactions.write().unwrap().insert(tx_id);

        let snapshot_ts = self.get_latest_commit_timestamp();

        Transaction{
            id:tx_id,
            snapshot_ts,
            state:TransactionState::Active,
            writes:HashMap::new(),
        }
    }

    pub fn commit_transaction(&self,transaction:&mut Transaction)->Result<()>{
        let tx_id = transaction.id;
        if !self.active_transactions.read().unwrap().contains(&tx_id){
            return Err(DbError::Transaction("Transaction not active".to_string()));
        }

        transaction.state = TransactionState::Committed;

        let commit_ts = VersionTimestamp::now();
        self.committed_transactions.write().unwrap().insert(tx_id,commit_ts);

        self.active_transactions.write().unwrap().remove(&tx_id);

        Ok(())
    }

    pub fn rollback_transaction(&self,transaction:&mut Transaction)->Result<()>{
        let tx_id = transaction.id;
        if !self.active_transactions.read().unwrap().contains(&tx_id){
            return Err(DbError::Transaction("Transaction not active".to_string()));
        }

        transaction.state = TransactionState::Aborted;
        self.active_transactions.write().unwrap().remove(&tx_id);

        Ok(())
    }

    pub fn is_transaction_committed(&self,tx_id:TransactionId)->bool{
        self.committed_transactions.read().unwrap().contains_key(&tx_id)
    }

    pub fn get_commit_timestamp(&self,tx_id:TransactionId)->Option<VersionTimestamp>{
        self.committed_transactions.read().unwrap().get(&tx_id).copied()
    }

    pub fn get_latest_commit_timestamp(&self)->VersionTimestamp{
        let committed = self.committed_transactions.read().unwrap();
        committed.values().max().copied().unwrap_or(VersionTimestamp(0))
    }
}

pub struct MvccStorage{
    base_storage:LsmStorage,
    transaction_manager:Arc<TransactionManager>,
    version_store:RwLock<HashMap<Vec<u8>,Vec<VersionedRecord>>>,
}

impl MvccStorage{
    pub fn new(base_storage:LsmStorage)->Self{
        Self{
            base_storage,
            transaction_manager:Arc::new(TransactionManager::new()),
            version_store:RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_version(

        &self,
        key: &[u8],
        transaction:&Transaction
    )->Result<Option<VersionedRecord>>{
        let versions = self.version_store.read().unwrap();

        if let Some(version_list) = version.get(key){
            for version in version_list.iter().rev(){
                if version.is_visible(transaction.id,transaction.snapshot_ts){
                    return Ok(Some(version.clone()));
                }
            }
        }

        if let Some(data) = self.base_storage.get(key).await?{
            let record = VersionedRecord::new(data,TransactionId(0));

            return Ok(Some(record));
        }

        Ok(None)
    }

    pub async fn put_version(
        &self,
        key:&[u8],
        value:Vec<u8>,
        transaction:&Transaction
    )->Result<()>{
        let mut versions = self.version_store.write().unwrap();

        let new_record = VersionedRecord::new(value,transaction.id);

        versions.entry(key.to_vec()).or_insert_with(Vec::new).push(new_record);

        Ok(())
    }

    pub async fn mark_version_expired(
        &self,
        key:&[u8],
        transaction:&Transaction
    )->Result<()>{
        let mut versions = self.version_store.write().unwrap();

        if let Some(version_list) = versions.get_mut(key){
            if let Some(latest_version) = version_list.last_mut(){
                latest_record.mark_experied(transaction.id);
            }
        }

        Ok(())
    }

     pub async fn apply_transaction_writes(&self, transaction: &Transaction) -> Result<()> {
        for (key, value_opt) in &transaction.writes {
            match value_opt {
                Some(value) => {
                    self.put_version(key, value.clone(), transaction).await?;
                }
                None => {
                    self.mark_version_expired(key, transaction).await?;
                }
            }
        }
        Ok(())
    }
    
    pub async fn scan_versions(
        &self,
        prefix: &[u8],
        transaction: &Transaction,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let versions = self.version_store.read().unwrap();
        
        for (key, version_list) in versions.iter() {
            if !key.starts_with(prefix) {
                continue;
            }
            
            // Find the latest visible version
            for version in version_list.iter().rev() {
                if version.is_visible(transaction.id, transaction.snapshot_ts) {
                    if !version.value.is_empty() { // Skip tombstones
                        results.push((key.clone(), version.value.clone()));
                    }
                    break;
                }
            }
        }
        
        Ok(results)
    }
}