use rust_db_core::{DbError, Result, CompactionConfig, CompactionStats, CompactionStrategy};
use super::{LsmStorage, SSTable, MemTable};
use std::path::{Path, PathBuf};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{info, warn, debug};

pub struct CompactionManager {
    config: CompactionConfig,
    storage: Arc<LsmStorage>,
    is_compacting: Mutex<bool>,
}

impl CompactionManager {
    pub fn new(storage: Arc<LsmStorage>, config: CompactionConfig) -> Self {
        Self {
            config,
            storage,
            is_compacting: Mutex::new(false),
        }
    }
    
    pub async fn trigger_compaction(&self) -> Result<CompactionStats> {
        let mut compacting = self.is_compacting.lock().await;
        if *compacting {
            return Err(DbError::Compaction("Compaction already in progress".to_string()));
        }
        
        *compacting = true;
        drop(compacting); // Release lock
        
        let stats = match &self.config.strategy {
            CompactionStrategy::Leveled { level_size_multiplier, level0_sstables_trigger } => {
                self.leveled_compaction(*level_size_multiplier, *level0_sstables_trigger).await
            }
            CompactionStrategy::Tiered { max_tier_size, tier_size_multiplier } => {
                self.tiered_compaction(*max_tier_size, *tier_size_multiplier).await
            }
            CompactionStrategy::SizeTiered { min_sstable_size, max_sstable_size, bucket_count } => {
                self.size_tiered_compaction(*min_sstable_size, *max_sstable_size, *bucket_count).await
            }
        }?;
        
        *self.is_compacting.lock().await = false;
        Ok(stats)
    }
    
    async fn leveled_compaction(
        &self, 
        level_size_multiplier: u64, 
        level0_sstables_trigger: usize
    ) -> Result<CompactionStats> {
        info!("Starting leveled compaction");
        let start_time = std::time::Instant::now();
        let mut stats = CompactionStats {
            sstables_merged: 0,
            space_reclaimed: 0,
            duration_ms: 0,
        };
        
        // Get current SSTables grouped by level
        let sstables_by_level = self.group_sstables_by_level().await;
        
        // Check Level 0 compaction trigger
        if let Some(level0_sstables) = sstables_by_level.get(&0) {
            if level0_sstables.len() >= level0_sstables_trigger {
                info!("Level 0 compaction triggered: {} SSTables", level0_sstables.len());
                let merged = self.merge_sstables(level0_sstables, 1).await?;
                stats.sstables_merged += merged;
            }
        }
        
        // Compact other levels
        for level in 1..=self.get_max_level(&sstables_by_level) {
            if let Some(current_level_sstables) = sstables_by_level.get(&level) {
                let next_level_size = self.calculate_level_size(level, level_size_multiplier);
                let current_size: u64 = current_level_sstables.iter().map(|sst| sst.size()).sum();
                
                if current_size > next_level_size {
                    info!("Level {} compaction triggered: {} bytes > {} bytes", 
                          level, current_size, next_level_size);
                    let merged = self.merge_sstables(current_level_sstables, level + 1).await?;
                    stats.sstables_merged += merged;
                }
            }
        }
        
        stats.duration_ms = start_time.elapsed().as_millis() as u64;
        info!("Leveled compaction completed: {:?}", stats);
        Ok(stats)
    }
    
    async fn tiered_compaction(&self, max_tier_size: u64, tier_size_multiplier: f64) -> Result<CompactionStats> {
        info!("Starting tiered compaction");
        let start_time = std::time::Instant::now();
        
        let sstables = self.get_all_sstables().await;
        let mut tiers = self.group_sstables_by_tier(&sstables, max_tier_size, tier_size_multiplier);
        
        let mut stats = CompactionStats {
            sstables_merged: 0,
            space_reclaimed: 0,
            duration_ms: 0,
        };
        
        // Compact tiers that exceed the size limit
        for (tier, tier_sstables) in &mut tiers {
            let tier_size: u64 = tier_sstables.iter().map(|sst| sst.size()).sum();
            if tier_size > max_tier_size {
                info!("Tier {} compaction triggered: {} bytes", tier, tier_size);
                let merged = self.merge_sstables(tier_sstables, *tier as u32).await?;
                stats.sstables_merged += merged;
            }
        }
        
        stats.duration_ms = start_time.elapsed().as_millis() as u64;
        info!("Tiered compaction completed: {:?}", stats);
        Ok(stats)
    }
    
    async fn size_tiered_compaction(
        &self, 
        min_sstable_size: u64, 
        max_sstable_size: u64, 
        bucket_count: usize
    ) -> Result<CompactionStats> {
        info!("Starting size-tiered compaction");
        let start_time = std::time::Instant::now();
        
        let sstables = self.get_all_sstables().await;
        let buckets = self.group_sstables_by_size(&sstables, min_sstable_size, max_sstable_size, bucket_count);
        
        let mut stats = CompactionStats {
            sstables_merged: 0,
            space_reclaimed: 0,
            duration_ms: 0,
        };
        
        // Compact buckets that have multiple SSTables
        for (bucket, bucket_sstables) in buckets {
            if bucket_sstables.len() > 1 {
                info!("Bucket {} compaction triggered: {} SSTables", bucket, bucket_sstables.len());
                let merged = self.merge_sstables(&bucket_sstables, 0).await?;
                stats.sstables_merged += merged;
            }
        }
        
        stats.duration_ms = start_time.elapsed().as_millis() as u64;
        info!("Size-tiered compaction completed: {:?}", stats);
        Ok(stats)
    }
    
    async fn merge_sstables(&self, sstables: &[SSTable], target_level: u32) -> Result<usize> {
        if sstables.is_empty() {
            return Ok(0);
        }
        
        let mut merged_data = BTreeMap::new();
        let mut total_size_before = 0u64;
        
        // Iterate through each SSTable and merge their data
        for sstable in sstables {
            total_size_before += sstable.size();
            debug!("Merging SSTable: {:?}", sstable.path());
            
            // Read all entries from this SSTable
            let entries = sstable.scan_all().await?;
            
            for (key, value) in entries {
                // Keep only the latest version of each key
                // In LSM-tree, newer entries have higher timestamps
                merged_data.entry(key)
                    .and_modify(|existing_value| {
                        // Compare timestamps and keep the newer one
                        if value.timestamp > existing_value.timestamp {
                            *existing_value = value.clone();
                        }
                    })
                    .or_insert(value);
            }
        }
        
        // Create new merged SSTable
        let new_sstable_path = self.generate_sstable_path(target_level);
        let new_sstable = SSTable::from_data(&new_sstable_path, merged_data).await?;
        
        // Remove old SSTables
        for sstable in sstables {
            tokio::fs::remove_file(sstable.path()).await.map_err(|e| {
                DbError::Io(format!("Failed to remove old SSTable: {}", e))
            })?;
        }
        
        // Add new SSTable to storage
        self.storage.add_sstable(new_sstable, target_level).await?;
        
        Ok(sstables.len())
    }
    
    // Helper methods
    async fn group_sstables_by_level(&self) -> HashMap<u32, Vec<SSTable>> {
        let mut sstables_by_level = HashMap::new();
        
        // Get all SSTables from storage and group by their level
        let all_sstables = self.storage.get_all_sstables().await;
        
        for sstable in all_sstables {
            let level = sstable.level();
            sstables_by_level.entry(level).or_insert_with(Vec::new).push(sstable);
        }
        
        sstables_by_level
    }
    
    async fn get_all_sstables(&self) -> Vec<SSTable> {
        self.storage.get_all_sstables().await
    }
    
    fn group_sstables_by_tier(
        &self, 
        sstables: &[SSTable], 
        max_tier_size: u64, 
        multiplier: f64
    ) -> HashMap<usize, Vec<SSTable>> {
        let mut tiers = HashMap::new();
        
        // Sort SSTables by size to group them into tiers
        let mut sorted_sstables = sstables.to_vec();
        sorted_sstables.sort_by_key(|sst| sst.size());
        
        let mut current_tier = 0;
        let mut current_tier_size = 0u64;
        let mut tier_limit = max_tier_size;
        
        for sstable in sorted_sstables {
            let sstable_size = sstable.size();
            
            // If adding this SSTable would exceed the tier limit, move to next tier
            if current_tier_size + sstable_size > tier_limit && current_tier_size > 0 {
                current_tier += 1;
                current_tier_size = 0;
                tier_limit = (tier_limit as f64 * multiplier) as u64;
            }
            
            tiers.entry(current_tier).or_insert_with(Vec::new).push(sstable);
            current_tier_size += sstable_size;
        }
        
        tiers
    }
    
    fn group_sstables_by_size(
        &self, 
        sstables: &[SSTable], 
        min_size: u64, 
        max_size: u64, 
        bucket_count: usize
    ) -> HashMap<usize, Vec<SSTable>> {
        let mut buckets = HashMap::new();
        
        // Calculate bucket size
        let bucket_range = (max_size - min_size) / bucket_count as u64;
        
        for sstable in sstables {
            let size = sstable.size();
            
            // Determine which bucket this SSTable belongs to
            let bucket_index = if size <= min_size {
                0
            } else if size >= max_size {
                bucket_count - 1
            } else {
                ((size - min_size) / bucket_range).min(bucket_count as u64 - 1) as usize
            };
            
            buckets.entry(bucket_index).or_insert_with(Vec::new).push(sstable.clone());
        }
        
        buckets
    }
    
    fn calculate_level_size(&self, level: u32, multiplier: u64) -> u64 {
        // Level size grows exponentially
        multiplier.pow(level)
    }
    
    fn get_max_level(&self, sstables_by_level: &HashMap<u32, Vec<SSTable>>) -> u32 {
        sstables_by_level.keys().max().copied().unwrap_or(0)
    }
    
    fn generate_sstable_path(&self, level: u32) -> PathBuf {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        
        self.storage.base_path().join(format!("sst_l{}_{}.bin", level, timestamp))
    }
}

// Background compaction task
pub struct BackgroundCompactor {
    manager: Arc<CompactionManager>,
    interval_secs: u64,
    stopped: Mutex<bool>,
}

impl BackgroundCompactor {
    pub fn new(manager: Arc<CompactionManager>, interval_secs: u64) -> Self {
        Self {
            manager,
            interval_secs,
            stopped: Mutex::new(false),
        }
    }
    
    pub async fn start(&self) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(self.interval_secs));
        
        loop {
            interval.tick().await;
            
            // Check if we should stop
            if *self.stopped.lock().await {
                break;
            }
            
            // Run compaction
            if let Err(e) = self.manager.trigger_compaction().await {
                warn!("Background compaction failed: {}", e);
            }
        }
        
        Ok(())
    }
    
    pub async fn stop(&self) {
        *self.stopped.lock().await = true;
    }
}