use rust_db_core::{DbError,Result,GcConfig,GcStats,VersionTimestamp};
use super::mvcc::MvccStorage;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use log::{info,warn,debug};

pub struct GarbageCollector{
    config:GcConfig,
    mvcc_storage:Arc<MvccStorage>,
    is_collecting:Mutex<bool>,
}



impl GarbageCollector{
    pub fn new(mvcc_storage:Arc<MvccStorage>,config:GcConfig)->Self{
        Self{
            config,
            mvcc_storage,
            is_collecting:Mutex::new(false),
        }
    }

    pub async fn run_garbage_collection(&self)->Result<GcStats>{
        let mut collecting = self.is_collecting.lock().await;
        if *collecting{
            return Err(DbError::GarbageCollection("GC  already in progress".to_string()));
        }

        *collecting = true;
        drop(collecting);

        let start_time = std::time::Instant::now();
        let mut stats = GcStats{
            versions_removed:0,
            space_reclaimed:0,
            duration_ms:0,
        };

        info!("starting garbage collections ");

        let oldest_snapshot_ts = self.get_oldest_active_snapshot().await;
        let retention_threshold = self.calculate_retention_threshold().await;

        let versions_to_remove = self.find_obsolete_versions(oldest_snapshot_ts,retention_threshold).await;

        for(key, version_index) in versions_to_remove{
            if let Err(e) = self.remove_version(&key,version_index).await{
                warn!("Failed to remove version for key {:?}: {}",key,e);
            }else{
                stats.versions_removed+=1;
            }
        }

        stats.duration_ms = start_time.elapsed().as_millis() as u64;
        info!("Garbage collection completed {:?}",stats);

        *self.is_collecting.lock().await = false;
        Ok(stats)
    }

    async fn get_oldest_active_snapshot(&self) -> VersionTimestamp{
        self.mvcc_storage.get_oldest_snapshot_timestamp()
    }

    async fn calculate_retention_threshold(&self)->VersionTimestamp{
        let now = VersionTimestamp::now();
        let retention_micros = self.config.version_retention_secs*1_000_000;

        if now.as_u64()> retention_micros{
            VersionTimestamp::from_u64(now.as_u64()-retention_micros)
        }else{
            VersionTimestamp::from_u64(0)
        }
    }

    async fn find_obsolete_versions(&self,oldest_snapshot_ts:VersionTimestamp,
    retention_threshold:VersionTimestamp) -> HashMap<Vec<u8>,usize>{
        let mut obsolete_versions = HashMap::new();
        let version_store = self.mvcc_storage.get_version_store();

        for(key,versions) in version_store.iter(){
            if versions.len() <= self.config.min_versions_to_keep as usize{
                continue;
            }

            for (index,version) in versions.iter().enumerate(){
                if version.created_ts <retention_threshold && version.created_ts<oldest_snapshot_ts && versions.len()> self.config.min_versions_to_keep as usize {
                    obsolete_versions.insert(key.clone(),index);
                    break;
                }
            }
        }
        obsolete_versions
    }

    async fn remove_version(&self,key:&[u8],version_index:usize)->Result<()>{
        let mut version_store = self.mvcc_storage.get_version_store_mut();

        if let Some(versions) = version_store.get_mut(key){
            if version_index <versions.len(){
                let removed_version = versions.remove(version_index);
                debug!("Removed version for key {:?} created at {}", key,removed_version.created_ts.as_u64());
            }
        }
        Ok(())
    }
}

pub struct BackgroundGc{
    gc:Arc<GarbageCollector>,
    interval_secs: u64,
    stopped:Mutex<bool>,
}

impl BackgroundGc{
    pub fn new(gc:Arc<GarbageCollector>,interval_secs:u64) -> Self{
        Self{
           gc,
           interval_secs,
           stopped:Mutex::new(false), 
        }
    }

    pub async fn start(&self)->Result<()>{
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(self.interval_secs));

        loop{
            interval.tick().await;

            if *self.stopped.lock().await{
                break;
            }

            if let Err(e) = self.gc.run_garbage_collection().await{
                warn!("Background garbage collection failed: {}",e);
            }
        }
        Ok(())
    }

    pub async fn stop(&self){
        *self.stopped.lock().await=true;
    }
}