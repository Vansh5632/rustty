use serde::{Serialize,Deserialize};
#[derive(Debug,Clone,Serialize,Deserialize)]

pub struct CompactionStats{
    pub sstables_merged:usize,
    pub space_reclaimed:u64,
    pub duration_ms:u64,
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum CompactionStrategy{
    Leveled{
        level_size_multiplier:u64,
        level0_sstables_trigger:usize,
    },
    Tiered{
        max_tier_size:u64,
        tier_size_multiplier:f64,
    },
    SizeTiered{
        min_sstable_size:u64,
        max_sstable_size:u64,
        bucket_count:usize,
    },
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct CompactionConfig{
    pub strategy:CompactionStrategy,
    pub enabled:bool,
    pub background_interval_secs:u64,
    pub max_sstable_per_level:usize,
}

impl Default for CompactionConfig{
    fn default()->Self{
        Self{
            strategy:CompactionStrategy::Leveled{
                level_size_multiplier:10,
                level0_sstables_trigger:4,
            },
            enabled:true,
            background_interval_secs:300,
            max_sstable_per_level:10,
        }
    }
}

//garbage collection types
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct GcConfig{
    pub enabled:bool,
    pub gc_interval_secs:u64,
    pub version_retention_secs:u64,
    pub min_versions_to_keep:u32,
}

impl Default for GcConfig{
    fn default()->Self{
        Self{
            enabled:true,
            gc_interval_secs:3600,
            version_retention_secs:86400,
            min_versions_to_keep:1,
        }
    }
}

#[derive(Debug,Clone)]
pub struct GcStats{
    pub versions_removed:usize,
    pub space_reclaimed:u64,
    pub duration_ms:u64,
}
