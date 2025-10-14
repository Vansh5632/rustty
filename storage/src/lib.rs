use rust_db_core::{DbError,Database,Result};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path,PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};


pub struct WriteAheadLog{
    file:BufWriter<File>,
    path:PathBuf,
}

impl WriteAheadLog{
    pub fn new(path: &Path)-> Result<Self>{
        let file = OpenOptions::new().create(true).append(true).open(path).map_err(|e| DbError::Storage(e.to_string()))?;

        Ok(WriteAheadLog { file: BufWriter::new(file), path: path.to_path_buf(), })
    }

    pub fn write_entry(&mut self,key:&[u8],value:&[u8]) -> Result<()>{
        let entry = WalEntry{
            key:key.to_vec(),
            value:value.to_vec(),
            timestamp:SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64,
        };

        bincode::serialize_into(&mut self.file,&entry).map_err(|e| DbError::Storage(e.to_string()))?;
        self.file.flush().map_err(|e| DbError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[derive(Serialize,Deserialize)]

struct WalEntry{
    key:Vec<u8>,
    value:Vec<u8>,
    timestamp:u64,
}

pub struct MemTable{
    data:BTreeMap<Vec<u8>,Vec<u8>>,
    size:usize,
}

impl MemTable{
    pub fn new() -> Self{
        MemTable { data: BTreeMap::new(), size: 0, }
    }

    pub fn insert(&mut self,key:Vec<u8>,value:Vec<u8>){
        self.size+=key.len()+value.len();
        self.data.insert(key, value);
    }

    pub fn get(&self,key:&[u8])->Option<Vec<u8>>{
        self.data.get(key).cloned()
    }

    pub fn scan(&self,prefix:&[u8])-> Vec<(Vec<u8>,Vec<u8>)>{
        self.data.range(prefix.to_vec()..).take_while(|(k,_)|K.starts_with(prefix)).map(|(k,v)| (k.clone(),v.clone())).collect()
    }

    pub fn should_flush(&self)->bool{
        self.size> *FLUSH_THRESHOLD
    }

    pub fn len(&self)->usize{
        self.data.len()
    }
}

pub struct SSTable{
    path:PathBuf,
    data:Mmap,
}

impl SSTable{
    pub fn from_memtable(path:&Path,memtable:&MemTable)-> Result<Self>{
        let mut file = OpenOptions::new().create(true).write(true).open(path).map_err(|e| DbError::Storage(e.to_string()))?;
        
        for(key,value) in &memtable.data{
            let entry = (key,value);
            bincode::serialize_into(&mut file,&entry).map_err(|e| DbError::Serialization(e.to_string()))?;
        }
        file.flush().map_err(|e| DbError::Storage(e.to_string()))?;

        let file = OpenOptions::new().read(true).open(path).map_err(|e| DbError::Storage(e.to_string()))?;

        unsafe{
            let data= Mmap::map(&file).map_err(|e| DbError::Storage(e.to_string()))?;
        }
   
    }
}


