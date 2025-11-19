use rust_db_core::{DbError, Database, Result, Schema, Filter, Operator, Value, FieldAccess};
use std::marker::PhantomData;

mod transaction;
pub use transaction::{TransactionalQueryBuilder, TransactionalQueryExt};

pub struct QueryEngine<D> {
    db: D,
}

impl<D: Database> QueryEngine<D> {
    pub fn new(db: D) -> Self {
        Self { db }
    }
    
    pub fn query<T: Schema + serde::de::DeserializeOwned + Send + Sync + FieldAccess>(&self) -> QueryBuilder<T, D> {
        QueryBuilder::new(&self.db)
    }
}

pub struct QueryBuilder<'a, T, D> {
    db: &'a D,
    filters: Vec<Filter>,
    limit: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<'a, T, D> QueryBuilder<'a, T, D> 
where 
    T: Schema + serde::de::DeserializeOwned + Send + Sync + FieldAccess,
    D: Database,
{
    pub fn new(db: &'a D) -> Self {
        Self {
            db,
            filters: Vec::new(),
            limit: None,
            _phantom: PhantomData,
        }
    }
    
    pub fn filter(mut self, field: &str, operator: Operator, value: Value) -> Self {
        self.filters.push(Filter {
            field: field.to_string(),
            operator,
            value,
        });
        self
    }
    
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    
    pub async fn execute(self) -> Result<Vec<T>> {
        let table_name = T::table_name();
        let prefix = table_name.as_bytes();
        
        // Scan all records for this table
        let records = self.db.scan(prefix).await?;
        
        let mut results = Vec::new();
        
        for (_key, value) in records {
            // Skip tombstones (empty values used for deletion)
            if value.is_empty() {
                continue;
            }
            
            let item: T = bincode::deserialize(&value)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
                
            // Apply filters
            if self.apply_filters(&item) {
                results.push(item);
                
                // Apply limit
                if let Some(limit) = self.limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }
        
        Ok(results)
    }
    
    fn apply_filters(&self, item: &T) -> bool {
        // Check all filters - item must pass ALL filters (AND logic)
        for filter in &self.filters {
            // Get the field value from the item
            let field_value = match item.get_field(&filter.field) {
                Some(val) => val,
                None => return false, // Field doesn't exist
            };
            
            // Apply the operator
            let matches = match &filter.operator {
                Operator::Eq => field_value == filter.value,
                Operator::Ne => field_value != filter.value,
                Operator::Gt => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a > b,
                    (Value::Float(a), Value::Float(b)) => a > b,
                    _ => false,
                },
                Operator::Lt => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a < b,
                    (Value::Float(a), Value::Float(b)) => a < b,
                    _ => false,
                },
                Operator::Gte => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a >= b,
                    (Value::Float(a), Value::Float(b)) => a >= b,
                    _ => false,
                },
                Operator::Lte => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a <= b,
                    (Value::Float(a), Value::Float(b)) => a <= b,
                    _ => false,
                },
                Operator::Contains => match (&field_value, &filter.value) {
                    (Value::String(a), Value::String(b)) => a.contains(b),
                    _ => false,
                },
                Operator::StartsWith => match (&field_value, &filter.value) {
                    (Value::String(a), Value::String(b)) => a.starts_with(b),
                    _ => false,
                },
                Operator::EndsWith => match (&field_value, &filter.value) {
                    (Value::String(a), Value::String(b)) => a.ends_with(b),
                    _ => false,
                },
            };
            
            // If any filter fails, reject the item
            if !matches {
                return false;
            }
        }
        
        // All filters passed
        true
    }
}

// Extension trait to add query method to any Database
pub trait QueryExt: Database {
    fn query<T: Schema + serde::de::DeserializeOwned + Send + Sync + FieldAccess>(&self) -> QueryBuilder<T, Self> 
    where 
        Self: Sized 
    {
        QueryBuilder::new(self)
    }
}

impl<D: Database> QueryExt for D {}