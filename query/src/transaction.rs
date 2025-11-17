use rust_db_core::{
    MvccDatabase, Transaction, TransactionContext, DbError, Result, Schema, FieldAccess,
    Filter as FieldFilter, Operator as FilterOperator, Value,
};
use async_trait::async_trait;
use std::marker::PhantomData;

pub struct TransactionalQueryBuilder<'a, T, D> {
    db: &'a D,
    transaction: &'a Transaction,
    filters: Vec<FieldFilter>,
    limit: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<'a, T, D> TransactionalQueryBuilder<'a, T, D> 
where 
    T: Schema + FieldAccess + serde::de::DeserializeOwned + Send + Sync,
    D: MvccDatabase,
{
    pub fn new(db: &'a D, transaction: &'a Transaction) -> Self {
        Self {
            db,
            transaction,
            filters: Vec::new(),
            limit: None,
            _phantom: PhantomData,
        }
    }
    
    pub fn filter(mut self, field: &str, operator: FilterOperator, value: Value) -> Self {
        self.filters.push(FieldFilter {
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
        
        // Use transaction-aware scan
        let records = self.db.scan_for_transaction(prefix, self.transaction).await?;
        
        let mut results = Vec::new();
        
        for (key, value) in records {
            // Skip tombstones
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
    
    // Apply filters to a deserialized item using FieldAccess
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
                FilterOperator::Eq => field_value == filter.value,
                FilterOperator::Ne => field_value != filter.value,
                FilterOperator::Gt => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a > b,
                    (Value::Float(a), Value::Float(b)) => a > b,
                    _ => false,
                },
                FilterOperator::Lt => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a < b,
                    (Value::Float(a), Value::Float(b)) => a < b,
                    _ => false,
                },
                FilterOperator::Gte => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a >= b,
                    (Value::Float(a), Value::Float(b)) => a >= b,
                    _ => false,
                },
                FilterOperator::Lte => match (&field_value, &filter.value) {
                    (Value::Int(a), Value::Int(b)) => a <= b,
                    (Value::Float(a), Value::Float(b)) => a <= b,
                    _ => false,
                },
                FilterOperator::Contains => match (&field_value, &filter.value) {
                    (Value::String(a), Value::String(b)) => a.contains(b),
                    _ => false,
                },
                FilterOperator::StartsWith => match (&field_value, &filter.value) {
                    (Value::String(a), Value::String(b)) => a.starts_with(b),
                    _ => false,
                },
                FilterOperator::EndsWith => match (&field_value, &filter.value) {
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

// Extension trait for transactional queries
pub trait TransactionalQueryExt: MvccDatabase {
    fn query_within_transaction<T: Schema + FieldAccess>(
        &self, 
        transaction: &Transaction
    ) -> TransactionalQueryBuilder<T, Self> 
    where 
        Self: Sized 
    {
        TransactionalQueryBuilder::new(self, transaction)
    }
}

impl<D: MvccDatabase> TransactionalQueryExt for D {}