use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use ring::{aead, rand};
use rust_db_core::{
    AccessDecision, AuditLogEntry, Database, DbError, EncryptionConfig, OperationType, Permission,
    Principal, Resource, Result, SecurityContext,
};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Security layer that wraps any Database implementation with authentication,
/// authorization, encryption, and audit logging
pub struct SecurityLayer<D> {
    inner: D,
    users: Arc<RwLock<HashMap<String, StoredUser>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
    audit_log: Arc<RwLock<Vec<AuditLogEntry>>>,
    encryption_config: EncryptionConfig,
    encryption_key: Option<aead::LessSafeKey>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct StoredUser {
    principal: Principal,
    password_hash: String,
    created_at: u64,
    last_login: Option<u64>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct Role {
    name: String,
    permissions: Vec<Permission>,
    description: String,
}

impl<D: Database> SecurityLayer<D> {
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
            encryption_config: EncryptionConfig::default(),
            encryption_key: None,
        }
    }

    pub fn with_encryption(mut self, config: EncryptionConfig, key_material: &[u8]) -> Result<Self> {
        if config.enabled {
            // Derive encryption key from key material
            let unbound_key = aead::UnboundKey::new(&aead::AES_256_GCM, key_material)
                .map_err(|e| DbError::Encryption(format!("Failed to create encryption key: {:?}", e)))?;
            self.encryption_key = Some(aead::LessSafeKey::new(unbound_key));
            self.encryption_config = config;
        }
        Ok(self)
    }

    /// Add a user with hashed password
    pub async fn add_user(&self, username: &str, password: &str, roles: Vec<String>) -> Result<Principal> {
        // Hash password using Argon2
        // Generate salt from username to ensure deterministic but unique salts
        let salt_input = format!("{}_{}", username, "rustdb_salt_v1");
        let salt = SaltString::encode_b64(&salt_input.as_bytes()[..16])
            .map_err(|e| DbError::Security(format!("Failed to create salt: {}", e)))?;
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| DbError::Security(format!("Password hashing failed: {}", e)))?
            .to_string();

        // Get permissions from roles
        let role_permissions = self.get_role_permissions(&roles).await?;

        let principal = Principal {
            id: username.to_string(),
            name: username.to_string(),
            roles: roles.clone(),
            permissions: role_permissions,
        };

        let stored_user = StoredUser {
            principal: principal.clone(),
            password_hash,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_login: None,
        };

        let mut users = self.users.write().await;
        users.insert(username.to_string(), stored_user);

        log::info!("User created: {}", username);
        Ok(principal)
    }

    /// Authenticate a user
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<Principal> {
        let mut users = self.users.write().await;

        let stored_user = users
            .get_mut(username)
            .ok_or_else(|| DbError::AccessDenied("Invalid credentials".to_string()))?;

        // Verify password
        let parsed_hash = PasswordHash::new(&stored_user.password_hash)
            .map_err(|e| DbError::Security(format!("Invalid password hash: {}", e)))?;

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| DbError::AccessDenied("Invalid credentials".to_string()))?;

        // Update last login
        stored_user.last_login = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        log::info!("User authenticated: {}", username);
        Ok(stored_user.principal.clone())
    }

    /// Remove a user
    pub async fn remove_user(&self, user_id: &str) -> Result<()> {
        let mut users = self.users.write().await;
        users
            .remove(user_id)
            .ok_or_else(|| DbError::Query(format!("User not found: {}", user_id)))?;

        log::info!("User removed: {}", user_id);
        Ok(())
    }

    /// Add a role with permissions
    pub async fn add_role(&self, name: String, permissions: Vec<Permission>, description: String) -> Result<()> {
        let role = Role {
            name: name.clone(),
            permissions,
            description,
        };

        let mut roles = self.roles.write().await;
        roles.insert(name.clone(), role);

        log::info!("Role created: {}", name);
        Ok(())
    }

    /// Get permissions for a list of roles
    async fn get_role_permissions(&self, role_names: &[String]) -> Result<Vec<Permission>> {
        let roles = self.roles.read().await;
        let mut permissions = Vec::new();

        for role_name in role_names {
            if let Some(role) = roles.get(role_name) {
                permissions.extend(role.permissions.clone());
            }
        }

        Ok(permissions)
    }

    /// Check access based on security context
    async fn check_access(&self, context: &SecurityContext) -> Result<AccessDecision> {
        match &context.principal {
            Some(principal) => {
                // Admin has all permissions
                if principal.has_permission(&Permission::Admin) {
                    return Ok(AccessDecision::Allow);
                }

                // Check specific permissions based on operation and resource
                let required_permission = self.required_permission(context);

                if principal.has_permission(&required_permission) {
                    Ok(AccessDecision::Allow)
                } else {
                    Ok(AccessDecision::Deny(format!(
                        "Missing permission: {:?}",
                        required_permission
                    )))
                }
            }
            None => Ok(AccessDecision::Deny("Not authenticated".to_string())),
        }
    }

    /// Determine required permission for an operation
    fn required_permission(&self, context: &SecurityContext) -> Permission {
        match (&context.operation, &context.resource) {
            (OperationType::Read, Resource::Table(name)) => Permission::ReadTable(name.clone()),
            (OperationType::Write, Resource::Table(name)) => Permission::WriteTable(name.clone()),
            (OperationType::Delete, Resource::Table(name)) => Permission::DeleteTable(name.clone()),
            (OperationType::Execute, Resource::Procedure(name)) => {
                Permission::ExecuteProcedure(name.clone())
            }
            (OperationType::Create, Resource::Table(_)) => Permission::CreateTable,
            (OperationType::Drop, Resource::Table(_)) => Permission::DropTable,
            _ => Permission::Admin, // Require admin for unknown operations
        }
    }

    /// Log an operation to the audit log
    async fn log_operation(&self, context: &SecurityContext, success: bool, details: &str) -> Result<()> {
        let entry = AuditLogEntry::new(
            context.principal.as_ref().map(|p| p.id.clone()),
            format!("{:?}", context.operation),
            format!("{:?}", context.resource),
            success,
            details.to_string(),
        );

        let mut log = self.audit_log.write().await;
        log.push(entry);

        // Keep only last 10,000 entries
        if log.len() > 10000 {
            let drain_count = log.len() - 10000;
            log.drain(0..drain_count);
        }

        Ok(())
    }

    /// Get audit log entries
    pub async fn get_audit_log(&self) -> Vec<AuditLogEntry> {
        let log = self.audit_log.read().await;
        log.clone()
    }

    /// Encrypt data
    fn encrypt<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        if !self.encryption_config.enabled {
            return bincode::serialize(value)
                .map_err(|e| DbError::Serialization(e.to_string()));
        }

        let key = self
            .encryption_key
            .as_ref()
            .ok_or_else(|| DbError::Encryption("Encryption key not set".to_string()))?;

        // Serialize value
        let plaintext = bincode::serialize(value)
            .map_err(|e| DbError::Serialization(e.to_string()))?;

        // Generate random nonce
        let mut nonce_bytes = [0u8; 12];
        use ring::rand::SecureRandom;
        let rng = rand::SystemRandom::new();
        rng.fill(&mut nonce_bytes)
            .map_err(|e| DbError::Encryption(format!("Failed to generate nonce: {:?}", e)))?;

        let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);

        // Encrypt
        let mut ciphertext = plaintext;
        key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut ciphertext)
            .map_err(|e| DbError::Encryption(format!("Encryption failed: {:?}", e)))?;

        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    /// Decrypt data
    fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        if !self.encryption_config.enabled {
            return Ok(encrypted.to_vec());
        }

        let key = self
            .encryption_key
            .as_ref()
            .ok_or_else(|| DbError::Encryption("Encryption key not set".to_string()))?;

        // Extract nonce (first 12 bytes)
        if encrypted.len() < 12 {
            return Err(DbError::Encryption("Invalid encrypted data".to_string()));
        }

        let (nonce_bytes, ciphertext) = encrypted.split_at(12);
        let nonce = aead::Nonce::try_assume_unique_for_key(nonce_bytes)
            .map_err(|_| DbError::Encryption("Invalid nonce".to_string()))?;

        // Decrypt
        let mut plaintext = ciphertext.to_vec();
        let decrypted = key
            .open_in_place(nonce, aead::Aad::empty(), &mut plaintext)
            .map_err(|e| DbError::Encryption(format!("Decryption failed: {:?}", e)))?;

        Ok(decrypted.to_vec())
    }

    /// Initialize with default roles
    pub async fn initialize_default_roles(&self) -> Result<()> {
        // Admin role
        self.add_role(
            "admin".to_string(),
            vec![Permission::Admin],
            "Full administrative access".to_string(),
        )
        .await?;

        // Read-only role
        self.add_role(
            "reader".to_string(),
            vec![
                Permission::ReadTable("*".to_string()),
            ],
            "Read-only access to all tables".to_string(),
        )
        .await?;

        // Writer role
        self.add_role(
            "writer".to_string(),
            vec![
                Permission::ReadTable("*".to_string()),
                Permission::WriteTable("*".to_string()),
            ],
            "Read and write access to all tables".to_string(),
        )
        .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<D: Database> Database for SecurityLayer<D> {
    async fn insert<T: Serialize + Send + Sync>(&self, key: &[u8], value: &T) -> Result<()> {
        let context = SecurityContext::new(
            None, // TODO: Get from thread-local or request context
            OperationType::Write,
            Resource::Table("unknown".to_string()),
        );

        let decision = self.check_access(&context).await?;
        match decision {
            AccessDecision::Allow | AccessDecision::AllowWithAudit => {
                // Encrypt if enabled
                let encrypted_value = self.encrypt(value)?;

                self.inner.insert(key, &encrypted_value).await?;
                self.log_operation(&context, true, "Insert successful")
                    .await?;
                Ok(())
            }
            AccessDecision::Deny(reason) => {
                self.log_operation(&context, false, &format!("Access denied: {}", reason))
                    .await?;
                Err(DbError::AccessDenied(reason))
            }
        }
    }

    async fn get<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        let context = SecurityContext::new(
            None,
            OperationType::Read,
            Resource::Table("unknown".to_string()),
        );

        let decision = self.check_access(&context).await?;
        match decision {
            AccessDecision::Allow | AccessDecision::AllowWithAudit => {
                let encrypted_data = self.inner.get::<Vec<u8>>(key).await?;

                self.log_operation(&context, true, "Read successful").await?;

                match encrypted_data {
                    Some(data) => {
                        let decrypted = self.decrypt(&data)?;
                        let result: T = bincode::deserialize(&decrypted)
                            .map_err(|e| DbError::Serialization(e.to_string()))?;
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            }
            AccessDecision::Deny(reason) => {
                self.log_operation(&context, false, &format!("Access denied: {}", reason))
                    .await?;
                Err(DbError::AccessDenied(reason))
            }
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let context = SecurityContext::new(
            None,
            OperationType::Delete,
            Resource::Table("unknown".to_string()),
        );

        let decision = self.check_access(&context).await?;
        match decision {
            AccessDecision::Allow | AccessDecision::AllowWithAudit => {
                self.inner.delete(key).await?;
                self.log_operation(&context, true, "Delete successful")
                    .await?;
                Ok(())
            }
            AccessDecision::Deny(reason) => {
                self.log_operation(&context, false, &format!("Access denied: {}", reason))
                    .await?;
                Err(DbError::AccessDenied(reason))
            }
        }
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let context = SecurityContext::new(
            None,
            OperationType::Read,
            Resource::Table("unknown".to_string()),
        );

        let decision = self.check_access(&context).await?;
        match decision {
            AccessDecision::Allow | AccessDecision::AllowWithAudit => {
                let results = self.inner.scan(prefix).await?;
                self.log_operation(&context, true, &format!("Scan returned {} items", results.len()))
                    .await?;
                Ok(results)
            }
            AccessDecision::Deny(reason) => {
                self.log_operation(&context, false, &format!("Access denied: {}", reason))
                    .await?;
                Err(DbError::AccessDenied(reason))
            }
        }
    }
}
