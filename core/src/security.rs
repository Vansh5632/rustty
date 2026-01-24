use serde::{Serialize, Deserialize};

// Security Principal (user/role)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    pub name: String,
    pub roles: Vec<String>,
    pub permissions: Vec<Permission>,
}

impl Principal {
    pub fn has_permission(&self, required: &Permission) -> bool {
        self.permissions.contains(required)
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

// Permission types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Permission {
    ReadTable(String),
    WriteTable(String),
    DeleteTable(String),
    CreateTable,
    DropTable,
    ExecuteProcedure(String),
    ManageUsers,
    Admin,
}

// Security context for operations
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub principal: Option<Principal>,
    pub operation: OperationType,
    pub resource: Resource,
    pub timestamp: u64,
}

impl SecurityContext {
    pub fn new(principal: Option<Principal>, operation: OperationType, resource: Resource) -> Self {
        Self {
            principal,
            operation,
            resource,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn has_permission(&self, required: &Permission) -> bool {
        self.principal
            .as_ref()
            .map(|p| p.has_permission(required))
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Execute,
    Create,
    Drop,
}

#[derive(Debug, Clone)]
pub enum Resource {
    Table(String),
    Procedure(String),
    Database,
    User(String),
}

// Access Control Decision
#[derive(Debug, Clone)]
pub enum AccessDecision {
    Allow,
    Deny(String), // Reason for denial
    AllowWithAudit, // Allow but log for audit
}

// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub id: String,
    pub timestamp: u64,
    pub principal_id: Option<String>,
    pub operation: String,
    pub resource: String,
    pub success: bool,
    pub details: String,
    pub ip_address: Option<String>,
}

impl AuditLogEntry {
    pub fn new(
        principal_id: Option<String>,
        operation: String,
        resource: String,
        success: bool,
        details: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            principal_id,
            operation,
            resource,
            success,
            details,
            ip_address: None,
        }
    }
}

// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub enabled: bool,
    pub algorithm: EncryptionAlgorithm,
    pub key_rotation_days: u32,
    pub field_level_encryption: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    ChaCha20Poly1305,
    XChaCha20Poly1305,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_rotation_days: 90,
            field_level_encryption: false,
        }
    }
}
