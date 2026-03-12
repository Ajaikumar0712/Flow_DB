//! Authentication and authorization layer for FlowDB.
//!
//! Provides:
//!   - User management: CREATE USER, DROP USER, SET PASSWORD
//!   - Role-based access control: GRANT, REVOKE permissions on tables
//!   - Session authentication: AUTH <user> <password>
//!   - Permission checking against table operations

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::FlowError;

// ── Permission types ──────────────────────────────────────────────────────

/// Operations that can be granted or revoked on a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Op {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

impl Op {
    /// Parse an operation name (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "select" | "get" | "read"  => Some(Op::Select),
            "insert" | "put" | "write" => Some(Op::Insert),
            "update" | "set"           => Some(Op::Update),
            "delete" | "del"           => Some(Op::Delete),
            "all" | "*"                => Some(Op::All),
            _                          => None,
        }
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Select => write!(f, "SELECT"),
            Op::Insert => write!(f, "INSERT"),
            Op::Update => write!(f, "UPDATE"),
            Op::Delete => write!(f, "DELETE"),
            Op::All    => write!(f, "ALL"),
        }
    }
}

// ── Table permission ──────────────────────────────────────────────────────

/// A grant of one or more operations on a specific table (or all tables).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePermission {
    /// `None` means all tables.
    pub table: Option<String>,
    pub ops: HashSet<Op>,
}

// ── User ─────────────────────────────────────────────────────────────────

/// A database user with credentials and fine-grained table permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    /// Hex-encoded SHA-256 of `salt + ":" + password`.
    pub password_hash: String,
    pub salt: String,
    /// Admins bypass all permission checks.
    pub is_admin: bool,
    pub permissions: Vec<TablePermission>,
}

impl User {
    /// Create a new user with a hashed password.
    pub fn new(username: impl Into<String>, password: &str, is_admin: bool) -> Self {
        let salt = generate_salt();
        let password_hash = hash_password(&salt, password);
        Self {
            username: username.into(),
            password_hash,
            salt,
            is_admin,
            permissions: Vec::new(),
        }
    }

    /// Verify a plain-text password against the stored hash.
    pub fn verify_password(&self, password: &str) -> bool {
        self.password_hash == hash_password(&self.salt, password)
    }

    /// Check whether this user can perform `op` on `table`.
    pub fn can(&self, op: &Op, table: &str) -> bool {
        if self.is_admin {
            return true;
        }
        for perm in &self.permissions {
            let table_match = perm.table.as_deref().map_or(true, |t| t == table);
            if table_match && (perm.ops.contains(op) || perm.ops.contains(&Op::All)) {
                return true;
            }
        }
        false
    }
}

// ── AuthManager ──────────────────────────────────────────────────────────

/// Thread-safe manager for users, passwords, and permissions.
pub struct AuthManager {
    users: RwLock<HashMap<String, User>>,
    /// When `false` all operations are allowed (open / single-user mode).
    pub auth_required: bool,
}

impl AuthManager {
    /// Open mode — no auth enforcement. Every client is treated as an admin.
    pub fn open() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            auth_required: false,
        }
    }

    /// Secured mode — requires authentication. Creates a root admin user.
    pub fn secured(admin_user: &str, admin_password: &str) -> Self {
        let mut users = HashMap::new();
        users.insert(
            admin_user.to_string(),
            User::new(admin_user, admin_password, true),
        );
        Self {
            users: RwLock::new(users),
            auth_required: true,
        }
    }

    /// Authenticate `username` + `password`.
    /// Returns the `User` on success; in open mode returns a synthetic admin.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<User, FlowError> {
        if !self.auth_required {
            return Ok(User::new("anonymous", "", true));
        }
        let users = self.users.read().unwrap();
        let user = users.get(username).ok_or_else(|| {
            FlowError::auth(format!("user `{username}` not found"))
        })?;
        if !user.verify_password(password) {
            return Err(FlowError::auth("invalid password"));
        }
        Ok(user.clone())
    }

    /// CREATE USER — admin only.
    pub fn create_user(
        &self,
        acting: &User,
        username: &str,
        password: &str,
        is_admin: bool,
    ) -> Result<(), FlowError> {
        if !acting.is_admin {
            return Err(FlowError::auth("only admins can create users"));
        }
        let mut users = self.users.write().unwrap();
        if users.contains_key(username) {
            return Err(FlowError::auth(format!("user `{username}` already exists")));
        }
        users.insert(username.to_string(), User::new(username, password, is_admin));
        Ok(())
    }

    /// DROP USER — admin only.
    pub fn drop_user(&self, acting: &User, username: &str) -> Result<(), FlowError> {
        if !acting.is_admin {
            return Err(FlowError::auth("only admins can drop users"));
        }
        let mut users = self.users.write().unwrap();
        if users.remove(username).is_none() {
            return Err(FlowError::auth(format!("user `{username}` not found")));
        }
        Ok(())
    }

    /// GRANT op ON table TO user — admin only.
    /// Pass `table = None` to grant on ALL tables.
    pub fn grant(
        &self,
        acting: &User,
        target_user: &str,
        op: Op,
        table: Option<String>,
    ) -> Result<(), FlowError> {
        if !acting.is_admin {
            return Err(FlowError::auth("only admins can grant permissions"));
        }
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(target_user).ok_or_else(|| {
            FlowError::auth(format!("user `{target_user}` not found"))
        })?;
        if let Some(perm) = user.permissions.iter_mut().find(|p| p.table == table) {
            perm.ops.insert(op);
        } else {
            let mut ops = HashSet::new();
            ops.insert(op);
            user.permissions.push(TablePermission { table, ops });
        }
        Ok(())
    }

    /// REVOKE op ON table FROM user — admin only.
    pub fn revoke(
        &self,
        acting: &User,
        target_user: &str,
        op: &Op,
        table: Option<&str>,
    ) -> Result<(), FlowError> {
        if !acting.is_admin {
            return Err(FlowError::auth("only admins can revoke permissions"));
        }
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(target_user).ok_or_else(|| {
            FlowError::auth(format!("user `{target_user}` not found"))
        })?;
        for perm in &mut user.permissions {
            if perm.table.as_deref() == table {
                perm.ops.remove(op);
            }
        }
        Ok(())
    }

    /// SHOW USERS — returns `(username, is_admin)` pairs.
    pub fn list_users(&self) -> Vec<(String, bool)> {
        let users = self.users.read().unwrap();
        let mut list: Vec<(String, bool)> = users
            .values()
            .map(|u| (u.username.clone(), u.is_admin))
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    /// Change a user's password. Admins can change anyone's; users their own.
    pub fn set_password(
        &self,
        acting: &User,
        target: &str,
        new_password: &str,
    ) -> Result<(), FlowError> {
        if !acting.is_admin && acting.username != target {
            return Err(FlowError::auth("cannot change another user's password"));
        }
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(target).ok_or_else(|| {
            FlowError::auth(format!("user `{target}` not found"))
        })?;
        user.salt = generate_salt();
        user.password_hash = hash_password(&user.salt, new_password);
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Generate a random salt using a UUID.
fn generate_salt() -> String {
    uuid::Uuid::new_v4().to_string().replace('-', "")
}

/// Compute SHA-256(salt + ":" + password), returned as a lowercase hex string.
fn hash_password(salt: &str, password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(salt.as_bytes());
    h.update(b":");
    h.update(password.as_bytes());
    h.finalize().iter().map(|b| format!("{:02x}", b)).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_auth() {
        let mgr = AuthManager::secured("root", "secret");
        let root = mgr.authenticate("root", "secret").unwrap();
        assert!(root.is_admin);
        assert!(mgr.authenticate("root", "wrong").is_err());
    }

    #[test]
    fn test_grant_revoke() {
        let mgr = AuthManager::secured("root", "pass");
        let root = mgr.authenticate("root", "pass").unwrap();

        mgr.create_user(&root, "alice", "pw", false).unwrap();
        mgr.grant(&root, "alice", Op::Select, Some("users".into())).unwrap();

        let alice = {
            let users = mgr.users.read().unwrap();
            users["alice"].clone()
        };
        assert!(alice.can(&Op::Select, "users"));
        assert!(!alice.can(&Op::Insert, "users"));

        mgr.revoke(&root, "alice", &Op::Select, Some("users")).unwrap();
        let alice2 = {
            let users = mgr.users.read().unwrap();
            users["alice"].clone()
        };
        assert!(!alice2.can(&Op::Select, "users"));
    }

    #[test]
    fn test_open_mode_allows_all() {
        let mgr = AuthManager::open();
        let user = mgr.authenticate("anyone", "anything").unwrap();
        assert!(user.is_admin);
    }
}
