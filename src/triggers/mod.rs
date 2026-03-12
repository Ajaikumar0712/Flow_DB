//! Event-driven trigger system.
//!
//! Triggers fire after a matching mutation and execute a FlowQL query.
//!
//! Syntax:
//!   TRIGGER <name> WHEN PUT|SET|DEL <table> DO <query>
//!   DROP TRIGGER <name>
//!   SHOW TRIGGERS

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::FlowError;

// ── Trigger event type ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Put,
    Set,
    Del,
}

impl TriggerEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerEvent::Put => "PUT",
            TriggerEvent::Set => "SET",
            TriggerEvent::Del => "DEL",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "PUT" => Some(Self::Put),
            "SET" => Some(Self::Set),
            "DEL" => Some(Self::Del),
            _ => None,
        }
    }
}

// ── Trigger definition ────────────────────────────────────────────────────

/// A single trigger definition stored in the TriggerStore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    /// Unique trigger name.
    pub name: String,
    /// Event that fires this trigger.
    pub event: TriggerEvent,
    /// The table name being watched.
    pub table: String,
    /// The FlowQL query to execute when fired.
    pub do_query: String,
}

// ── Trigger store ─────────────────────────────────────────────────────────

/// Thread-safe registry of all defined triggers.
pub struct TriggerStore {
    triggers: RwLock<HashMap<String, Trigger>>,
}

impl TriggerStore {
    pub fn new() -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new trigger. Fails if a trigger with the same name already exists.
    pub fn create(&self, trigger: Trigger) -> Result<(), FlowError> {
        let mut g = self.triggers.write().map_err(|_| FlowError::Io("lock poisoned".into()))?;
        if g.contains_key(&trigger.name) {
            return Err(FlowError::Parse(format!("trigger `{}` already exists", trigger.name)));
        }
        g.insert(trigger.name.clone(), trigger);
        Ok(())
    }

    /// Remove a trigger by name.
    pub fn drop_trigger(&self, name: &str) -> Result<(), FlowError> {
        let mut g = self.triggers.write().map_err(|_| FlowError::Io("lock poisoned".into()))?;
        if g.remove(name).is_none() {
            return Err(FlowError::Parse(format!("trigger `{name}` not found")));
        }
        Ok(())
    }

    /// List all registered triggers.
    pub fn list(&self) -> Vec<Trigger> {
        self.triggers
            .read()
            .map(|g| g.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Return all triggers that match a given event + table combination.
    pub fn get_matching(&self, event: &TriggerEvent, table: &str) -> Vec<Trigger> {
        self.triggers
            .read()
            .map(|g| {
                g.values()
                    .filter(|t| &t.event == event && t.table == table)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trigger(name: &str, event: TriggerEvent, table: &str) -> Trigger {
        Trigger {
            name: name.into(),
            event,
            table: table.into(),
            do_query: format!("PUT logs {{ msg: \"{}\" }}", name),
        }
    }

    #[test]
    fn test_create_and_list() {
        let store = TriggerStore::new();
        store.create(make_trigger("t1", TriggerEvent::Put, "orders")).unwrap();
        store.create(make_trigger("t2", TriggerEvent::Del, "users")).unwrap();
        assert_eq!(store.list().len(), 2);
    }

    #[test]
    fn test_duplicate_name_errors() {
        let store = TriggerStore::new();
        store.create(make_trigger("t1", TriggerEvent::Put, "orders")).unwrap();
        assert!(store.create(make_trigger("t1", TriggerEvent::Set, "orders")).is_err());
    }

    #[test]
    fn test_drop_trigger() {
        let store = TriggerStore::new();
        store.create(make_trigger("t1", TriggerEvent::Put, "orders")).unwrap();
        store.drop_trigger("t1").unwrap();
        assert_eq!(store.list().len(), 0);
        assert!(store.drop_trigger("t1").is_err());
    }

    #[test]
    fn test_get_matching() {
        let store = TriggerStore::new();
        store.create(make_trigger("t1", TriggerEvent::Put, "orders")).unwrap();
        store.create(make_trigger("t2", TriggerEvent::Put, "users")).unwrap();
        store.create(make_trigger("t3", TriggerEvent::Del, "orders")).unwrap();
        let m = store.get_matching(&TriggerEvent::Put, "orders");
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].name, "t1");
    }
}
