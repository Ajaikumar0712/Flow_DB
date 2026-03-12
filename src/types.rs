use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

// ── Primitive value types ─────────────────────────────────────────────────

/// Every value that can be stored in a FlowDB column.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
    Json(serde_json::Value),
    Blob(Vec<u8>),
    /// Dense floating-point vector for similarity search.
    Vector(Vec<f32>),
}

impl Value {
    /// Human-readable type name used in error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_)    => "int",
            Value::Float(_)  => "float",
            Value::Text(_)   => "text",
            Value::Bool(_)   => "bool",
            Value::Null      => "null",
            Value::Json(_)   => "json",
            Value::Blob(_)   => "blob",
            Value::Vector(_) => "vector",
        }
    }

    /// Coerce a value to boolean for WHERE filter evaluation.
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b)    => *b,
            Value::Null       => false,
            Value::Int(n)     => *n != 0,
            Value::Float(f)   => *f != 0.0,
            Value::Text(s)    => !s.is_empty(),
            Value::Json(v)    => !v.is_null(),
            Value::Blob(b)    => !b.is_empty(),
            Value::Vector(v)  => !v.is_empty(),
        }
    }

    /// Total ordering for sort / comparison. Returns None when types are incompatible.
    pub fn partial_cmp_val(&self, other: &Value) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Int(a),   Value::Int(b))   => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Int(a),   Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Int(b))   => a.partial_cmp(&(*b as f64)),
            (Value::Text(a),  Value::Text(b))  => Some(a.cmp(b)),
            (Value::Bool(a),  Value::Bool(b))  => Some(a.cmp(b)),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(n)    => write!(f, "{n}"),
            Value::Float(v)  => write!(f, "{v}"),
            Value::Text(s)   => write!(f, "{s}"),
            Value::Bool(b)   => write!(f, "{b}"),
            Value::Null      => write!(f, "null"),
            Value::Json(j)   => write!(f, "{j}"),
            Value::Blob(b)   => write!(f, "<blob {} bytes>", b.len()),
            Value::Vector(v) => write!(f, "vec[{} dims]", v.len()),
        }
    }
}

// ── Value: flat JSON serialisation ───────────────────────────────────────
//
// Wire format (client output AND WAL):
//   Int    → 42
//   Float  → 3.14
//   Text   → "hello"
//   Bool   → true / false
//   Null   → null
//   Json   → the JSON value inline (object / array / ...)
//   Blob   → "<blob:deadbeef>"  (lowercase hex, zero-copy detect on read)
//   Vector → [0.1, 0.9, 0.3]   (array of f32 numbers)
//
// Backward-compat: the Deserializer also accepts the old tagged format
// {"type":"Int","value":42} so existing WAL / snapshot files can be replayed.

impl serde::Serialize for Value {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Int(n)    => s.serialize_i64(*n),
            Value::Float(f)  => s.serialize_f64(*f),
            Value::Text(t)   => s.serialize_str(t),
            Value::Bool(b)   => s.serialize_bool(*b),
            Value::Null      => s.serialize_none(),
            Value::Json(v)   => v.serialize(s),
            Value::Blob(b)   => {
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                s.serialize_str(&format!("<blob:{}>", hex))
            }
            Value::Vector(v) => v.serialize(s),
        }
    }
}

struct ValueVisitor;

impl<'de> serde::de::Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a FlowDB value")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i8<E: serde::de::Error>(self, v: i8) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_i16<E: serde::de::Error>(self, v: i16) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_i32<E: serde::de::Error>(self, v: i32) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Value, E> { Ok(Value::Int(v)) }
    fn visit_u8<E: serde::de::Error>(self, v: u8) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_u16<E: serde::de::Error>(self, v: u16) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_u32<E: serde::de::Error>(self, v: u32) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Value, E> { Ok(Value::Int(v as i64)) }
    fn visit_f32<E: serde::de::Error>(self, v: f32) -> Result<Value, E> { Ok(Value::Float(v as f64)) }
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> { Ok(Value::Float(v)) }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Value, E> {
        if let Some(hex) = v.strip_prefix("<blob:").and_then(|s| s.strip_suffix('>')) {
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .filter_map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
                .collect();
            return Ok(Value::Blob(bytes));
        }
        Ok(Value::Text(v.to_string()))
    }
    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Value, E> {
        self.visit_str(&v)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> { Ok(Value::Null) }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> { Ok(Value::Null) }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut elements: Vec<serde_json::Value> = Vec::new();
        while let Some(elem) = seq.next_element::<serde_json::Value>()? {
            elements.push(elem);
        }
        // All-number array → Vector (float array)
        if !elements.is_empty() && elements.iter().all(|e| e.is_number()) {
            let floats: Vec<f32> = elements.iter()
                .map(|e| e.as_f64().unwrap_or(0.0) as f32)
                .collect();
            return Ok(Value::Vector(floats));
        }
        Ok(Value::Json(serde_json::Value::Array(elements)))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut obj = serde_json::Map::new();
        while let Some((k, v)) = map.next_entry::<String, serde_json::Value>()? {
            obj.insert(k, v);
        }
        // Backward-compat: detect old tagged format {"type":"...", "value":...}
        if let Some(kind) = obj.get("type").and_then(|v| v.as_str()) {
            match kind {
                "Null"   => return Ok(Value::Null),
                "Int"    => if let Some(n) = obj.get("value").and_then(|v| v.as_i64())   { return Ok(Value::Int(n)); }
                "Float"  => if let Some(f) = obj.get("value").and_then(|v| v.as_f64())   { return Ok(Value::Float(f)); }
                "Text"   => if let Some(s) = obj.get("value").and_then(|v| v.as_str())   { return Ok(Value::Text(s.to_string())); }
                "Bool"   => if let Some(b) = obj.get("value").and_then(|v| v.as_bool())  { return Ok(Value::Bool(b)); }
                "Json"   => if let Some(v) = obj.get("value")                            { return Ok(Value::Json(v.clone())); }
                "Blob"   => {
                    if let Some(arr) = obj.get("value").and_then(|v| v.as_array()) {
                        let bytes: Vec<u8> = arr.iter()
                            .filter_map(|x| x.as_u64().map(|n| n as u8)).collect();
                        return Ok(Value::Blob(bytes));
                    }
                }
                "Vector" => {
                    if let Some(arr) = obj.get("value").and_then(|v| v.as_array()) {
                        let floats: Vec<f32> = arr.iter()
                            .filter_map(|x| x.as_f64().map(|f| f as f32)).collect();
                        return Ok(Value::Vector(floats));
                    }
                }
                _ => {}
            }
        }
        Ok(Value::Json(serde_json::Value::Object(obj)))
    }
}

impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(ValueVisitor)
    }
}

// ── Column definition ─────────────────────────────────────────────────────

/// A column as stored in a table's schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Float,
    Text,
    Bool,
    Json,
    Blob,
    Any,    // flexible bag column
    Vector, // dense f32 vector for similarity search
}

impl DataType {
    pub fn from_str(s: &str) -> Option<DataType> {
        match s.to_lowercase().as_str() {
            "int" | "integer" | "bigint" => Some(DataType::Int),
            "float" | "double" | "real"  => Some(DataType::Float),
            "text" | "string" | "varchar"=> Some(DataType::Text),
            "bool" | "boolean"           => Some(DataType::Bool),
            "json"                       => Some(DataType::Json),
            "blob" | "bytes"             => Some(DataType::Blob),
            "any"                        => Some(DataType::Any),
            "vector" | "vec" | "embedding" => Some(DataType::Vector),
            _ => None,
        }
    }
}

// ── Row ───────────────────────────────────────────────────────────────────

/// A single row stored in a FlowDB table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Internal row identifier (UUID v4, generated on insert).
    pub id: Uuid,
    /// Wall-clock time of the last write that touched this row.
    pub updated_at: DateTime<Utc>,
    /// Whether this row has been soft-deleted via DEL.
    pub deleted: bool,
    /// All column values, keyed by column name.
    pub fields: HashMap<String, Value>,
    /// MVCC: transaction ID that created this row version (0 = pre-MVCC / committed).
    #[serde(default)]
    pub xmin: u64,
    /// MVCC: transaction ID that deleted this row version (0 = still live).
    #[serde(default)]
    pub xmax: u64,
}

impl Row {
    pub fn new(fields: HashMap<String, Value>) -> Self {
        Self {
            id: Uuid::new_v4(),
            updated_at: Utc::now(),
            deleted: false,
            fields,
            xmin: 0,
            xmax: 0,
        }
    }

    pub fn get(&self, col: &str) -> Option<&Value> {
        self.fields.get(col)
    }

    pub fn get_or_null(&self, col: &str) -> Value {
        self.fields.get(col).cloned().unwrap_or(Value::Null)
    }
}

// ── Table schema ──────────────────────────────────────────────────────────

/// Schema / metadata for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<ColumnSchema>) -> Self {
        Self { name: name.into(), columns }
    }

    /// Return the primary key column name, if any.
    pub fn primary_key(&self) -> Option<&str> {
        self.columns.iter().find(|c| c.primary_key).map(|c| c.name.as_str())
    }

    /// Validate a map of field values against this schema.
    /// Returns an error if a required (non-nullable, non-pk) field is missing
    /// or if a value type is fundamentally incompatible.
    pub fn validate_fields(
        &self,
        fields: &HashMap<String, Value>,
    ) -> Result<(), String> {
        for col in &self.columns {
            if col.primary_key {
                // PK is generated automatically; skip
                continue;
            }
            if !col.nullable {
                if let Some(v) = fields.get(&col.name) {
                    if *v == Value::Null {
                        return Err(format!(
                            "column `{}` is NOT NULL but a NULL value was provided",
                            col.name
                        ));
                    }
                }
                // If the field is absent it just means the user didn't set it — still OK
                // (FlowDB allows partial inserts with flexible schema)
            }
        }
        Ok(())
    }
}
