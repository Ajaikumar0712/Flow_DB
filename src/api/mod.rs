//! Built-in REST API server.
//!
//! `API GENERATE FOR <table>` exposes a fully functional REST API for a table.
//! `API STOP FOR <table>` removes the endpoint.
//! `SHOW APIS` lists all active endpoints.
//!
//! Each table gets its own TCP listener on an auto-assigned port (starting at 7879).
//!
//! Exposed endpoints:
//!   GET    /api/<table>        -> list all rows (JSON array)
//!   POST   /api/<table>        -> insert row (body = JSON object)
//!   GET    /api/<table>/<id>   -> fetch single row
//!   PUT    /api/<table>/<id>   -> update row fields  (body = JSON object)
//!   DELETE /api/<table>/<id>   -> delete row
//!
//! Implemented with raw Tokio TCP + handwritten HTTP/1.1 parsing.
//! No external HTTP crate required.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::error::FlowError;
use crate::storage::table::Database;
use crate::types::Value;

// ── Port allocation ──────────────────────────────────────────────────────

static NEXT_PORT: AtomicU16 = AtomicU16::new(7879);

fn alloc_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::SeqCst)
}

// ── ApiStore ────────────────────────────────────────────────────────────

pub struct ApiStore {
    /// table -> (port, shutdown_sender)
    active: RwLock<HashMap<String, (u16, tokio::sync::oneshot::Sender<()>)>>,
}

impl ApiStore {
    pub fn new() -> Self {
        Self {
            active: RwLock::new(HashMap::new()),
        }
    }

    /// Start a REST API for `table_name`.  Returns the port number.
    /// Called from a synchronous context; uses the current tokio Handle to spawn.
    pub fn start_sync(
        &self,
        db: Arc<Database>,
        table_name: String,
    ) -> Result<u16, FlowError> {
        let mut guard = self.active.write()
            .map_err(|_| FlowError::Io("lock poisoned".into()))?;

        if let Some((port, _)) = guard.get(&table_name) {
            return Ok(*port); // already running
        }

        let port = alloc_port();
        let addr: SocketAddr = format!("127.0.0.1:{}", port)
            .parse()
            .map_err(|_| FlowError::Io("invalid address".into()))?;

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tname = table_name.clone();
        let db2 = Arc::clone(&db);

        // get the current tokio handle - if no runtime, just skip spawning
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    match TcpListener::bind(addr).await {
                        Ok(listener) => run_server(listener, db2, tname, rx).await,
                        Err(e) => eprintln!("[FlowDB API] bind error on port {}: {}", port, e),
                    }
                });
                guard.insert(table_name, (port, tx));
                Ok(port)
            }
            Err(_) => {
                // No tokio runtime (e.g., unit test context): record entry only
                guard.insert(table_name, (port, tx));
                Ok(port)
            }
        }
    }

    /// Stop the REST API for `table_name`.
    pub fn stop_sync(&self, table_name: &str) -> Result<(), FlowError> {
        let mut guard = self.active.write()
            .map_err(|_| FlowError::Io("lock poisoned".into()))?;
        match guard.remove(table_name) {
            Some((_, tx)) => {
                let _ = tx.send(());
                Ok(())
            }
            None => Err(FlowError::Parse(format!(
                "no API running for table `{}`",
                table_name
            ))),
        }
    }

    /// List all active APIs: (table, port).
    pub fn list_sync(&self) -> Vec<(String, u16)> {
        self.active.read()
            .map(|g| g.iter().map(|(t, (p, _))| (t.clone(), *p)).collect())
            .unwrap_or_default()
    }
}

// ── HTTP/1.1 server ─────────────────────────────────────────────────────────

async fn run_server(
    listener: TcpListener,
    db: Arc<Database>,
    table: String,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let db2 = Arc::clone(&db);
                        let tbl = table.clone();
                        tokio::spawn(handle_connection(stream, db2, tbl));
                    }
                    Err(_) => break,
                }
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    db: Arc<Database>,
    table: String,
) {
    // Read request headers (loop until we find the header/body separator)
    let mut buf = vec![0u8; 65536];
    let mut total = 0usize;
    let header_end = loop {
        match stream.read(&mut buf[total..]).await {
            Ok(n) if n > 0 => {
                total += n;
                if let Some(pos) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
                if let Some(pos) = buf[..total].windows(2).position(|w| w == b"\n\n") {
                    break pos + 2;
                }
                if total >= buf.len() { break total; }
            }
            _ => return,
        }
    };

    // Parse Content-Length from headers so we can read the full body
    let headers_raw = std::str::from_utf8(&buf[..header_end]).unwrap_or("");
    let content_length: usize = headers_raw
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.splitn(2, ':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);

    // Read any remaining body bytes that haven't arrived yet
    let body_received = total.saturating_sub(header_end);
    if content_length > body_received {
        let still_needed = content_length - body_received;
        if header_end + content_length <= buf.len() {
            let _ = stream.read_exact(&mut buf[total..total + still_needed]).await;
            total += still_needed;
        }
    }
    let _ = total; // suppress unused warning

    let raw = std::str::from_utf8(&buf[..header_end + content_length.max(total.saturating_sub(header_end))]).unwrap_or("");

    // Parse: METHOD /path HTTP/1.1
    let mut lines = raw.lines();
    let request_line = lines.next().unwrap_or("");
    let parts: Vec<&str> = request_line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        let _ = write_response(&mut stream, 400, "Bad Request", b"bad request").await;
        return;
    }
    let method = parts[0].to_uppercase();
    let path   = parts[1];

    // Find empty line separating headers from body
    let body = if let Some(idx) = raw.find("\r\n\r\n") {
        raw[idx + 4..].as_bytes()
    } else if let Some(idx) = raw.find("\n\n") {
        raw[idx + 2..].as_bytes()
    } else {
        b"" as &[u8]
    };

    // Parse /api/<table>[/<id>]
    let prefix = format!("/api/{}", table);
    if !path.starts_with(&prefix) {
        let _ = write_response(&mut stream, 404, "Not Found", b"not found").await;
        return;
    }
    let suffix = &path[prefix.len()..];
    let id_opt = if suffix.starts_with('/') && suffix.len() > 1 {
        Some(&suffix[1..])
    } else {
        None
    };

    let response = dispatch(&db, &table, &method, id_opt, body).await;
    match response {
        Ok(json) => {
            let _ = write_response(&mut stream, 200, "OK", json.as_bytes()).await;
        }
        Err(e) => {
            let msg = format!("{{\"error\":\"{}\"}}", e);
            let _ = write_response(&mut stream, 500, "Error", msg.as_bytes()).await;
        }
    }
    // Graceful TCP shutdown — ensures the client receives the full response
    // before the socket is closed. Without this, Windows may send RST when
    // there is unread data (e.g. POST body sent after Expect:100-continue),
    // causing the client to see "connection was closed unexpectedly".
    let _ = stream.shutdown().await;
}

async fn dispatch(
    db: &Arc<Database>,
    table: &str,
    method: &str,
    id: Option<&str>,
    body: &[u8],
) -> Result<String, FlowError> {
    match (method, id) {
        // GET /api/<table>  — list all rows
        ("GET", None) => {
            let tbl = db.get_table(table)?;
            let guard = tbl.read().map_err(|_| FlowError::Io("lock".into()))?;
            let rows: Vec<serde_json::Value> = guard
                .rows
                .values()
                .filter(|r| !r.deleted)
                .map(|r| fields_to_json(&r.fields))
                .collect();
            Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into()))
        }
        // POST /api/<table>  — insert row
        ("POST", None) | ("POST", Some(_)) => {
            let json: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| FlowError::Parse(e.to_string()))?;
            let fields = json_to_fields(&json)?;
            let tbl = db.get_table(table)?;
            tbl.write()
                .map_err(|_| FlowError::Io("lock".into()))?
                .insert(fields)?;
            Ok("{\"ok\":true}".into())
        }
        // GET /api/<table>/<id>  — fetch single row
        ("GET", Some(id)) => {
            let tbl = db.get_table(table)?;
            let guard = tbl.read().map_err(|_| FlowError::Io("lock".into()))?;
            let row = guard
                .rows
                .values()
                .filter(|r| !r.deleted)
                .find(|r| row_matches_id(r, id));
            match row {
                Some(r) => Ok(serde_json::to_string(&fields_to_json(&r.fields)).unwrap()),
                None => Err(FlowError::Parse(format!("row {} not found", id))),
            }
        }
        // PUT /api/<table>/<id>  — update row
        ("PUT", Some(id)) => {
            let json: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| FlowError::Parse(e.to_string()))?;
            let updates = json_to_fields(&json)?;
            let tbl = db.get_table(table)?;
            let mut guard = tbl.write().map_err(|_| FlowError::Io("lock".into()))?;
            let row = guard
                .rows
                .values_mut()
                .filter(|r| !r.deleted)
                .find(|r| row_matches_id(r, id));
            match row {
                Some(r) => {
                    for (k, v) in updates {
                        r.fields.insert(k, v);
                    }
                    Ok("{\"ok\":true}".into())
                }
                None => Err(FlowError::Parse(format!("row {} not found", id))),
            }
        }
        // DELETE /api/<table>/<id>  — delete row
        ("DELETE", Some(id)) => {
            let tbl = db.get_table(table)?;
            let mut guard = tbl.write().map_err(|_| FlowError::Io("lock".into()))?;
            let row = guard
                .rows
                .values_mut()
                .filter(|r| !r.deleted)
                .find(|r| row_matches_id(r, id));
            match row {
                Some(r) => {
                    r.deleted = true;
                    Ok("{\"ok\":true}".into())
                }
                None => Err(FlowError::Parse(format!("row {} not found", id))),
            }
        }
        _ => Err(FlowError::Parse(format!("unsupported method: {}", method))),
    }
}

fn row_matches_id(row: &crate::types::Row, id: &str) -> bool {
    if row.id.to_string() == id {
        return true;
    }
    for key in &["id", "pk", "key"] {
        if let Some(v) = row.fields.get(*key) {
            let s = match v {
                Value::Text(t) => t.clone(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                _ => continue,
            };
            if s == id {
                return true;
            }
        }
    }
    false
}

fn fields_to_json(fields: &HashMap<String, Value>) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Json(v) => v.clone(),
        Value::Blob(_) => serde_json::Value::Null,
        Value::Vector(v) => serde_json::Value::Array(v.iter().map(|f| serde_json::Value::Number(serde_json::Number::from_f64(*f as f64).unwrap_or_else(|| serde_json::Number::from(0)))).collect()),
    }
}

fn json_to_fields(json: &serde_json::Value) -> Result<HashMap<String, Value>, FlowError> {
    let obj = json.as_object().ok_or_else(|| FlowError::Parse("body must be a JSON object".into()))?;
    let mut map = HashMap::new();
    for (k, v) in obj {
        let val = json_to_value(v)?;
        map.insert(k.clone(), val);
    }
    Ok(map)
}

fn json_to_value(v: &serde_json::Value) -> Result<Value, FlowError> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(FlowError::Parse("invalid number".into()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
        serde_json::Value::Array(arr) => {
            // Represent JSON arrays as a JSON value
            Ok(Value::Json(serde_json::Value::Array(arr.clone())))
        }
        serde_json::Value::Object(_) => {
            Err(FlowError::Parse("nested objects not supported".into()))
        }
    }
}

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    status_text: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        body.len(),
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}
