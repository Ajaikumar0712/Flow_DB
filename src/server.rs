//! Async TCP server for FlowDB.
//!
//! Protocol (line-based text):
//!   client sends: one FlowQL statement per line (ends with \n)
//!   server sends: JSON response + "\n" for each statement
//!
//! Format of server response:
//!   { "status": "ok", "result": <QueryResult as JSON> }
//!   { "status": "error", "message": "<error message>" }
//!
//! One Tokio task per connection. Shared database behind Arc<RwLock>.
//! Each connection gets its own TransactionManager.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::api::ApiStore;
use crate::cluster::ClusterRegistry;
use crate::engine::executor::{Executor, QueryResult};
use crate::engine::watch::{WatchEvent, WatchRegistry};
use crate::error::FlowError;
use crate::metrics::Metrics;
use crate::sql::ast::Stmt;
use crate::sql::parser::Parser;
use crate::storage::table::Database;
use crate::transaction::TransactionManager;
use crate::triggers::TriggerStore;
use crate::wal::WalWriter;

// ── Server ────────────────────────────────────────────────────────────────

pub struct Server {
    db: Arc<Database>,
    wal: Arc<WalWriter>,
    metrics: Arc<Metrics>,
    addr: SocketAddr,
    data_dir: Option<Arc<PathBuf>>,
    watch_registry: Arc<WatchRegistry>,
    cluster_registry: Option<Arc<ClusterRegistry>>,
    /// Shared trigger store — persists across all connections.
    trigger_store: Arc<TriggerStore>,
    /// Shared REST API server store — persists across all connections.
    api_store: Arc<ApiStore>,
}

impl Server {
    pub fn new(
        db: Arc<Database>,
        wal: Arc<WalWriter>,
        metrics: Arc<Metrics>,
        addr: SocketAddr,
    ) -> Self {
        Self {
            db,
            wal,
            metrics,
            addr,
            data_dir: None,
            watch_registry: Arc::new(WatchRegistry::new()),
            cluster_registry: None,
            trigger_store: Arc::new(TriggerStore::new()),
            api_store: Arc::new(ApiStore::new()),
        }
    }

    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(Arc::new(data_dir));
        self
    }

    pub fn with_watch_registry(mut self, wr: Arc<WatchRegistry>) -> Self {
        self.watch_registry = wr;
        self
    }

    pub fn with_cluster_registry(mut self, cr: Arc<ClusterRegistry>) -> Self {
        self.cluster_registry = Some(cr);
        self
    }

    /// Start accepting connections. Runs until the process is killed.
    pub async fn serve(self) -> Result<(), FlowError> {
        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| FlowError::Io(format!("cannot bind to {}: {e}", self.addr)))?;

        info!("FlowDB server listening on {}", self.addr);

        let db           = self.db.clone();
        let wal          = self.wal.clone();
        let metrics      = self.metrics.clone();
        let data_dir     = self.data_dir.clone();
        let watch_reg    = self.watch_registry.clone();
        let cluster_reg  = self.cluster_registry.clone();
        let trigger_store = self.trigger_store.clone();
        let api_store     = self.api_store.clone();

        loop {
            let (stream, peer) = listener
                .accept()
                .await
                .map_err(|e| FlowError::Io(format!("accept error: {e}")))?;
            info!("new connection from {peer}");

            let db_c   = db.clone();
            let wal_c  = wal.clone();
            let met_c  = metrics.clone();
            let dir_c  = data_dir.clone();
            let wr_c   = watch_reg.clone();
            let cr_c   = cluster_reg.clone();
            let ts_c   = trigger_store.clone();
            let as_c   = api_store.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, db_c, wal_c, met_c, dir_c, wr_c, cr_c, ts_c, as_c, peer).await {
                    warn!("connection {peer} error: {e}");
                }
                info!("connection {peer} closed");
            });
        }
    }
}

// ── Per-connection handler ────────────────────────────────────────────────

async fn handle_connection(
    stream: TcpStream,
    db: Arc<Database>,
    wal: Arc<WalWriter>,
    metrics: Arc<Metrics>,
    data_dir: Option<Arc<PathBuf>>,
    watch_registry: Arc<WatchRegistry>,
    cluster_registry: Option<Arc<ClusterRegistry>>,
    trigger_store: Arc<TriggerStore>,
    api_store: Arc<ApiStore>,
    _peer: SocketAddr,
) -> Result<(), FlowError> {
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    // All writes go through a channel so both the main loop and watcher
    // tasks can send responses without fighting over &mut OwnedWriteHalf.
    let (write_tx, mut write_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut w = writer;
        while let Some(msg) = write_rx.recv().await {
            if w.write_all(msg.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let executor = match data_dir {
        Some(ref dir) => {
            let mut exec = Executor::with_data_dir(db, metrics.clone(), dir.as_ref().clone());
            exec.watch_registry = Some(Arc::clone(&watch_registry));
            exec.cluster_registry = cluster_registry;
            exec.trigger_store = trigger_store;
            exec.api_store = api_store;
            Arc::new(exec)
        }
        None => {
            let mut exec = Executor::new(db, metrics.clone());
            exec.watch_registry = Some(Arc::clone(&watch_registry));
            exec.cluster_registry = cluster_registry;
            exec.trigger_store = trigger_store;
            exec.api_store = api_store;
            Arc::new(exec)
        }
    };
    let mut tx_mgr = TransactionManager::new(executor, wal);

    // Send banner
    write_tx.send(
        "{\"status\":\"welcome\",\"message\":\"FlowDB \u{2014} ready. Type FlowQL queries.\"}\n".to_string()
    ).ok();

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|e| FlowError::Io(e.to_string()))?
    {
        let line = line.trim().to_string();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }

        if line.eq_ignore_ascii_case("METRICS") {
            let summary = metrics.summary();
            let resp = json!({ "status": "ok", "metrics": summary }).to_string() + "\n";
            write_tx.send(resp).ok();
            continue;
        }

        let t0 = std::time::Instant::now();
        let query_id = metrics.start_query(&line);

        let stmts = match Parser::parse_str(&line) {
            Ok(s) => s,
            Err(e) => {
                metrics.record_error(&e);
                metrics.finish_query(query_id, t0.elapsed());
                let resp = json!({"status":"error","message":e.to_string()}).to_string() + "\n";
                write_tx.send(resp).ok();
                continue;
            }
        };

        let mut last_result: Option<QueryResult> = None;
        let mut exec_error: Option<FlowError> = None;

        'stmts: for stmt in stmts {
            match stmt {
                Stmt::Watch { table, filter } => {
                    let (row_tx, mut row_rx) =
                        tokio::sync::mpsc::unbounded_channel::<WatchEvent>();
                    let id = watch_registry.subscribe(table, filter, row_tx);
                    let wtx = write_tx.clone();
                    tokio::spawn(async move {
                        while let Some(evt) = row_rx.recv().await {
                            let json_evt = json!({
                                "status": "watch",
                                "id": evt.watch_id,
                                "op": evt.op,
                                "row": evt.row.fields,
                            }).to_string() + "\n";
                            if wtx.send(json_evt).is_err() {
                                break;
                            }
                        }
                    });
                    metrics.finish_query(query_id, t0.elapsed());
                    let ack = json!({
                        "status": "ok",
                        "watch_id": id,
                        "message": format!("watching, subscription id={id}"),
                    }).to_string() + "\n";
                    write_tx.send(ack).ok();
                    continue; // next input line (the outer while loop)
                }
                Stmt::Unwatch { id } => {
                    watch_registry.unsubscribe(id);
                    metrics.finish_query(query_id, t0.elapsed());
                    let ack = json!({
                        "status": "ok",
                        "message": format!("unwatch {id}"),
                    }).to_string() + "\n";
                    write_tx.send(ack).ok();
                    continue; // next input line
                }
                other => match tx_mgr.execute(other) {
                    Ok(r)  => last_result = Some(r),
                    Err(e) => { exec_error = Some(e); break 'stmts; }
                },
            }
        }

        metrics.finish_query(query_id, t0.elapsed());

        let resp = match exec_error {
            Some(e) => {
                metrics.record_error(&e);
                json!({"status":"error","message":e.to_string()}).to_string()
            }
            None => {
                let r = last_result
                    .unwrap_or_else(|| QueryResult::ok("(empty)", t0.elapsed()));
                json!({"status":"ok","result":r}).to_string()
            }
        };
        write_tx.send(resp + "\n").ok();
    }
    Ok(())
}

