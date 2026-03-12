//! FlowDB — server entry point.
//!
//! Supports four modes:
//!   flowdb-server                    → run in console (foreground)
//!   flowdb-server install            → register as Windows service
//!   flowdb-server uninstall          → remove Windows service
//!   flowdb-server start              → start the Windows service
//!   flowdb-server stop               → stop the Windows service
//!   flowdb-server run-service        → internal: entry point when launched by SCM
#![allow(dead_code)]

mod auth;
mod error;
mod metrics;
mod resource;
mod sql;
mod storage;
mod engine;
mod types;
mod transaction;
mod wal;
mod server;
mod cluster;
mod mvcc;
mod ai;
mod triggers;
mod graph;
mod api;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser as ClapParser, Subcommand};
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

use crate::metrics::Metrics;
use crate::server::Server;
use crate::storage::persist;
use crate::storage::table::Database;
use crate::wal::WalWriter;
use crate::cluster::ClusterRegistry;
use crate::engine::watch::WatchRegistry;

// ── Service name constant ─────────────────────────────────────────────────

pub const SERVICE_NAME: &str = "FlowDBLite";
pub const SERVICE_DISPLAY: &str = "FlowDB Database";
pub const SERVICE_DESC: &str = "FlowDB — a custom database engine with FlowQL query language";

// ── CLI ───────────────────────────────────────────────────────────────────

#[derive(ClapParser, Debug)]
#[command(
    name = "flowdb-server",
    about = "FlowDB database server",
    version,
    long_about = "FlowDB database server.\n\nRun without arguments to start in console mode.\nUse subcommands to manage the Windows Service."
)]
struct Cli {
    /// Address to listen on (console mode)
    #[arg(short, long, default_value = "127.0.0.1:7878", global = true)]
    addr: SocketAddr,

    /// Path to WAL file
    #[arg(short, long, default_value = "flowdb.wal", global = true)]
    wal: PathBuf,

    /// Directory for catalog + snapshot persistence
    #[arg(short, long, default_value = "flowdb-data", global = true)]
    data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info", global = true)]
    log_level: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Install FlowDB as a Windows Service (run once, requires admin)
    Install,
    /// Remove the FlowDB Windows Service (requires admin)
    Uninstall,
    /// Start the FlowDB Windows Service
    Start,
    /// Stop the FlowDB Windows Service
    Stop,
    /// Internal: launched by Windows Service Control Manager
    #[command(hide = true)]
    RunService,
}

// ── Entry point ───────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Install)     => service_mgmt::install(&cli.addr, &cli.wal, &cli.log_level),
        Some(Commands::Uninstall)   => service_mgmt::uninstall(),
        Some(Commands::Start)       => service_mgmt::start(),
        Some(Commands::Stop)        => service_mgmt::stop(),
        Some(Commands::RunService)  => {
            // Launched by SCM — hand off to the Windows service dispatcher
            #[cfg(windows)]
            windows_svc::run_as_service(cli.addr, cli.wal, cli.log_level);
            #[cfg(not(windows))]
            eprintln!("run-service is Windows-only");
        }
        None => {
            // Console mode — run in foreground, log to stdout
            run_console(cli.addr, cli.wal, cli.data_dir, cli.log_level);
        }
    }
}

// ── Console mode ─────────────────────────────────────────────────────────

fn init_logging(log_level: &str) {
    let filter = EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .json()
        .init();
}

pub(crate) fn run_server(addr: SocketAddr, wal_path: PathBuf, data_dir: PathBuf) {
    info!("FlowDB starting — addr={addr}, wal={}, data={}",
        wal_path.display(), data_dir.display());
    let db      = Arc::new(Database::new());
    let wal     = Arc::new(WalWriter::open(&wal_path).expect("failed to open WAL"));
    let metrics = Arc::new(Metrics::new());

    // Recover from catalog + snapshots (no-op if data_dir is fresh)
    if let Err(e) = persist::recover(&db, &data_dir) {
        eprintln!("recovery error: {e}");
        std::process::exit(1);
    }

    let watch_registry   = Arc::new(WatchRegistry::new());
    let cluster_registry = Arc::new(ClusterRegistry::new());

    let srv = Server::new(db, wal, metrics, addr)
        .with_data_dir(data_dir)
        .with_watch_registry(Arc::clone(&watch_registry))
        .with_cluster_registry(Arc::clone(&cluster_registry));

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            // Background heartbeat task: probe each cluster peer every 10 s.
            let cr = Arc::clone(&cluster_registry);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    let addrs = cr.peer_addrs();
                    for peer_addr in addrs {
                        let t = std::time::Instant::now();
                        let result = tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            tokio::net::TcpStream::connect(&peer_addr),
                        )
                        .await;
                        if let Ok(Ok(_)) = result {
                            cr.record_heartbeat(&peer_addr, t.elapsed().as_millis() as u64);
                        } else {
                            cr.record_unreachable(&peer_addr);
                        }
                    }
                }
            });

            if let Err(e) = srv.serve().await {
                eprintln!("server error: {e}");
                std::process::exit(1);
            }
        });
}

fn run_console(addr: SocketAddr, wal: PathBuf, data_dir: PathBuf, log_level: String) {
    init_logging(&log_level);
    info!("Running in console mode. Press Ctrl+C to stop.");
    run_server(addr, wal, data_dir);
}

// ── Windows service logic ─────────────────────────────────────────────────

#[cfg(windows)]
mod windows_svc {
    use std::ffi::OsString;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::{define_windows_service, service_dispatcher};

    use super::run_server;

    // These are set once before the service dispatcher takes over
    static mut SERVICE_ADDR: Option<SocketAddr> = None;
    static mut SERVICE_WAL:  Option<PathBuf> = None;
    static mut SERVICE_DATA: Option<PathBuf> = None;

    define_windows_service!(ffi_service_main, service_main);

    pub fn run_as_service(addr: SocketAddr, wal: PathBuf, log_level: String) {
        // Initialize logging to file since we have no console
        let filter = tracing_subscriber::EnvFilter::try_new(&log_level)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .json()
            .init();

        // Store config for the service entry point
        unsafe {
            SERVICE_ADDR = Some(addr);
            SERVICE_WAL  = Some(wal);
            SERVICE_DATA = Some(PathBuf::from("flowdb-data"));
        }

        service_dispatcher::start(super::SERVICE_NAME, ffi_service_main)
            .expect("service dispatcher failed");
    }

    #[allow(static_mut_refs)]
    fn service_main(_args: Vec<OsString>) {
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();

        let event_handler = move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                ServiceControl::Stop => {
                    let _ = shutdown_tx.send(());
                    ServiceControlHandlerResult::NoError
                }
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        };

        let status_handle = service_control_handler::register(super::SERVICE_NAME, event_handler)
            .expect("register service handler");

        // Report: Running
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Running,
            controls_accepted: ServiceControlAccept::STOP,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        }).expect("set service status Running");

        // Spawn server on a background thread so we can monitor shutdown_rx
        let addr     = unsafe { SERVICE_ADDR.expect("addr not set") };
        let wal      = unsafe { SERVICE_WAL.take().expect("wal not set") };
        let data_dir = unsafe { SERVICE_DATA.take().unwrap_or_else(|| PathBuf::from("flowdb-data")) };

        let server_thread = std::thread::spawn(move || {
            run_server(addr, wal, data_dir);
        });

        // Block until SCM sends Stop
        let _ = shutdown_rx.recv();

        // Report: Stopped
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        }).expect("set service status Stopped");

        drop(server_thread);
    }
}

// ── Service management (install/uninstall/start/stop) ────────────────────

mod service_mgmt {
    use std::net::SocketAddr;
    use std::path::PathBuf;

    pub fn install(addr: &SocketAddr, wal: &PathBuf, log_level: &str) {
        #[cfg(windows)]
        {
            use std::ffi::OsString;
            use windows_service::service::{
                ServiceAccess, ServiceErrorControl, ServiceInfo, ServiceStartType, ServiceType,
            };
            use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

            let manager = ServiceManager::local_computer(
                None::<&str>,
                ServiceManagerAccess::CREATE_SERVICE,
            ).unwrap_or_else(|e| { eprintln!("Cannot open Service Manager: {e}\nRun as Administrator."); std::process::exit(1); });

            let exe = std::env::current_exe().expect("current exe path");
            // Build the service binary path with stored args
            let bin_path = format!(
                "\"{}\" --addr {} --wal \"{}\" --log-level {} run-service",
                exe.display(), addr, wal.display(), log_level
            );

            let service_info = ServiceInfo {
                name: OsString::from(crate::SERVICE_NAME),
                display_name: OsString::from(crate::SERVICE_DISPLAY),
                service_type: ServiceType::OWN_PROCESS,
                start_type: ServiceStartType::AutoStart,
                error_control: ServiceErrorControl::Normal,
                executable_path: std::path::PathBuf::from(&bin_path),
                launch_arguments: vec![],
                dependencies: vec![],
                account_name: None,  // LocalSystem
                account_password: None,
            };

            match manager.create_service(&service_info, ServiceAccess::CHANGE_CONFIG) {
                Ok(svc) => {
                    // Set description
                    svc.set_description(crate::SERVICE_DESC).ok();
                    println!("✓ Service '{}' installed successfully.", crate::SERVICE_NAME);
                    println!("  Start it with: flowdb-server start");
                    println!("  Or via Services panel (services.msc)");
                }
                Err(e) => eprintln!("Failed to install service: {e}"),
            }
        }
        #[cfg(not(windows))]
        eprintln!("Service management is Windows-only. Use a systemd unit on Linux.");
    }

    pub fn uninstall() {
        #[cfg(windows)]
        {
            use windows_service::service::ServiceAccess;
            use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

            let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
                .unwrap_or_else(|e| { eprintln!("Cannot open Service Manager: {e}"); std::process::exit(1); });

            let svc = manager.open_service(crate::SERVICE_NAME, ServiceAccess::DELETE)
                .unwrap_or_else(|e| { eprintln!("Cannot open service '{}': {e}", crate::SERVICE_NAME); std::process::exit(1); });

            match svc.delete() {
                Ok(_)  => println!("✓ Service '{}' uninstalled.", crate::SERVICE_NAME),
                Err(e) => eprintln!("Failed to uninstall: {e}"),
            }
        }
        #[cfg(not(windows))]
        eprintln!("Service management is Windows-only.");
    }

    pub fn start() {
        #[cfg(windows)]
        {
            use windows_service::service::ServiceAccess;
            use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

            let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
                .unwrap_or_else(|e| { eprintln!("Cannot open Service Manager: {e}"); std::process::exit(1); });

            let svc = manager.open_service(crate::SERVICE_NAME, ServiceAccess::START)
                .unwrap_or_else(|e| { eprintln!("Cannot open service: {e}"); std::process::exit(1); });

            match svc.start::<&str>(&[]) {
                Ok(_)  => println!("✓ Service '{}' started.", crate::SERVICE_NAME),
                Err(e) => eprintln!("Failed to start: {e}"),
            }
        }
        #[cfg(not(windows))]
        eprintln!("Service management is Windows-only.");
    }

    pub fn stop() {
        #[cfg(windows)]
        {
            use std::time::Duration;
            use windows_service::service::{ServiceAccess, ServiceState};
            use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

            let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
                .unwrap_or_else(|e| { eprintln!("Cannot open Service Manager: {e}"); std::process::exit(1); });

            let svc = manager.open_service(
                crate::SERVICE_NAME,
                ServiceAccess::STOP | ServiceAccess::QUERY_STATUS,
            ).unwrap_or_else(|e| { eprintln!("Cannot open service: {e}"); std::process::exit(1); });

            let status = svc.stop().unwrap_or_else(|e| { eprintln!("Failed to stop: {e}"); std::process::exit(1); });

            if status.current_state == ServiceState::StopPending {
                println!("Service is stopping...");
                std::thread::sleep(Duration::from_secs(3));
            }
            println!("✓ Service '{}' stopped.", crate::SERVICE_NAME);
        }
        #[cfg(not(windows))]
        eprintln!("Service management is Windows-only.");
    }
}

