//! FlowDB — interactive REPL (Read-Eval-Print Loop).
//!
//! Connects to a running FlowDB server via TCP and provides
//! an interactive prompt for entering FlowQL queries.
//!
//! This binary is a pure TCP client — it does NOT import any server-side
//! modules. All processing happens on the server; this binary only handles
//! I/O and display.

use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::net::SocketAddr;

use clap::Parser as ClapParser;
use serde_json::Value;

#[derive(ClapParser, Debug)]
#[command(name = "flowdb-repl", about = "FlowDB interactive REPL", version)]
struct Cli {
    /// Server address to connect to
    #[arg(short, long, default_value = "127.0.0.1:7878")]
    addr: SocketAddr,
}

fn main() {
    let cli = Cli::parse();

    let stream = TcpStream::connect(cli.addr).unwrap_or_else(|e| {
        eprintln!("Cannot connect to FlowDB server at {}: {e}", cli.addr);
        eprintln!("Start the server with: flowdb-server");
        std::process::exit(1);
    });

    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut writer = stream;

    // Read and print welcome banner
    let mut banner = String::new();
    reader.read_line(&mut banner).ok();
    print_response(&banner);

    let stdin = io::stdin();
    let mut input_buf = String::new();

    loop {
        // Prompt
        print!("flowql> ");
        io::stdout().flush().ok();

        input_buf.clear();
        match stdin.lock().read_line(&mut input_buf) {
            Ok(0) => {
                println!("\nBye!");
                break;
            }
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
            Ok(_) => {}
        }

        let line = input_buf.trim();
        if line.is_empty() {
            continue;
        }
        if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") || line.eq_ignore_ascii_case("\\q") {
            println!("Bye!");
            break;
        }
        if line.eq_ignore_ascii_case("\\help") || line.eq_ignore_ascii_case("help") {
            print_help();
            continue;
        }

        // Send query to server
        let msg = format!("{line}\n");
        if let Err(e) = writer.write_all(msg.as_bytes()) {
            eprintln!("send error: {e}");
            break;
        }

        // Read response
        let mut resp_line = String::new();
        match reader.read_line(&mut resp_line) {
            Ok(0) => {
                eprintln!("server closed connection");
                break;
            }
            Err(e) => {
                eprintln!("receive error: {e}");
                break;
            }
            Ok(_) => {}
        }
        print_response(&resp_line);
    }
}

fn print_response(raw: &str) {
    match serde_json::from_str::<serde_json::Value>(raw.trim()) {
        Ok(v) => {
            let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("?");
            if status == "error" {
                let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("unknown error");
                eprintln!("[ERROR] {msg}");
                return;
            }
            if status == "welcome" {
                let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
                println!("{msg}");
                return;
            }
            if let Some(metrics_str) = v.get("metrics").and_then(|m| m.as_str()) {
                println!("{metrics_str}");
                return;
            }
            if let Some(result) = v.get("result") {
                print_result(result);
            }
        }
        Err(_) => {
            // Not JSON — print as-is
            print!("{raw}");
        }
    }
}

fn print_result(result: &Value) {
    // Check for Rows result
    if let (Some(cols), Some(rows)) = (result.get("Rows").and_then(|r| r.get("columns")), result.get("Rows").and_then(|r| r.get("rows"))) {
        print_table(cols, rows);
        return;
    }
    // Check for Count
    if let Some(count_obj) = result.get("Count") {
        let n = count_obj.get("affected").and_then(|n| n.as_u64()).unwrap_or(0);
        let ms = count_obj.get("elapsed_ms").and_then(|n| n.as_u64()).unwrap_or(0);
        println!("{n} row(s) affected  ({ms}ms)");
        return;
    }
    // Check for Ok
    if let Some(ok_obj) = result.get("Ok") {
        let msg = ok_obj.get("message").and_then(|m| m.as_str()).unwrap_or("OK");
        let ms = ok_obj.get("elapsed_ms").and_then(|n| n.as_u64()).unwrap_or(0);
        println!("{msg}  ({ms}ms)");
        return;
    }
    // Check for Plan
    if let Some(plan_obj) = result.get("Plan") {
        let desc = plan_obj.get("description").and_then(|d| d.as_str()).unwrap_or("(no plan)");
        println!("{desc}");
        return;
    }
    // Fallback: pretty-print JSON
    println!("{}", serde_json::to_string_pretty(result).unwrap_or_else(|_| result.to_string()));
}

fn print_table(cols: &Value, rows: &Value) {
    let headers: Vec<&str> = cols
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    let data_rows: Vec<Vec<String>> = rows
        .as_array()
        .map(|outer| {
            outer
                .iter()
                .map(|row| {
                    row.as_array()
                        .map(|cells| {
                            cells.iter().map(|c| format_cell(c)).collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default();

    if headers.is_empty() {
        println!("(no columns)");
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &data_rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Print header
    let header_line: String = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:<width$}", h, width = widths.get(i).copied().unwrap_or(0)))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{header_line}");
    let sep: String = widths.iter().map(|&w| "-".repeat(w)).collect::<Vec<_>>().join("-+-");
    println!("{sep}");

    // Print rows
    if data_rows.is_empty() {
        println!("(0 rows)");
    } else {
        for row in &data_rows {
            let row_line: String = row
                .iter()
                .enumerate()
                .map(|(i, cell)| format!("{:<width$}", cell, width = widths.get(i).copied().unwrap_or(0)))
                .collect::<Vec<_>>()
                .join(" | ");
            println!("{row_line}");
        }
        println!("\n({} row{})", data_rows.len(), if data_rows.len() == 1 { "" } else { "s" });
    }
}

fn format_cell(v: &Value) -> String {
    // FlowDB serializes values with a type tag: {"type":"Int","value":1}
    // Unwrap that envelope so we display the raw value, not the JSON object.
    if let Some(val) = v.get("value") {
        let type_str = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        return match type_str {
            "Text" => val.as_str().unwrap_or("").to_string(),
            "Null" => "null".into(),
            "Blob" => format!("<blob {} bytes>", val.as_array().map(|a| a.len()).unwrap_or(0)),
            // Int, Float, Bool, Json — use the JSON representation directly
            _ => val.to_string(),
        };
    }
    // Fallback for plain JSON primitives
    match v {
        Value::Null => "null".into(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

fn print_help() {
    println!(r#"
FlowDB — FlowQL Command Reference
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Data Manipulation:
  GET <table> [WHERE <expr>] [ORDER BY <col> ASC|DESC] [LIMIT n] [TIMEOUT "5s"]
  PUT <table> {{ col: val, … }}
  SET <table> {{ col: val, … }} [WHERE <expr>]
  DEL <table> [WHERE <expr>]
  FIND <table> WHERE <col> ~ "pattern" [LIMIT n]

Data Definition:
  MAKE TABLE <name> (col type [primary key], …)
  DROP TABLE <name>
  MAKE INDEX ON <table>(<col>)

Transactions:
  BEGIN
  COMMIT
  ROLLBACK

Admin / Monitoring:
  SHOW RUNNING QUERIES
  KILL QUERY <id>
  EXPLAIN <query>
  METRICS             ← show server metrics snapshot

REPL commands:
  \help / help        ← this help
  exit / quit / \q    ← disconnect

Supported types: int, float, text, bool, json, blob, any
Operators: = != < <= > >= AND OR NOT ~ (fuzzy)
"#);
}
