#!/usr/bin/env pwsh
# ─────────────────────────────────────────────────────────────────
#  FlowDB – interactive query test runner
#  Usage:  .\test_queries.ps1
#  Requires: flowdb-server running on 127.0.0.1:7878
# ─────────────────────────────────────────────────────────────────

$server = "127.0.0.1"
$port   = 7878

function Send-Query {
    param([string]$query)
    try {
        $client  = [System.Net.Sockets.TcpClient]::new($server, $port)
        $stream  = $client.GetStream()
        $writer  = [System.IO.StreamWriter]::new($stream)
        $reader  = [System.IO.StreamReader]::new($stream)
        $writer.AutoFlush = $true

        # Discard the welcome banner the server sends on connect
        $null = $reader.ReadLine()

        $writer.WriteLine($query)
        Start-Sleep -Milliseconds 150
        $response = $reader.ReadLine()

        $client.Close()
        return $response | ConvertFrom-Json
    } catch {
        return @{ status = "error"; message = $_.Exception.Message }
    }
}

function Run-Test {
    param([string]$label, [string]$query)
    Write-Host ""
    Write-Host "► $label" -ForegroundColor Cyan
    Write-Host "  $query" -ForegroundColor Gray
    $result = Send-Query $query
    if ($result.status -eq "ok" -or $result.status -eq "plan") {
        Write-Host "  [OK] $($result | ConvertTo-Json -Depth 5 -Compress)" -ForegroundColor Green
    } else {
        Write-Host "  [FAIL] $($result | ConvertTo-Json -Depth 5 -Compress)" -ForegroundColor Red
    }
}

Write-Host "═══════════════════════════════════════════════" -ForegroundColor Yellow
Write-Host "  FlowDB – Query Test Runner" -ForegroundColor Yellow
Write-Host "  Server: ${server}:${port}" -ForegroundColor Yellow
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Yellow

# ── 1. DDL ───────────────────────────────────────────────────────
Write-Host "`n[1] Table Management" -ForegroundColor Magenta

Run-Test "DROP TABLE (cleanup)" "DROP TABLE users"
Run-Test "DROP TABLE (cleanup)" "DROP TABLE products"
Run-Test "DROP TABLE (cleanup)" "DROP TABLE items"

Run-Test "MAKE TABLE users" `
    'MAKE TABLE users ( id int PRIMARY KEY, name text, age int, active bool )'

Run-Test "MAKE TABLE products" `
    'MAKE TABLE products ( id int PRIMARY KEY, name text, price float )'

Run-Test "MAKE TABLE items (with vector)" `
    'MAKE TABLE items ( id int PRIMARY KEY, label text, embedding vector )'

Run-Test "MAKE INDEX on users(name)" "MAKE INDEX ON users (name)"
Run-Test "MAKE INDEX on products(price)" "MAKE INDEX ON products (price)"

Run-Test "SHOW TABLES" "SHOW TABLES"

# ── 2. Write Data ────────────────────────────────────────────────
Write-Host "`n[2] Writing Data (PUT)" -ForegroundColor Magenta

Run-Test "PUT users row 1" 'PUT users { id: 1, name: "Alice", age: 30, active: true }'
Run-Test "PUT users row 2" 'PUT users { id: 2, name: "Bob", age: 25, active: false }'
Run-Test "PUT users row 3" 'PUT users { id: 3, name: "Carol", age: 35, active: true }'
Run-Test "PUT users row 4" 'PUT users { id: 4, name: "Dave", age: 22, active: true }'

Run-Test "PUT products row 1" 'PUT products { id: 1, name: "Widget", price: 9.99 }'
Run-Test "PUT products row 2" 'PUT products { id: 2, name: "Gadget", price: 24.99 }'
Run-Test "PUT products row 3" 'PUT products { id: 3, name: "Doohickey", price: 4.49 }'

Run-Test "PUT items (vectors)" 'PUT items { id: 1, label: "cat",  embedding: [0.9, 0.1, 0.0] }'
Run-Test "PUT items (vectors)" 'PUT items { id: 2, label: "dog",  embedding: [0.8, 0.2, 0.1] }'
Run-Test "PUT items (vectors)" 'PUT items { id: 3, label: "fish", embedding: [0.1, 0.1, 0.9] }'

Run-Test "PUT upsert (same PK)" 'PUT users { id: 1, name: "Alice", age: 31, active: true }'

# ── 3. Read Data ─────────────────────────────────────────────────
Write-Host "`n[3] Reading Data (GET)" -ForegroundColor Magenta

Run-Test "GET all users"                     "GET users"
Run-Test "GET with WHERE ="                  'GET users WHERE id = 1'
Run-Test "GET with WHERE active=true"        "GET users WHERE active = true"
Run-Test "GET with WHERE age >="             "GET users WHERE age >= 25"
Run-Test "GET with AND"                      "GET users WHERE age >= 25 AND active = true"
Run-Test "GET with OR"                       'GET users WHERE name = "Alice" OR name = "Bob"'
Run-Test "GET with NOT"                      "GET users WHERE NOT active = true"
Run-Test "GET ORDER BY age ASC"              "GET users ORDER BY age ASC"
Run-Test "GET ORDER BY age DESC"             "GET users ORDER BY age DESC"
Run-Test "GET ORDER BY LIMIT"                "GET users ORDER BY age ASC LIMIT 2"
Run-Test 'GET with TIMEOUT "5s"'             'GET users WHERE active = true TIMEOUT "5s"'
Run-Test "GET arithmetic in filter"          "GET products WHERE price * 1.2 < 30.0"

# ── 4. Update & Delete ───────────────────────────────────────────
Write-Host "`n[4] Update & Delete" -ForegroundColor Magenta

Run-Test "SET specific row"    'SET users { age: 26 } WHERE id = 2'
Run-Test "SET all matching"    "SET users { active: true } WHERE age < 25"
Run-Test "GET to verify SET"   "GET users WHERE id = 2"
Run-Test "DEL specific row"    "DEL users WHERE id = 4"
Run-Test "GET to verify DEL"   "GET users WHERE id = 4"

# ── 5. Fuzzy Search ──────────────────────────────────────────────
Write-Host "`n[5] Fuzzy Search (FIND)" -ForegroundColor Magenta

Run-Test "FIND partial name"   'FIND users WHERE name ~ "alic"'
Run-Test "FIND product"        'FIND products WHERE name ~ "widge" LIMIT 5'
Run-Test "FIND no match"       'FIND users WHERE name ~ "zzzzz"'

# ── 6. Vector Similarity ─────────────────────────────────────────
Write-Host "`n[6] Vector Similarity (SIMILAR)" -ForegroundColor Magenta

Run-Test "SIMILAR ON embedding"        "SIMILAR items ON embedding TO [0.85, 0.15, 0.05] LIMIT 2"
Run-Test "SIMILAR default limit"       "SIMILAR items ON embedding TO [0.1, 0.1, 0.9]"

# ── 7. Transactions ──────────────────────────────────────────────
Write-Host "`n[7] Transactions" -ForegroundColor Magenta

Run-Test "BEGIN"    "BEGIN"
Run-Test "COMMIT"   "COMMIT"
Run-Test "ROLLBACK" "ROLLBACK"

# ── 8. Cluster ───────────────────────────────────────────────────
Write-Host "`n[8] Cluster Commands" -ForegroundColor Magenta

Run-Test "CLUSTER STATUS (empty)" "CLUSTER STATUS"
Run-Test "CLUSTER JOIN"           'CLUSTER JOIN "192.168.1.20:7878"'
Run-Test "CLUSTER STATUS"         "CLUSTER STATUS"
Run-Test "CLUSTER PART"           'CLUSTER PART "192.168.1.20:7878"'

# ── 9. Admin ─────────────────────────────────────────────────────
Write-Host "`n[9] Admin Commands" -ForegroundColor Magenta

Run-Test "EXPLAIN GET"           "EXPLAIN GET users WHERE age > 25"
Run-Test "SHOW RUNNING QUERIES"  "SHOW RUNNING QUERIES"
Run-Test "METRICS"               "METRICS"
Run-Test "CHECKPOINT"            "CHECKPOINT"

# ── 10. AI Native Search ─────────────────────────────────────────
Write-Host "`n[10] AI Native Search (AI SEARCH)" -ForegroundColor Magenta

Run-Test "DROP TABLE docs (cleanup)"  "DROP TABLE docs"
Run-Test "MAKE TABLE docs" `
    'MAKE TABLE docs ( id int PRIMARY KEY, title text, body text )'
Run-Test "PUT doc 1" 'PUT docs { id: 1, title: "Rust intro",      body: "rust systems programming language memory safe" }'
Run-Test "PUT doc 2" 'PUT docs { id: 2, title: "Database design",  body: "database query storage engine index performance" }'
Run-Test "PUT doc 3" 'PUT docs { id: 3, title: "Python tutorial",  body: "python scripting dynamic typing interpreted" }'
Run-Test "PUT doc 4" 'PUT docs { id: 4, title: "DB performance",   body: "database index query optimisation storage engine" }'

Run-Test "AI SEARCH no limit"    'AI SEARCH docs "database query engine"'
Run-Test "AI SEARCH LIMIT 2"     'AI SEARCH docs "database query engine" LIMIT 2'
Run-Test "AI SEARCH unrelated"   'AI SEARCH docs "python scripting" LIMIT 3'

# ── 11. Time Travel ──────────────────────────────────────────────
Write-Host "`n[11] Time Travel Queries" -ForegroundColor Magenta

Run-Test "GET AS OF timestamp"    'GET users AS OF "2020-01-01"'
Run-Test "GET AS OF future date"  'GET users AS OF "2099-12-31"'
Run-Test "GET VERSION 0"          "GET users VERSION 0"
Run-Test "GET VERSION 1"          "GET users VERSION 1"
Run-Test "GET VERSION 999"        "GET users VERSION 999"

# ── 12. Event-Driven Triggers ────────────────────────────────────
Write-Host "`n[12] Event-Driven Triggers (TRIGGER)" -ForegroundColor Magenta

Run-Test "DROP TABLE audit (cleanup)"   "DROP TABLE audit"
Run-Test "MAKE TABLE audit" `
    'MAKE TABLE audit ( id int PRIMARY KEY, event text )'

Run-Test "CREATE TRIGGER on PUT users" `
    'TRIGGER user_put_log WHEN PUT users DO PUT audit { id: 99, event: "user created" }'
Run-Test "CREATE TRIGGER on SET users" `
    'TRIGGER user_set_log WHEN SET users DO PUT audit { id: 98, event: "user updated" }'
Run-Test "CREATE TRIGGER on DEL users" `
    'TRIGGER user_del_log WHEN DEL users DO PUT audit { id: 97, event: "user deleted" }'

Run-Test "SHOW TRIGGERS"   "SHOW TRIGGERS"

Run-Test "PUT fires PUT trigger"  'PUT users { id: 10, name: "TriggerUser", age: 20, active: true }'
Run-Test "GET audit after PUT"    "GET audit"

Run-Test "SET fires SET trigger"  "SET users { age: 21 } WHERE id = 10"
Run-Test "GET audit after SET"    "GET audit"

Run-Test "DEL fires DEL trigger"  "DEL users WHERE id = 10"
Run-Test "GET audit after DEL"    "GET audit"

Run-Test "DROP TRIGGER user_put_log"  "DROP TRIGGER user_put_log"
Run-Test "DROP TRIGGER user_set_log"  "DROP TRIGGER user_set_log"
Run-Test "DROP TRIGGER user_del_log"  "DROP TRIGGER user_del_log"
Run-Test "SHOW TRIGGERS (empty)"  "SHOW TRIGGERS"

# ── 13. Graph + Relational Hybrid ────────────────────────────────
Write-Host "`n[13] Graph + Relational Hybrid (GRAPH MATCH)" -ForegroundColor Magenta

Run-Test "DROP TABLE follows (cleanup)"  "DROP TABLE follows"
Run-Test "MAKE TABLE follows" `
    'MAKE TABLE follows ( id int PRIMARY KEY, from_id int, to_id int, since text )'

Run-Test "PUT follows Alice→Bob"    'PUT follows { id: 1, from_id: 1, to_id: 2, since: "2024-01" }'
Run-Test "PUT follows Alice→Carol"  'PUT follows { id: 2, from_id: 1, to_id: 3, since: "2024-02" }'
Run-Test "PUT follows Bob→Carol"    'PUT follows { id: 3, from_id: 2, to_id: 3, since: "2024-03" }'

Run-Test "GRAPH MATCH all" `
    "GRAPH MATCH (a:users)-[e:follows]->(b:users)"
Run-Test "GRAPH MATCH WHERE src name" `
    'GRAPH MATCH (a:users)-[e:follows]->(b:users) WHERE a.name = "Alice"'
Run-Test "GRAPH MATCH WHERE dst name" `
    'GRAPH MATCH (a:users)-[e:follows]->(b:users) WHERE b.name = "Carol"'
Run-Test "GRAPH MATCH LIMIT 1" `
    "GRAPH MATCH (a:users)-[e:follows]->(b:users) LIMIT 1"

# ── 14. Built-in REST API Server ─────────────────────────────────
Write-Host "`n[14] Built-in REST API Server (API GENERATE)" -ForegroundColor Magenta

Run-Test "API GENERATE FOR users"     "API GENERATE FOR users"
Run-Test "API GENERATE FOR products"  "API GENERATE FOR products"
Run-Test "SHOW APIS"                  "SHOW APIS"

# Extract actual URL from the API GENERATE response
$usersApiResp  = Send-Query "API GENERATE FOR users"
$productsApiResp = Send-Query "API GENERATE FOR products"
# Parse URL from message like: REST API for `users` running at http://...
$usersUrl = ($usersApiResp.result.Ok.message -replace '.*running at (http://\S+)','$1')
if (-not $usersUrl -or -not $usersUrl.StartsWith('http')) {
    $usersUrl = "http://127.0.0.1:7879/api/users"
}

# Hit the auto-generated REST endpoints via HTTP
Start-Sleep -Milliseconds 400
Write-Host ""
Write-Host "  Testing auto-generated REST endpoints (dynamic port)..." -ForegroundColor Cyan
Write-Host "  Users API URL: $usersUrl" -ForegroundColor Gray
try {
    $r = Invoke-WebRequest -Uri $usersUrl -UseBasicParsing -TimeoutSec 5
    Write-Host "  [OK] GET $usersUrl  →  $($r.StatusCode)  $($r.Content.Substring(0,[Math]::Min(120,$r.Content.Length)))" -ForegroundColor Green
} catch {
    Write-Host "  [FAIL] GET $usersUrl  →  $($_.Exception.Message)" -ForegroundColor Red
}
try {
    $body = '{"id":999,"name":"RESTUser","age":28,"active":true}'
    $r = Invoke-WebRequest -Uri $usersUrl -Method POST -Body $body -ContentType "application/json" -UseBasicParsing -TimeoutSec 5
    Write-Host "  [OK] POST $usersUrl  →  $($r.StatusCode)  $($r.Content)" -ForegroundColor Green
} catch {
    Write-Host "  [FAIL] POST $usersUrl  →  $($_.Exception.Message)" -ForegroundColor Red
}
try {
    $r = Invoke-WebRequest -Uri "$usersUrl/1" -UseBasicParsing -TimeoutSec 5
    Write-Host "  [OK] GET $usersUrl/1  →  $($r.StatusCode)  $($r.Content)" -ForegroundColor Green
} catch {
    Write-Host "  [FAIL] GET $usersUrl/1  →  $($_.Exception.Message)" -ForegroundColor Red
}

Run-Test "API STOP FOR users"     "API STOP FOR users"
Run-Test "API STOP FOR products"  "API STOP FOR products"
Run-Test "SHOW APIS (empty)"      "SHOW APIS"

# ── Summary ──────────────────────────────────────────────────────
Write-Host ""
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Yellow
Write-Host "  Done. Connect interactively with:" -ForegroundColor Yellow
Write-Host "  .\target\release\flowdb-repl.exe" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Yellow
