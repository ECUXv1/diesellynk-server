use std::os::unix::process::ExitStatusExt;
use actix::prelude::*;
use actix_files::Files;
use actix_web::{
    get, post, web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_actors::ws;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Mutex, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// -- HELPERS -------------------------------------------------------------------

fn generate_token() -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("{:x}", ts ^ 0xDEADBEEFCAFEBABE)
}

fn generate_session_id() -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    format!("DL-{:X}", ts)
}


/// HTTP/HTTPS POST using openssl s_client (available in Railway container)
fn http_post(url: &str, body: &str) -> Result<String, String> {
    let is_https = url.starts_with("https://");
    let url_clean = url.trim_start_matches("https://").trim_start_matches("http://");
    let slash_pos = url_clean.find('/').unwrap_or(url_clean.len());
    let host_port = &url_clean[..slash_pos];
    let path      = if slash_pos < url_clean.len() { &url_clean[slash_pos..] } else { "/" };
    let host      = host_port.split(':').next().unwrap_or(host_port);
    let port: u16 = host_port.split(':').nth(1).and_then(|p| p.parse().ok())
        .unwrap_or(if is_https { 443 } else { 80 });

    let request = format!(
        "POST {} HTTP/1.0
Host: {}
Content-Type: application/x-www-form-urlencoded
Content-Length: {}
Connection: close

{}",
        path, host, body.len(), body
    );

    let output = if is_https {
        // Use openssl s_client for HTTPS -- available in Railway container
        use std::io::Write;
        let mut child = std::process::Command::new("openssl")
            .args(&["s_client", "-connect", &format!("{}:{}", host, port),
                    "-quiet", "-no_ign_eof"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| format!("openssl error: {}", e))?;
        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(request.as_bytes()).ok();
        }
        child.wait_with_output().map_err(|e| format!("openssl output error: {}", e))?
    } else {
        use std::io::{Read, Write};
        let addr = format!("{}:{}", host, port);
        let mut stream = std::net::TcpStream::connect(&addr)
            .map_err(|e| format!("Connect error: {}", e))?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(15))).ok();
        stream.write_all(request.as_bytes()).map_err(|e| format!("Write error: {}", e))?;
        let mut resp = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => resp.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        std::process::Output { status: std::os::unix::process::ExitStatusExt::from_raw(0), stdout: resp, stderr: vec![] }
    };

    let response = String::from_utf8_lossy(&output.stdout).to_string();
    if let Some(pos) = response.find("

") {
        Ok(response[pos + 4..].to_string())
    } else if response.contains("{") {
        // Try to find JSON directly
        if let Some(start) = response.find('{') {
            Ok(response[start..].to_string())
        } else {
            Ok(response)
        }
    } else {
        Err(format!("Unexpected response: {}", &response[..response.len().min(100)]))
    }
}

fn http_delete(url: &str, token: &str) -> Result<(), String> {
    use std::io::Write;

    let url_clean = url.trim_start_matches("https://").trim_start_matches("http://");
    let slash_pos = url_clean.find('/').unwrap_or(url_clean.len());
    let host_port = &url_clean[..slash_pos];
    let path      = if slash_pos < url_clean.len() { &url_clean[slash_pos..] } else { "/" };
    let host      = host_port.split(':').next().unwrap_or(host_port);
    let port: u16 = host_port.split(':').nth(1).and_then(|p| p.parse().ok()).unwrap_or(80);

    let addr = format!("{}:{}", host, port);
    if let Ok(mut stream) = std::net::TcpStream::connect(&addr) {
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).ok();
        let request = format!(
            "DELETE {} HTTP/1.0
Host: {}
Guacamole-Token: {}
Connection: close

",
            path, host, token
        );
        stream.write_all(request.as_bytes()).ok();
    }
    Ok(())
}

fn base64_decode(s: &str) -> Result<String, ()> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    let mut bytes = Vec::new();
    let chars: Vec<u8> = s.bytes().filter(|b| *b != b'=').collect();
    for chunk in chars.chunks(4) {
        let mut vals = [0u8; 4];
        for (i, &c) in chunk.iter().enumerate() {
            vals[i] = alphabet.iter().position(|&x| x == c).unwrap_or(0) as u8;
        }
        bytes.push((vals[0] << 2) | (vals[1] >> 4));
        if chunk.len() > 2 { bytes.push((vals[1] << 4) | (vals[2] >> 2)); }
        if chunk.len() > 3 { bytes.push((vals[2] << 6) | vals[3]); }
    }
    String::from_utf8(bytes).map_err(|_| ())
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn now_string() -> String {
    now_secs().to_string()
}

fn get_port() -> u16 {
    std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080)
}

fn get_admin_credentials() -> (String, String) {
    (
        std::env::var("ADMIN_USERNAME").unwrap_or_else(|_| "admin".to_string()),
        std::env::var("ADMIN_PASSWORD").unwrap_or_else(|_| "DL-Admin-2025!".to_string()),
    )
}

// -- SQLITE DATABASE -----------------------------------------------------------

fn init_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS orgs (
            org_id        TEXT PRIMARY KEY,
            display_name  TEXT NOT NULL,
            password      TEXT NOT NULL,
            is_diesellynk INTEGER NOT NULL DEFAULT 0,
            created_at    INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            session_id            TEXT PRIMARY KEY,
            org_id                TEXT NOT NULL,
            driver_token          TEXT NOT NULL,
            tech_token            TEXT NOT NULL,
            status                TEXT NOT NULL DEFAULT 'Awaiting Technician',
            command               TEXT NOT NULL DEFAULT '',
            requires_ack          INTEGER NOT NULL DEFAULT 0,
            customer_ack          INTEGER NOT NULL DEFAULT 0,
            customer_message      TEXT NOT NULL DEFAULT '',
            priority              TEXT NOT NULL DEFAULT 'info',
            show_modal            INTEGER NOT NULL DEFAULT 0,
            nexiq_connected       INTEGER NOT NULL DEFAULT 0,
            adapter_name          TEXT NOT NULL DEFAULT '',
            fault_codes           TEXT NOT NULL DEFAULT '[]',
            truck_info            TEXT,
            live_data             TEXT,
            service_requested     INTEGER NOT NULL DEFAULT 0,
            service_request_time  TEXT NOT NULL DEFAULT '',
            service_type          TEXT NOT NULL DEFAULT '',
            tablet_ws_url         TEXT NOT NULL DEFAULT '',
            guac_url              TEXT NOT NULL DEFAULT '',
            event_log             TEXT NOT NULL DEFAULT '[]',
            connected_techs       TEXT NOT NULL DEFAULT '[]',
            created_at            INTEGER NOT NULL,
            last_activity         INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS session_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id   TEXT NOT NULL,
            org_id       TEXT NOT NULL,
            started_at   TEXT NOT NULL,
            ended_at     TEXT NOT NULL,
            status       TEXT NOT NULL,
            service_type TEXT NOT NULL,
            fault_count  INTEGER NOT NULL DEFAULT 0,
            truck_info   TEXT,
            created_at   INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rate_limits (
            ip            TEXT NOT NULL,
            endpoint      TEXT NOT NULL,
            hits          INTEGER NOT NULL DEFAULT 1,
            window_start  INTEGER NOT NULL,
            PRIMARY KEY (ip, endpoint)
        );
    ")?;

    // Migrations -- add new columns to existing databases safely
    conn.execute_batch("
        ALTER TABLE sessions ADD COLUMN guac_url TEXT NOT NULL DEFAULT '';
    ").ok(); // ok() ignores error if column already exists

    // Seed default orgs if not exists
    let dl_exists: bool = conn.query_row(
        "SELECT COUNT(*) FROM orgs WHERE org_id = 'diesellynk'",
        [], |r| r.get::<_, i64>(0)
    ).unwrap_or(0) > 0;

    if !dl_exists {
        let dl_pass = std::env::var("DL_MASTER_PASSWORD")
            .unwrap_or_else(|_| "DL-master-2025!".to_string());
        conn.execute(
            "INSERT OR IGNORE INTO orgs (org_id, display_name, password, is_diesellynk, created_at)
             VALUES (?1, ?2, ?3, 1, ?4)",
            params!["diesellynk", "DieselLynk Internal", dl_pass, now_secs() as i64],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO orgs (org_id, display_name, password, is_diesellynk, created_at)
             VALUES (?1, ?2, ?3, 0, ?4)",
            params!["demo_fleet", "Demo Fleet", "demo1234", now_secs() as i64],
        )?;
    }

    // Load any orgs from ORG_PASSWORDS env var
    if let Ok(env_val) = std::env::var("ORG_PASSWORDS") {
        for entry in env_val.split(',') {
            let parts: Vec<&str> = entry.splitn(2, ':').collect();
            if parts.len() == 2 {
                let id   = parts[0].trim();
                let pass = parts[1].trim();
                conn.execute(
                    "INSERT OR IGNORE INTO orgs (org_id, display_name, password, is_diesellynk, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![id, id, pass, if id == "diesellynk" { 1 } else { 0 }, now_secs() as i64],
                )?;
            }
        }
    }

    Ok(())
}

type Db = Arc<Mutex<Connection>>;

fn open_db() -> Db {
    // Railway: set DATABASE_URL to a persistent volume path
    // Default: /app/diesellynk.db (works if Railway volume mounted)
    // Fallback: /tmp/diesellynk.db (ephemeral -- survives restarts but not redeploys)
    let path = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        // Try /app first, fall back to /tmp
        if std::path::Path::new("/app").exists() {
            "/app/diesellynk.db".to_string()
        } else {
            "/tmp/diesellynk.db".to_string()
        }
    });
    let conn = Connection::open(&path).expect("Failed to open SQLite database");
    init_db(&conn).expect("Failed to initialize database schema");
    println!("[DB] SQLite opened at {}", path);
    Arc::new(Mutex::new(conn))
}

// -- DATA STRUCTURES -----------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EventEntry {
    timestamp: String,
    kind:      String,
    message:   String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FaultCode {
    spn:         u32,
    fmi:         u8,
    source:      String,
    description: String,
    active:      bool,
    count:       u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TruckInfo {
    vin:      String,
    make:     String,
    model:    String,
    year:     u16,
    engine:   String,
    odometer: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LiveData {
    engine_rpm:      Option<f32>,
    coolant_temp:    Option<f32>,
    oil_pressure:    Option<f32>,
    boost_pressure:  Option<f32>,
    vehicle_speed:   Option<f32>,
    battery_voltage: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SessionState {
    session_id:           String,
    org_id:               String,
    #[serde(skip_serializing)]
    driver_token:         String,
    #[serde(skip_serializing)]
    tech_token:           String,
    status:               String,
    command:              String,
    requires_ack:         bool,
    customer_ack:         bool,
    customer_message:     String,
    priority:             String,
    show_modal:           bool,
    nexiq_connected:      bool,
    adapter_name:         String,
    fault_codes:          Vec<FaultCode>,
    truck_info:           Option<TruckInfo>,
    live_data:            Option<LiveData>,
    service_requested:    bool,
    service_request_time: String,
    service_type:         String,
    tablet_ws_url:        String,
    guac_url:             String,   // Guacamole direct RDP connection URL
    event_log:            Vec<EventEntry>,
    connected_techs:      Vec<String>,  // list of tech org_ids currently connected
    last_activity:        u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TechSession {
    tech_auth_token: String,
    org_id:          String,
    is_diesellynk:   bool,
    created_at:      u64,
    last_seen:       u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdminSession {
    token:      String,
    created_at: u64,
}

// -- REQUEST/RESPONSE TYPES ----------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    org_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TechLoginRequest {
    org_id:   String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct TechJoinRequest {
    tech_auth_token: String,
}

#[derive(Debug, Deserialize)]
struct UpdateRequest {
    tech_token:   String,
    status:       String,
    command:      String,
    requires_ack: bool,
    priority:     String,
    show_modal:   bool,
}

#[derive(Debug, Deserialize)]
struct ActiveSessionsQuery {
    tech_auth_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CustomerReplyRequest {
    driver_token:     String,
    customer_ack:     bool,
    customer_message: String,
}

#[derive(Debug, Deserialize)]
struct ServiceRequest {
    driver_token: String,
    service_type: String,
}

#[derive(Debug, Deserialize)]
struct NexiqStatusRequest {
    driver_token: String,
    connected:    bool,
    adapter_name: String,
    truck_info:   Option<TruckInfo>,
    fault_codes:  Option<Vec<FaultCode>>,
    live_data:    Option<LiveData>,
}

#[derive(Debug, Deserialize)]
struct ManualTruckInfoRequest {
    driver_token: String,
    vin:          String,
    make:         String,
    model:        String,
    year:         u16,
    engine:       String,
    odometer:     Option<u32>,
}

#[derive(Debug, Serialize)]
struct CreateSessionResponse {
    session_id:   String,
    org_id:       String,
    driver_token: String,
    tech_token:   String,
    tech_url:     String,
    driver_url:   String,
}

#[derive(Debug, Serialize)]
struct ApiResponse {
    ok:      bool,
    message: String,
}

// Admin types
#[derive(Debug, Deserialize)]
struct AdminLoginRequest    { username: String, password: String }
#[derive(Debug, Deserialize)]
struct AdminAddOrgRequest   { org_id: String, password: String, display_name: String }
#[derive(Debug, Deserialize)]
struct AdminResetPasswordRequest { org_id: String, password: String }
#[derive(Debug, Deserialize)]
struct AdminRemoveOrgRequest     { org_id: String }
#[derive(Debug, Deserialize)]
struct AdminAuthQuery            { admin_token: Option<String> }

// -- RATE LIMITING -------------------------------------------------------------

struct RateLimiter {
    // in-memory: ip+endpoint -> (hits, window_start)
    buckets: HashMap<String, (u32, u64)>,
}

impl RateLimiter {
    fn new() -> Self { Self { buckets: HashMap::new() } }

    // Returns true if request should be allowed
    fn check(&mut self, key: &str, limit: u32, window_secs: u64) -> bool {
        let now = now_secs();
        let entry = self.buckets.entry(key.to_string()).or_insert((0, now));

        if now - entry.1 >= window_secs {
            // New window
            *entry = (1, now);
            true
        } else if entry.0 < limit {
            entry.0 += 1;
            true
        } else {
            false
        }
    }

    // Clean up old buckets periodically
    fn cleanup(&mut self) {
        let now = now_secs();
        self.buckets.retain(|_, (_, window_start)| now - *window_start < 3600);
    }
}

// -- APP STATE -----------------------------------------------------------------

struct AppState {
    db:             Db,
    sessions:       Mutex<HashMap<String, SessionState>>,
    tech_sessions:  Mutex<HashMap<String, TechSession>>,
    admin_sessions: Mutex<HashMap<String, AdminSession>>,
    rate_limiter:   Mutex<RateLimiter>,
    broadcaster:    Addr<WsBroadcaster>,
}

// -- SESSION DB HELPERS --------------------------------------------------------

fn save_session(db: &Db, s: &SessionState) {
    let conn = db.lock().unwrap();
    let fault_codes  = serde_json::to_string(&s.fault_codes).unwrap_or_default();
    let truck_info   = s.truck_info.as_ref().and_then(|t| serde_json::to_string(t).ok());
    let live_data    = s.live_data.as_ref().and_then(|l| serde_json::to_string(l).ok());
    let event_log    = serde_json::to_string(&s.event_log).unwrap_or_default();
    let conn_techs   = serde_json::to_string(&s.connected_techs).unwrap_or_default();

    conn.execute(
        "INSERT OR REPLACE INTO sessions
         (session_id, org_id, driver_token, tech_token, status, command,
          requires_ack, customer_ack, customer_message, priority, show_modal,
          nexiq_connected, adapter_name, fault_codes, truck_info, live_data,
          service_requested, service_request_time, service_type, tablet_ws_url,
          guac_url, event_log, connected_techs, created_at, last_activity)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25)",
        params![
            s.session_id, s.org_id, s.driver_token, s.tech_token,
            s.status, s.command,
            s.requires_ack as i32, s.customer_ack as i32,
            s.customer_message, s.priority, s.show_modal as i32,
            s.nexiq_connected as i32, s.adapter_name,
            fault_codes, truck_info, live_data,
            s.service_requested as i32, s.service_request_time,
            s.service_type, s.tablet_ws_url, s.guac_url,
            event_log, conn_techs,
            now_secs() as i64, s.last_activity as i64,
        ],
    ).ok();
}

fn load_sessions_from_db(db: &Db) -> HashMap<String, SessionState> {
    let conn = db.lock().unwrap();
    let mut map = HashMap::new();

    let mut stmt = match conn.prepare(
        "SELECT session_id, org_id, driver_token, tech_token, status, command,
                requires_ack, customer_ack, customer_message, priority, show_modal,
                nexiq_connected, adapter_name, fault_codes, truck_info, live_data,
                service_requested, service_request_time, service_type, tablet_ws_url,
                guac_url, event_log, connected_techs, last_activity
         FROM sessions WHERE last_activity > ?1"
    ) {
        Ok(s) => s,
        Err(_) => return map,
    };

    // Only load sessions active in last 12 hours
    let cutoff = (now_secs() - 43200) as i64;
    let rows = stmt.query_map(params![cutoff], |row| {
        Ok((
            row.get::<_, String>(0)?,   // session_id
            row.get::<_, String>(1)?,   // org_id
            row.get::<_, String>(2)?,   // driver_token
            row.get::<_, String>(3)?,   // tech_token
            row.get::<_, String>(4)?,   // status
            row.get::<_, String>(5)?,   // command
            row.get::<_, i32>(6)?,      // requires_ack
            row.get::<_, i32>(7)?,      // customer_ack
            row.get::<_, String>(8)?,   // customer_message
            row.get::<_, String>(9)?,   // priority
            row.get::<_, i32>(10)?,     // show_modal
            row.get::<_, i32>(11)?,     // nexiq_connected
            row.get::<_, String>(12)?,  // adapter_name
            row.get::<_, String>(13)?,  // fault_codes
            row.get::<_, Option<String>>(14)?, // truck_info
            row.get::<_, Option<String>>(15)?, // live_data
            row.get::<_, i32>(16)?,     // service_requested
            row.get::<_, String>(17)?,  // service_request_time
            row.get::<_, String>(18)?,  // service_type
            row.get::<_, String>(19)?,  // tablet_ws_url
            row.get::<_, String>(20)?,  // guac_url
            row.get::<_, String>(21)?,  // event_log
            row.get::<_, String>(22)?,  // connected_techs
            row.get::<_, i64>(23)?,     // last_activity
        ))
    });

    if let Ok(rows) = rows {
        for row in rows.flatten() {
            let s = SessionState {
                session_id:           row.0.clone(),
                org_id:               row.1,
                driver_token:         row.2,
                tech_token:           row.3,
                status:               row.4,
                command:              row.5,
                requires_ack:         row.6 != 0,
                customer_ack:         row.7 != 0,
                customer_message:     row.8,
                priority:             row.9,
                show_modal:           row.10 != 0,
                nexiq_connected:      row.11 != 0,
                adapter_name:         row.12,
                fault_codes:          serde_json::from_str(&row.13).unwrap_or_default(),
                truck_info:           row.14.and_then(|s| serde_json::from_str(&s).ok()),
                live_data:            row.15.and_then(|s| serde_json::from_str(&s).ok()),
                service_requested:    row.16 != 0,
                service_request_time: row.17,
                service_type:         row.18,
                tablet_ws_url:        row.19,
                guac_url:             row.20,
                event_log:            serde_json::from_str(&row.21).unwrap_or_default(),
                connected_techs:      serde_json::from_str(&row.22).unwrap_or_default(),
                last_activity:        row.23 as u64,
            };
            map.insert(row.0, s);
        }
    }
    map
}

fn save_history(db: &Db, s: &SessionState) {
    let conn = db.lock().unwrap();
    let truck_info = s.truck_info.as_ref().and_then(|t| serde_json::to_string(t).ok());
    let started_at = s.event_log.first().map(|e| e.timestamp.clone()).unwrap_or_default();
    conn.execute(
        "INSERT INTO session_history
         (session_id, org_id, started_at, ended_at, status, service_type, fault_count, truck_info, created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            s.session_id, s.org_id, started_at, now_string(),
            s.status, s.service_type, s.fault_codes.len() as i64,
            truck_info, now_secs() as i64,
        ],
    ).ok();
}

// Org DB helpers
fn load_orgs_from_db(db: &Db) -> HashMap<String, (String, String, bool)> {
    // Returns: org_id -> (password, display_name, is_diesellynk)
    let conn = db.lock().unwrap();
    let mut map = HashMap::new();
    if let Ok(mut stmt) = conn.prepare("SELECT org_id, password, display_name, is_diesellynk FROM orgs") {
        if let Ok(rows) = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i32>(3)?))
        }) {
            for row in rows.flatten() {
                map.insert(row.0, (row.1, row.2, row.3 != 0));
            }
        }
    }
    map
}

// -- SESSION HELPERS -----------------------------------------------------------

fn default_session(session_id: String, org_id: String) -> SessionState {
    SessionState {
        session_id: session_id.clone(),
        org_id,
        driver_token:         generate_token(),
        tech_token:           generate_token(),
        status:               "Awaiting Technician".to_string(),
        command:              "Session created -- standby for tech connection".to_string(),
        requires_ack:         false,
        customer_ack:         false,
        customer_message:     String::new(),
        priority:             "info".to_string(),
        show_modal:           false,
        nexiq_connected:      false,
        adapter_name:         String::new(),
        fault_codes:          Vec::new(),
        truck_info:           None,
        live_data:            None,
        service_requested:    false,
        service_request_time: String::new(),
        service_type:         String::new(),
        tablet_ws_url:        String::new(),
        guac_url:             String::new(),
        event_log:            Vec::new(),
        connected_techs:      Vec::new(),
        last_activity:        now_secs(),
    }
}

fn serialize_session(session: &SessionState) -> String {
    serde_json::to_string(session).unwrap_or_else(|_| "{}".to_string())
}

fn push_event(session: &mut SessionState, kind: &str, message: String) {
    session.event_log.push(EventEntry {
        timestamp: now_string(),
        kind: kind.to_string(),
        message,
    });
    if session.event_log.len() > 200 { session.event_log.remove(0); }
    session.last_activity = now_secs();
}

fn validate_tech_token(tech_sessions: &HashMap<String, TechSession>, token: &str) -> Option<TechSession> {
    tech_sessions.get(token).filter(|t| {
        // Tech sessions expire after 8 hours of inactivity
        now_secs() - t.last_seen < 28800
    }).cloned()
}

fn validate_admin_token(admin_sessions: &HashMap<String, AdminSession>, token: &str) -> bool {
    admin_sessions.get(token)
        .map(|s| now_secs() - s.created_at < 86400) // 24h expiry
        .unwrap_or(false)
}

fn check_rate_limit(state: &web::Data<AppState>, ip: &str, endpoint: &str, limit: u32, window: u64) -> bool {
    let key = format!("{}:{}", ip, endpoint);
    let mut rl = state.rate_limiter.lock().unwrap();
    rl.check(&key, limit, window)
}

fn get_client_ip(req: &HttpRequest) -> String {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .unwrap_or("unknown")
        .trim()
        .to_string()
}

// -- WEBSOCKET BROADCASTER -----------------------------------------------------

#[derive(Message)] #[rtype(result = "usize")]
struct Connect { room: String, addr: Recipient<WsMessage> }

#[derive(Message)] #[rtype(result = "()")]
struct Disconnect { room: String, id: usize }

#[derive(Message, Clone)] #[rtype(result = "()")]
struct Broadcast { room: String, message: String }

#[derive(Message, Clone)] #[rtype(result = "()")]
struct WsMessage(pub String);

struct WsBroadcaster {
    rooms:   HashMap<String, HashMap<usize, Recipient<WsMessage>>>,
    next_id: usize,
}

impl WsBroadcaster {
    fn new() -> Self { Self { rooms: HashMap::new(), next_id: 1 } }
}

impl Actor for WsBroadcaster { type Context = Context<Self>; }

impl Handler<Connect> for WsBroadcaster {
    type Result = usize;
    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> usize {
        let id = self.next_id; self.next_id += 1;
        self.rooms.entry(msg.room).or_insert_with(HashMap::new).insert(id, msg.addr);
        id
    }
}

impl Handler<Disconnect> for WsBroadcaster {
    type Result = ();
    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        if let Some(room) = self.rooms.get_mut(&msg.room) {
            room.remove(&msg.id);
            if room.is_empty() { self.rooms.remove(&msg.room); }
        }
    }
}

impl Handler<Broadcast> for WsBroadcaster {
    type Result = ();
    fn handle(&mut self, msg: Broadcast, _: &mut Context<Self>) {
        if let Some(room) = self.rooms.get(&msg.room) {
            for r in room.values() { let _ = r.do_send(WsMessage(msg.message.clone())); }
        }
    }
}

// -- WEBSOCKET SESSION ---------------------------------------------------------

struct WsSession {
    id: usize, room: String, hb: Instant,
    broadcaster: Addr<WsBroadcaster>, initial_state: String,
}

impl WsSession {
    fn new(room: String, broadcaster: Addr<WsBroadcaster>, initial_state: String) -> Self {
        Self { id: 0, room, hb: Instant::now(), broadcaster, initial_state }
    }
    fn start_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            if Instant::now().duration_since(act.hb) > Duration::from_secs(30) {
                act.broadcaster.do_send(Disconnect { room: act.room.clone(), id: act.id });
                ctx.stop(); return;
            }
            ctx.ping(b"");
        });
    }
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_heartbeat(ctx);
        let addr = ctx.address();
        self.broadcaster.send(Connect { room: self.room.clone(), addr: addr.recipient() })
            .into_actor(self)
            .map(|res, act, ctx| match res {
                Ok(id) => { act.id = id; ctx.text(act.initial_state.clone()); }
                Err(_) => ctx.stop(),
            })
            .wait(ctx);
    }
    fn stopped(&mut self, _: &mut Self::Context) {
        self.broadcaster.do_send(Disconnect { room: self.room.clone(), id: self.id });
    }
}

impl Handler<WsMessage> for WsSession {
    type Result = ();
    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) { ctx.text(msg.0); }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(m))       => { self.hb = Instant::now(); ctx.pong(&m); }
            Ok(ws::Message::Pong(_))       => { self.hb = Instant::now(); }
            Ok(ws::Message::Text(_))       => { self.hb = Instant::now(); }
            Ok(ws::Message::Binary(_))     => { self.hb = Instant::now(); }
            Ok(ws::Message::Close(reason)) => { ctx.close(reason); ctx.stop(); }
            Err(_) => ctx.stop(),
            _ => {}
        }
    }
}

// -- API ROUTES ----------------------------------------------------------------

/// POST /api/tech-login
#[post("/api/tech-login")]
async fn tech_login(
    req: HttpRequest,
    data: web::Data<AppState>,
    payload: web::Json<TechLoginRequest>,
) -> impl Responder {
    let ip = get_client_ip(&req);
    // Rate limit: 10 attempts per minute per IP
    if !check_rate_limit(&data, &ip, "tech-login", 10, 60) {
        return HttpResponse::TooManyRequests().json(ApiResponse {
            ok: false, message: "Too many login attempts -- please wait".to_string()
        });
    }

    let org_id   = payload.org_id.trim().to_lowercase();
    let password = payload.password.trim();

    let orgs = load_orgs_from_db(&data.db);
    match orgs.get(&org_id) {
        Some((correct_pass, _, is_dl)) if correct_pass.as_str() == password => {
            let token = generate_token();
            let is_diesellynk = *is_dl;
            let tech_sess = TechSession {
                tech_auth_token: token.clone(),
                org_id: org_id.clone(),
                is_diesellynk,
                created_at: now_secs(),
                last_seen:  now_secs(),
            };
            data.tech_sessions.lock().unwrap().insert(token.clone(), tech_sess);
            println!("[Auth] Tech logged in: org={}", org_id);

            #[derive(Serialize)]
            struct R { ok: bool, tech_auth_token: String, org_id: String, is_diesellynk: bool }
            HttpResponse::Ok().json(R { ok: true, tech_auth_token: token, org_id, is_diesellynk })
        }
        _ => HttpResponse::Unauthorized().json(ApiResponse {
            ok: false, message: "Invalid org ID or password".to_string()
        }),
    }
}

/// POST /api/create-session
#[post("/api/create-session")]
async fn create_session(
    req: HttpRequest,
    data: web::Data<AppState>,
    payload: web::Json<CreateSessionRequest>,
) -> impl Responder {
    let ip = get_client_ip(&req);
    // Rate limit: 30 sessions per minute per IP (handles fleet boot-up storms)
    if !check_rate_limit(&data, &ip, "create-session", 30, 60) {
        return HttpResponse::TooManyRequests().json(ApiResponse {
            ok: false, message: "Too many session requests".to_string()
        });
    }

    let org_id = payload.org_id.clone()
        .unwrap_or_else(|| "unknown".to_string())
        .trim().to_lowercase();

    let session_id = generate_session_id();
    let session    = default_session(session_id.clone(), org_id.clone());

    let response = CreateSessionResponse {
        driver_token: session.driver_token.clone(),
        tech_token:   session.tech_token.clone(),
        tech_url:     format!("/tech.html?session={}&org={}", session_id, org_id),
        driver_url:   format!("/index.html?session={}&org={}", session_id, org_id),
        session_id:   session_id.clone(),
        org_id,
    };

    save_session(&data.db, &session);
    data.sessions.lock().unwrap().insert(session_id, session);
    HttpResponse::Ok().json(response)
}

/// GET /api/session/{session_id}
#[get("/api/session/{session_id}")]
async fn get_session(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let session_id = path.into_inner();
    let sessions = data.sessions.lock().unwrap();
    match sessions.get(&session_id) {
        Some(s) => HttpResponse::Ok().json(s),
        None    => HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
    }
}

/// POST /api/tech-join/{session_id}
#[post("/api/tech-join/{session_id}")]
async fn tech_join(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<TechJoinRequest>,
) -> impl Responder {
    let session_id = path.into_inner();

    let tech_sess = {
        let mut tech_sessions = data.tech_sessions.lock().unwrap();
        match validate_tech_token(&tech_sessions, &payload.tech_auth_token) {
            Some(ts) => {
                // Update last_seen
                if let Some(t) = tech_sessions.get_mut(&payload.tech_auth_token) {
                    t.last_seen = now_secs();
                }
                ts
            },
            None => return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid or expired token -- please log in again".to_string()
            }),
        }
    };

    let mut sessions = data.sessions.lock().unwrap();
    let session = match sessions.get_mut(&session_id) {
        Some(s) => s,
        None    => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
    };

    let can_access = if tech_sess.is_diesellynk {
        session.service_type == "diesellynk_tech" || session.service_type.is_empty()
    } else {
        session.org_id == tech_sess.org_id
    };

    if !can_access {
        return HttpResponse::Forbidden().json(ApiResponse {
            ok: false, message: "Access denied -- session belongs to a different organization".to_string()
        });
    }

    // Add tech to connected_techs list if not already there
    if !session.connected_techs.contains(&tech_sess.org_id) {
        session.connected_techs.push(tech_sess.org_id.clone());
        push_event(session, "tech_join", format!("Tech joined: {}", tech_sess.org_id));
        save_session(&data.db, session);
    }

    #[derive(Serialize)]
    struct R { session_id: String, tech_token: String, org_id: String }
    HttpResponse::Ok().json(R {
        session_id: session.session_id.clone(),
        tech_token: session.tech_token.clone(),
        org_id:     session.org_id.clone(),
    })
}

/// GET /api/active-sessions
#[get("/api/active-sessions")]
async fn active_sessions(
    data: web::Data<AppState>,
    query: web::Query<ActiveSessionsQuery>,
) -> impl Responder {
    let tech_sess = {
        let mut tech_sessions = data.tech_sessions.lock().unwrap();
        match query.tech_auth_token.as_deref().and_then(|t| validate_tech_token(&tech_sessions, t)) {
            Some(ts) => {
                if let Some(t) = query.tech_auth_token.as_deref()
                    .and_then(|tok| tech_sessions.get_mut(tok)) {
                    t.last_seen = now_secs();
                }
                ts
            },
            None => return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Please log in to view sessions".to_string()
            }),
        }
    };

    let sessions = data.sessions.lock().unwrap();

    #[derive(Serialize)]
    struct SessionSummary {
        session_id: String, org_id: String, status: String,
        service_requested: bool, service_type: String,
        nexiq_connected: bool, fault_code_count: usize,
        truck_info: Option<TruckInfo>, service_request_time: String,
        adapter_name: String, connected_tech_count: usize,
    }

    let summaries: Vec<SessionSummary> = sessions.values()
        .filter(|s| {
            if tech_sess.is_diesellynk {
                s.service_type == "diesellynk_tech" || s.org_id == "diesellynk"
            } else {
                s.org_id == tech_sess.org_id
            }
        })
        .map(|s| SessionSummary {
            session_id:           s.session_id.clone(),
            org_id:               s.org_id.clone(),
            status:               s.status.clone(),
            service_requested:    s.service_requested,
            service_type:         s.service_type.clone(),
            nexiq_connected:      s.nexiq_connected,
            fault_code_count:     s.fault_codes.len(),
            truck_info:           s.truck_info.clone(),
            service_request_time: s.service_request_time.clone(),
            adapter_name:         s.adapter_name.clone(),
            connected_tech_count: s.connected_techs.len(),
        })
        .collect();

    HttpResponse::Ok().json(summaries)
}

/// POST /api/update/{session_id}
#[post("/api/update/{session_id}")]
async fn update_session(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<UpdateRequest>,
) -> impl Responder {
    let session_id = path.into_inner();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
        };

        if session.tech_token != payload.tech_token {
            return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid tech token".to_string() });
        }

        session.status       = payload.status.clone();
        session.command      = payload.command.clone();
        session.requires_ack = payload.requires_ack;
        session.priority     = payload.priority.clone();
        session.show_modal   = payload.show_modal;
        session.customer_ack = false;
        session.customer_message = String::new();

        if payload.status == "Technician Disconnected" || payload.status == "Session Complete" {
            session.service_requested    = false;
            session.service_request_time = String::new();
            session.service_type         = String::new();
            session.connected_techs.clear();

            // Delete Guacamole RDP connection so remote access is revoked
            if !session.guac_url.is_empty() {
                let guac_url_clone = session.guac_url.clone();
                let guac_base = std::env::var("GUAC_URL").unwrap_or_default();
                let guac_user = std::env::var("GUAC_USER").unwrap_or_else(|_| "guacadmin".to_string());
                let guac_pass = std::env::var("GUAC_PASS").unwrap_or_default();

                if !guac_base.is_empty() && !guac_pass.is_empty() {
                    // Extract connection ID from guac_url
                    // URL format: https://guac.../guacamole/#/client/BASE64
                    // BASE64 decodes to: CONNECTION_ID\0c\0datasource
                    if let Some(encoded) = guac_url_clone.split("/#/client/").nth(1) {
                        // Pad base64 if needed
                        let padded = format!("{}{}", encoded, "==".repeat((4 - encoded.len() % 4) % 4));
                        if let Ok(decoded) = base64_decode(&padded) {
                            if let Some(conn_id) = decoded.split('\0').next() {
                                let token_url = format!("{}/api/tokens", guac_base.trim_end_matches('/'));
                                let body = format!("username={}&password={}", guac_user, guac_pass);
                                let del_url = format!("{}/api/session/data/postgresql/connections/{}", guac_base.trim_end_matches('/'), conn_id);
                                // Get token then delete connection using pure Rust HTTP
                                if let Ok(Ok(token_resp)) = std::thread::spawn(move || {
                                    http_post(&token_url, &body)
                                }).join() {
                                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&token_resp) {
                                        if let Some(token) = json.get("authToken").and_then(|v| v.as_str()) {
                                            let _ = http_delete(&del_url, token);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                session.guac_url = String::new();
            }
        }

        push_event(session, "tech_command", format!("Tech: '{}' | {}", session.command, session.priority));
        let json = serialize_session(session);
        save_session(&data.db, session);

        // Write history on disconnect or complete
        if payload.status == "Technician Disconnected" || payload.status == "Session Complete" {
            save_history(&data.db, session);
        }
        json
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Updated".to_string() })
}

/// POST /api/customer-reply/{session_id}
#[post("/api/customer-reply/{session_id}")]
async fn customer_reply(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<CustomerReplyRequest>,
) -> impl Responder {
    let session_id = path.into_inner();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
        };

        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid driver token".to_string() });
        }

        session.customer_ack     = payload.customer_ack;
        session.customer_message = payload.customer_message.clone();
        if payload.customer_ack { session.requires_ack = false; session.show_modal = false; }

        push_event(session, "driver_reply", format!("Driver: ack={} | '{}'", session.customer_ack, session.customer_message));
        let json = serialize_session(session);
        save_session(&data.db, session);
        json
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Reply received".to_string() })
}

/// POST /api/service-request/{session_id}
#[post("/api/service-request/{session_id}")]
async fn service_request(
    req: HttpRequest,
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<ServiceRequest>,
) -> impl Responder {
    let ip = get_client_ip(&req);
    // Rate limit: 5 service requests per minute per IP
    if !check_rate_limit(&data, &ip, "service-request", 5, 60) {
        return HttpResponse::TooManyRequests().json(ApiResponse {
            ok: false, message: "Too many requests".to_string()
        });
    }

    let session_id = path.into_inner();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
        };

        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid driver token".to_string() });
        }

        session.service_requested    = true;
        session.service_request_time = now_string();
        session.service_type         = payload.service_type.clone();
        session.status  = "Service Requested".to_string();
        session.command = "Connecting you to a technician -- please stand by".to_string();
        session.priority = "action".to_string();

        push_event(session, "service_request", format!("Driver requested service -- type: {}", session.service_type));
        let json = serialize_session(session);
        save_session(&data.db, session);
        json
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Service request sent".to_string() })
}

/// POST /api/nexiq-status/{session_id}
#[post("/api/nexiq-status/{session_id}")]
async fn nexiq_status(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<NexiqStatusRequest>,
) -> impl Responder {
    let session_id = path.into_inner();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
        };

        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid driver token".to_string() });
        }

        let was = session.nexiq_connected;
        session.nexiq_connected = payload.connected;
        session.adapter_name    = payload.adapter_name.clone();
        if let Some(t) = &payload.truck_info  { session.truck_info  = Some(t.clone()); }
        if let Some(c) = &payload.fault_codes { session.fault_codes = c.clone(); }
        if let Some(l) = &payload.live_data   { session.live_data   = Some(l.clone()); }

        if payload.connected && !was {
            push_event(session, "nexiq", format!("Nexiq connected: {}", session.adapter_name));
            if let Some(t) = &session.truck_info {
                push_event(session, "truck_id", format!("VIN: {} | {} {} {} {}", t.vin, t.year, t.make, t.model, t.engine));
            }
        } else if !payload.connected && was {
            push_event(session, "nexiq", "Nexiq disconnected".to_string());
        }
        if !session.fault_codes.is_empty() {
            push_event(session, "fault_codes", format!("{} fault code(s) detected", session.fault_codes.len()));
        }

        let json = serialize_session(session);
        save_session(&data.db, session);
        json
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Nexiq status updated".to_string() })
}

/// POST /api/manual-truck-info/{session_id}
/// Driver enters truck info manually when no Nexiq adapter available
#[post("/api/manual-truck-info/{session_id}")]
async fn manual_truck_info(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<ManualTruckInfoRequest>,
) -> impl Responder {
    let session_id = path.into_inner();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
        };

        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid driver token".to_string() });
        }

        session.truck_info = Some(TruckInfo {
            vin:      payload.vin.trim().to_uppercase(),
            make:     payload.make.trim().to_string(),
            model:    payload.model.trim().to_string(),
            year:     payload.year,
            engine:   payload.engine.trim().to_string(),
            odometer: payload.odometer,
        });

        push_event(session, "truck_id", format!(
            "Manual entry: {} {} {} {} (VIN: {})",
            payload.year, payload.make, payload.model, payload.engine, payload.vin
        ));

        let json = serialize_session(session);
        save_session(&data.db, session);
        json
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Truck info saved".to_string() })
}

/// GET /api/tablet-info/{org_id}
/// Returns the most recently active session for an org so tunnel_reg can find it
#[get("/api/tablet-info/{org_id}")]
async fn tablet_info(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> impl Responder {
    let org_id   = path.into_inner();
    let sessions = data.sessions.lock().unwrap();

    // Find most recently active session for this org
    let session = sessions.values()
        .filter(|s| s.org_id == org_id)
        .max_by_key(|s| s.last_activity);

    match session {
        Some(s) => HttpResponse::Ok().json(serde_json::json!({
            "session_id":   s.session_id,
            "driver_token": s.driver_token,
            "org_id":       s.org_id,
        })),
        None => HttpResponse::NotFound().json(ApiResponse {
            ok: false, message: "No active session for this org".to_string()
        }),
    }
}

/// POST /api/tablet-register/{session_id}
#[post("/api/tablet-register/{session_id}")]
async fn tablet_register(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<serde_json::Value>,
) -> impl Responder {
    let session_id    = path.into_inner();
    let driver_token  = payload.get("driver_token").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ws_url        = payload.get("ws_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let guac_url      = payload.get("guac_url").and_then(|v| v.as_str()).unwrap_or("").to_string();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
        };

        if session.driver_token != driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid driver token".to_string() });
        }

        session.tablet_ws_url = ws_url.clone();
        if !guac_url.is_empty() {
            session.guac_url = guac_url.clone();
        }
        push_event(session, "tablet", format!("Tablet registered tunnel: {}", ws_url));
        let json = serialize_session(session);
        save_session(&data.db, session);
        json
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Tablet registered".to_string() })
}

// -- ADMIN ROUTES --------------------------------------------------------------

/// POST /api/admin/login
#[post("/api/admin/login")]
async fn admin_login(
    req: HttpRequest,
    data: web::Data<AppState>,
    payload: web::Json<AdminLoginRequest>,
) -> impl Responder {
    let ip = get_client_ip(&req);
    if !check_rate_limit(&data, &ip, "admin-login", 5, 60) {
        return HttpResponse::TooManyRequests().json(ApiResponse {
            ok: false, message: "Too many attempts".to_string()
        });
    }

    let (admin_user, admin_pass) = get_admin_credentials();
    if payload.username.trim() == admin_user && payload.password.trim() == admin_pass {
        let token = generate_token();
        data.admin_sessions.lock().unwrap().insert(token.clone(), AdminSession {
            token: token.clone(), created_at: now_secs(),
        });
        #[derive(Serialize)]
        struct R { ok: bool, admin_token: String }
        HttpResponse::Ok().json(R { ok: true, admin_token: token })
    } else {
        HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid credentials".to_string() })
    }
}

/// GET /api/admin/orgs
#[get("/api/admin/orgs")]
async fn admin_get_orgs(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let orgs     = load_orgs_from_db(&data.db);
    let sessions = data.sessions.lock().unwrap();
    let techs    = data.tech_sessions.lock().unwrap();

    let list: Vec<serde_json::Value> = orgs.iter().map(|(id, (_, name, is_dl))| {
        serde_json::json!({
            "org_id":        id,
            "display_name":  name,
            "is_diesellynk": is_dl,
            "session_count": sessions.values().filter(|s| &s.org_id == id).count(),
            "active_techs":  techs.values().filter(|t| &t.org_id == id).count(),
            "password_set":  true,
        })
    }).collect();

    HttpResponse::Ok().json(list)
}

/// POST /api/admin/orgs/add
#[post("/api/admin/orgs/add")]
async fn admin_add_org(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
    payload: web::Json<AdminAddOrgRequest>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let org_id = payload.org_id.trim().to_lowercase();
    if org_id.is_empty() || payload.password.trim().is_empty() {
        return HttpResponse::BadRequest().json(ApiResponse { ok: false, message: "org_id and password required".to_string() });
    }

    let conn = data.db.lock().unwrap();
    match conn.execute(
        "INSERT INTO orgs (org_id, display_name, password, is_diesellynk, created_at) VALUES (?1,?2,?3,0,?4)",
        params![org_id, payload.display_name.trim(), payload.password.trim(), now_secs() as i64],
    ) {
        Ok(_)  => HttpResponse::Ok().json(ApiResponse { ok: true, message: format!("Org '{}' created", org_id) }),
        Err(e) => {
            if e.to_string().contains("UNIQUE") {
                HttpResponse::Conflict().json(ApiResponse { ok: false, message: "Org already exists".to_string() })
            } else {
                HttpResponse::InternalServerError().json(ApiResponse { ok: false, message: e.to_string() })
            }
        }
    }
}

/// POST /api/admin/orgs/remove
#[post("/api/admin/orgs/remove")]
async fn admin_remove_org(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
    payload: web::Json<AdminRemoveOrgRequest>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let org_id = payload.org_id.trim().to_lowercase();
    if org_id == "diesellynk" {
        return HttpResponse::BadRequest().json(ApiResponse { ok: false, message: "Cannot remove DieselLynk org".to_string() });
    }

    let conn = data.db.lock().unwrap();
    let rows = conn.execute("DELETE FROM orgs WHERE org_id = ?1", params![org_id]).unwrap_or(0);
    if rows > 0 {
        HttpResponse::Ok().json(ApiResponse { ok: true, message: format!("Org '{}' removed", org_id) })
    } else {
        HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Org not found".to_string() })
    }
}

/// POST /api/admin/orgs/reset-password
#[post("/api/admin/orgs/reset-password")]
async fn admin_reset_password(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
    payload: web::Json<AdminResetPasswordRequest>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let conn = data.db.lock().unwrap();
    let rows = conn.execute(
        "UPDATE orgs SET password = ?1 WHERE org_id = ?2",
        params![payload.password.trim(), payload.org_id.trim()],
    ).unwrap_or(0);

    if rows > 0 {
        HttpResponse::Ok().json(ApiResponse { ok: true, message: format!("Password updated for '{}'", payload.org_id) })
    } else {
        HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Org not found".to_string() })
    }
}

/// GET /api/admin/sessions
#[get("/api/admin/sessions")]
async fn admin_all_sessions(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let sessions = data.sessions.lock().unwrap();

    #[derive(Serialize)]
    struct S {
        session_id: String, org_id: String, status: String,
        service_requested: bool, service_type: String,
        nexiq_connected: bool, fault_code_count: usize,
        truck_info: Option<TruckInfo>, service_request_time: String,
        tablet_ws_url: String, connected_tech_count: usize,
    }

    let list: Vec<S> = sessions.values().map(|s| S {
        session_id:           s.session_id.clone(), org_id: s.org_id.clone(),
        status:               s.status.clone(), service_requested: s.service_requested,
        service_type:         s.service_type.clone(), nexiq_connected: s.nexiq_connected,
        fault_code_count:     s.fault_codes.len(), truck_info: s.truck_info.clone(),
        service_request_time: s.service_request_time.clone(),
        tablet_ws_url:        s.tablet_ws_url.clone(),
        connected_tech_count: s.connected_techs.len(),
    }).collect();

    HttpResponse::Ok().json(list)
}

/// GET /api/admin/stats
#[get("/api/admin/stats")]
async fn admin_stats(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let sessions  = data.sessions.lock().unwrap();
    let techs     = data.tech_sessions.lock().unwrap();
    let orgs      = load_orgs_from_db(&data.db);

    let history_count: i64 = {
        let conn = data.db.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM session_history", [], |r| r.get(0)).unwrap_or(0)
    };

    HttpResponse::Ok().json(serde_json::json!({
        "total_sessions":    sessions.len(),
        "active_techs":      techs.len(),
        "service_requested": sessions.values().filter(|s| s.service_requested).count(),
        "nexiq_connected":   sessions.values().filter(|s| s.nexiq_connected).count(),
        "total_orgs":        orgs.len(),
        "total_history":     history_count,
        "multi_tech_sessions": sessions.values().filter(|s| s.connected_techs.len() > 1).count(),
    }))
}

/// GET /api/admin/history
#[get("/api/admin/history")]
async fn admin_history(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = { let s = data.admin_sessions.lock().unwrap(); query.admin_token.as_deref().map(|t| validate_admin_token(&s, t)).unwrap_or(false) };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let conn = data.db.lock().unwrap();
    let mut stmt = match conn.prepare(
        "SELECT session_id, org_id, started_at, ended_at, status, service_type, fault_count, truck_info
         FROM session_history ORDER BY created_at DESC LIMIT 500"
    ) {
        Ok(s)  => s,
        Err(_) => return HttpResponse::Ok().json(Vec::<serde_json::Value>::new()),
    };

    let rows = stmt.query_map([], |r| {
        Ok(serde_json::json!({
            "session_id":   r.get::<_, String>(0)?,
            "org_id":       r.get::<_, String>(1)?,
            "started_at":   r.get::<_, String>(2)?,
            "ended_at":     r.get::<_, String>(3)?,
            "status":       r.get::<_, String>(4)?,
            "service_type": r.get::<_, String>(5)?,
            "fault_count":  r.get::<_, i64>(6)?,
            "truck_info":   r.get::<_, Option<String>>(7)?,
        }))
    });

    let history: Vec<serde_json::Value> = match rows {
        Ok(mapped) => mapped.flatten().collect(),
        Err(_)     => Vec::new(),
    };
    HttpResponse::Ok().json(history)
}

/// GET /api/guac-token -- calls Guacamole server-side, returns ONLY the token
/// Credentials never leave the server
#[get("/api/guac-token")]
async fn guac_token(
    data: web::Data<AppState>,
    query: web::Query<ActiveSessionsQuery>,
) -> impl Responder {
    let tech_sessions = data.tech_sessions.lock().unwrap();
    let valid = query.tech_auth_token.as_deref()
        .map(|t| validate_tech_token(&tech_sessions, t))
        .unwrap_or(None);
    drop(tech_sessions);

    if valid.is_none() {
        return HttpResponse::Unauthorized().json(ApiResponse {
            ok: false, message: "Please log in first".to_string()
        });
    }

    let guac_url  = std::env::var("GUAC_URL").unwrap_or_default();
    let guac_user = std::env::var("GUAC_USER").unwrap_or_else(|_| "guacadmin".to_string());
    let guac_pass = std::env::var("GUAC_PASS").unwrap_or_default();

    if guac_url.is_empty() || guac_pass.is_empty() {
        return HttpResponse::Ok().json(serde_json::json!({
            "ok": false, "message": "Guacamole not configured on server"
        }));
    }

    // Call Guacamole API server-side using pure Rust TCP -- no curl needed
    // Credentials never touch the browser
    let token_url = format!("{}/api/tokens", guac_url.trim_end_matches('/'));
    let body      = format!("username={}&password={}", guac_user, guac_pass);
    let guac_url_clone = guac_url.clone();

    let result = web::block(move || {
        http_post(&token_url, &body)
    }).await;

    match result {
        Ok(Ok(resp_str)) => {
            match serde_json::from_str::<serde_json::Value>(&resp_str) {
                Ok(json) => {
                    let token = json.get("authToken")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if token.is_empty() {
                        HttpResponse::Ok().json(serde_json::json!({
                            "ok": false, "message": "Guacamole login failed"
                        }))
                    } else {
                        // Return ONLY the token -- credentials never leave server
                        HttpResponse::Ok().json(serde_json::json!({
                            "ok": true,
                            "token": token,
                            "guac_url": guac_url_clone,
                        }))
                    }
                },
                Err(_) => HttpResponse::Ok().json(serde_json::json!({
                    "ok": false, "message": "Invalid Guacamole response"
                })),
            }
        },
        Ok(Err(e)) => HttpResponse::Ok().json(serde_json::json!({
            "ok": false, "message": format!("Guacamole error: {}", e)
        })),
        Err(e) => HttpResponse::Ok().json(serde_json::json!({
            "ok": false, "message": format!("Server error: {}", e)
        })),
    }
}

async fn ws_route(
    req: HttpRequest,
    stream: web::Payload,
    query: web::Query<HashMap<String, String>>,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let session_id = query.get("session").cloned().unwrap_or_else(|| "DL-default".to_string());
    let org_id     = query.get("org").cloned().unwrap_or_else(|| "unknown".to_string());

    let initial_state = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = sessions.entry(session_id.clone())
            .or_insert_with(|| {
                let s = default_session(session_id.clone(), org_id.clone());
                save_session(&data.db, &s);
                s
            });
        serialize_session(session)
    };

    ws::start(WsSession::new(session_id, data.broadcaster.clone(), initial_state), &req, stream)
}

// -- MAIN ----------------------------------------------------------------------

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("");
    println!("     DieselLynk Server v0.4.0              ");
    println!("     Phase 2 -- Production Ready            ");
    println!("");

    // Open database and load existing sessions back into memory
    let db = open_db();
    let sessions_from_db = load_sessions_from_db(&db);
    println!("[Boot] Loaded {} active sessions from database", sessions_from_db.len());

    let (admin_user, _) = get_admin_credentials();
    println!("[Admin] Admin username: {}", admin_user);

    let broadcaster = WsBroadcaster::new().start();
    let app_state   = web::Data::new(AppState {
        db:             db.clone(),
        sessions:       Mutex::new(sessions_from_db),
        tech_sessions:  Mutex::new(HashMap::new()),
        admin_sessions: Mutex::new(HashMap::new()),
        rate_limiter:   Mutex::new(RateLimiter::new()),
        broadcaster,
    });

    // Background cleanup task -- runs every 5 minutes using actix_rt
    let db_bg    = db.clone();
    let state_bg = app_state.clone();
    actix_web::rt::spawn(async move {
        loop {
            actix_web::rt::time::sleep(Duration::from_secs(300)).await;
            let cutoff = (now_secs() - 43200) as i64; // 12 hours
            // Remove expired sessions from memory
            let mut sessions = state_bg.sessions.lock().unwrap();
            let expired: Vec<String> = sessions.values()
                .filter(|s| s.last_activity < now_secs() - 43200)
                .map(|s| s.session_id.clone())
                .collect();
            for id in &expired {
                println!("[Cleanup] Expiring session: {}", id);
                sessions.remove(id);
            }
            drop(sessions);
            // Clean expired sessions from DB too
            if let Ok(conn) = db_bg.lock() {
                conn.execute("DELETE FROM sessions WHERE last_activity < ?1", params![cutoff]).ok();
                conn.execute("DELETE FROM rate_limits WHERE window_start < ?1", params![(now_secs() - 3600) as i64]).ok();
            }
            // Clean rate limiter buckets
            state_bg.rate_limiter.lock().unwrap().cleanup();
            // Clean expired tech sessions from memory
            let mut tech_sessions = state_bg.tech_sessions.lock().unwrap();
            tech_sessions.retain(|_, t| now_secs() - t.last_seen < 28800); // 8h
            drop(tech_sessions);
            // Clean expired admin sessions
            let mut admin_sessions = state_bg.admin_sessions.lock().unwrap();
            admin_sessions.retain(|_, a| now_secs() - a.created_at < 86400); // 24h
        }
    });

    let port = get_port();
    println!("[Server] Binding on port {}", port);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .app_data(web::JsonConfig::default().error_handler(|err, _| {
                let msg = format!("Invalid JSON: {}", err);
                actix_web::error::InternalError::from_response(
                    err,
                    HttpResponse::BadRequest().json(ApiResponse { ok: false, message: msg }),
                ).into()
            }))
            // Admin
            .service(admin_login)
            .service(admin_get_orgs)
            .service(admin_add_org)
            .service(admin_remove_org)
            .service(admin_reset_password)
            .service(admin_all_sessions)
            .service(admin_stats)
            .service(admin_history)
            // Tech
            .service(tech_login)
            .service(tech_join)
            .service(active_sessions)
            // Session
            .service(create_session)
            .service(get_session)
            .service(update_session)
            // Driver
            .service(customer_reply)
            .service(service_request)
            .service(manual_truck_info)
            // Nexiq agent
            .service(nexiq_status)
            .service(tablet_register)
            .service(tablet_info)
            // WebSocket
            .route("/ws", web::get().to(ws_route))
            // Guacamole token proxy
            .service(guac_token)
            // Static files
            .service(Files::new("/", "./static").index_file("index.html"))
    })
    .bind(format!("0.0.0.0:{}", port))?
    .run()
    .await
}
