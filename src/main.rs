use actix::prelude::*;
use actix_files::Files;
use actix_web::{
    get, post, web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_actors::ws;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ── TOKEN / ID GENERATORS ─────────────────────────────────────────────────────

fn generate_token() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", ts ^ 0xDEADBEEFCAFEBABE)
}

fn generate_session_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("DL-{:X}", ts)
}

fn now_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

// ── ORG REGISTRY ─────────────────────────────────────────────────────────────
// Org types:
//   "diesellynk" — DieselLynk internal techs, see ALL diesellynk_tech requests
//   anything else — company org, sees only their own sessions
//
// To add orgs without redeploying, set env var:
//   ORG_PASSWORDS="diesellynk:masterpass,fleet_abc:theirpass,acme:acmepass"

fn load_org_registry() -> HashMap<String, OrgRecord> {
    let mut map = HashMap::new();

    let defaults = vec![
        ("diesellynk", "DL-master-2025!", "DieselLynk Internal", true),
        ("demo_fleet",  "demo1234",        "Demo Fleet",          false),
    ];

    for (id, pass, name, is_dl) in defaults {
        map.insert(id.to_string(), OrgRecord {
            org_id:        id.to_string(),
            password:      pass.to_string(),
            display_name:  name.to_string(),
            created_at:    now_string(),
            is_diesellynk: is_dl,
            tech_count:    0,
            session_count: 0,
        });
    }

    if let Ok(env_val) = std::env::var("ORG_PASSWORDS") {
        for entry in env_val.split(',') {
            let parts: Vec<&str> = entry.splitn(2, ':').collect();
            if parts.len() == 2 {
                let id = parts[0].trim().to_string();
                map.insert(id.clone(), OrgRecord {
                    org_id:        id.clone(),
                    password:      parts[1].trim().to_string(),
                    display_name:  id.clone(),
                    created_at:    now_string(),
                    is_diesellynk: id == "diesellynk",
                    tech_count:    0,
                    session_count: 0,
                });
            }
        }
    }
    map
}

fn get_admin_credentials() -> (String, String) {
    let username = std::env::var("ADMIN_USERNAME").unwrap_or_else(|_| "admin".to_string());
    let password = std::env::var("ADMIN_PASSWORD").unwrap_or_else(|_| "DL-Admin-2025!".to_string());
    (username, password)
}

fn validate_admin_token(admin_sessions: &HashMap<String, AdminSession>, token: &str) -> bool {
    admin_sessions.contains_key(token)
}

// ── DATA STRUCTURES ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EventEntry {
    timestamp: String,
    kind: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FaultCode {
    spn: u32,
    fmi: u8,
    source: String,
    description: String,
    active: bool,
    count: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TruckInfo {
    vin: String,
    make: String,
    model: String,
    year: u16,
    engine: String,
    odometer: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LiveData {
    engine_rpm: Option<f32>,
    coolant_temp: Option<f32>,
    oil_pressure: Option<f32>,
    boost_pressure: Option<f32>,
    vehicle_speed: Option<f32>,
    battery_voltage: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SessionState {
    session_id: String,
    org_id: String,

    #[serde(skip_serializing)]
    driver_token: String,
    #[serde(skip_serializing)]
    tech_token: String,

    status: String,
    command: String,
    requires_ack: bool,
    customer_ack: bool,
    customer_message: String,
    priority: String,
    show_modal: bool,

    nexiq_connected: bool,
    adapter_name: String,
    fault_codes: Vec<FaultCode>,
    truck_info: Option<TruckInfo>,
    live_data: Option<LiveData>,

    service_requested: bool,
    service_request_time: String,
    service_type: String,

    tablet_ws_url: String,   // noVNC websockify URL registered by the tablet

    event_log: Vec<EventEntry>,
}

// ── TECH AUTH SESSION ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TechSession {
    tech_auth_token: String,
    org_id: String,
    is_diesellynk: bool,
}

// ── REQUEST / RESPONSE TYPES ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    org_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TechLoginRequest {
    org_id: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct TechJoinRequest {
    tech_auth_token: String,
}

#[derive(Debug, Deserialize)]
struct UpdateRequest {
    tech_token: String,
    status: String,
    command: String,
    requires_ack: bool,
    priority: String,
    show_modal: bool,
}

#[derive(Debug, Deserialize)]
struct ActiveSessionsQuery {
    tech_auth_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CustomerReplyRequest {
    driver_token: String,
    customer_ack: bool,
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
    connected: bool,
    adapter_name: String,
    truck_info: Option<TruckInfo>,
    fault_codes: Option<Vec<FaultCode>>,
    live_data: Option<LiveData>,
}

#[derive(Debug, Serialize)]
struct CreateSessionResponse {
    session_id: String,
    org_id: String,
    driver_token: String,
    tech_token: String,
    tech_url: String,
    driver_url: String,
}

// ── ADMIN STRUCTURES ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrgRecord {
    org_id:       String,
    password:     String,
    display_name: String,
    created_at:   String,
    is_diesellynk: bool,
    tech_count:   usize,
    session_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionHistoryEntry {
    session_id:   String,
    org_id:       String,
    started_at:   String,
    ended_at:     String,
    status:       String,
    service_type: String,
    fault_count:  usize,
    truck_info:   Option<TruckInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdminSession {
    token:      String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct AdminLoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct AdminAddOrgRequest {
    org_id:       String,
    password:     String,
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct AdminResetPasswordRequest {
    org_id:   String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct AdminRemoveOrgRequest {
    org_id: String,
}

#[derive(Debug, Deserialize)]
struct AdminAuthQuery {
    admin_token: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiResponse {
    ok: bool,
    message: String,
}

// ── APP STATE ─────────────────────────────────────────────────────────────────

struct AppState {
    sessions:        Mutex<HashMap<String, SessionState>>,
    tech_sessions:   Mutex<HashMap<String, TechSession>>,
    org_registry:    Mutex<HashMap<String, OrgRecord>>,
    admin_sessions:  Mutex<HashMap<String, AdminSession>>,
    session_history: Mutex<Vec<SessionHistoryEntry>>,
    broadcaster:     Addr<WsBroadcaster>,
}

// ── SESSION HELPERS ───────────────────────────────────────────────────────────

fn default_session(session_id: String, org_id: String) -> SessionState {
    SessionState {
        session_id: session_id.clone(),
        org_id,
        driver_token: generate_token(),
        tech_token:   generate_token(),
        status:  "Awaiting Technician".to_string(),
        command: "Session created — standby for tech connection".to_string(),
        requires_ack: false,
        customer_ack: false,
        customer_message: String::new(),
        priority: "info".to_string(),
        show_modal: false,
        nexiq_connected: false,
        adapter_name: String::new(),
        fault_codes: Vec::new(),
        truck_info: None,
        live_data: None,
        service_requested: false,
        service_request_time: String::new(),
        service_type: String::new(),
        tablet_ws_url: String::new(),
        event_log: Vec::new(),
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
    if session.event_log.len() > 100 { session.event_log.remove(0); }
}

fn validate_tech_token(tech_sessions: &HashMap<String, TechSession>, token: &str) -> Option<TechSession> {
    tech_sessions.get(token).cloned()
}
// ── WEBSOCKET BROADCASTER ─────────────────────────────────────────────────────

#[derive(Message)] #[rtype(result = "usize")]
struct Connect { room: String, addr: Recipient<WsMessage> }

#[derive(Message)] #[rtype(result = "()")]
struct Disconnect { room: String, id: usize }

#[derive(Message, Clone)] #[rtype(result = "()")]
struct Broadcast { room: String, message: String }

#[derive(Message, Clone)] #[rtype(result = "()")]
struct WsMessage(pub String);

struct WsBroadcaster {
    rooms: HashMap<String, HashMap<usize, Recipient<WsMessage>>>,
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

// ── WEBSOCKET SESSION ─────────────────────────────────────────────────────────

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
            if Instant::now().duration_since(act.hb) > Duration::from_secs(20) {
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
            Ok(ws::Message::Ping(m))        => { self.hb = Instant::now(); ctx.pong(&m); }
            Ok(ws::Message::Pong(_))        => { self.hb = Instant::now(); }
            Ok(ws::Message::Text(_))        => { self.hb = Instant::now(); }
            Ok(ws::Message::Binary(_))      => { self.hb = Instant::now(); }
            Ok(ws::Message::Close(reason))  => { ctx.close(reason); ctx.stop(); }
            Err(_) => ctx.stop(),
            _ => {}
        }
    }
}

// ── API ROUTES ────────────────────────────────────────────────────────────────

/// POST /api/tablet-register/{session_id}
/// Tablet registers its Cloudflare tunnel URL so tech can connect remote desktop
#[post("/api/tablet-register/{session_id}")]
async fn tablet_register(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<serde_json::Value>,
) -> impl Responder {
    let session_id = path.into_inner();

    let driver_token = payload.get("driver_token")
        .and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ws_url = payload.get("ws_url")
        .and_then(|v| v.as_str()).unwrap_or("").to_string();

    let json_to_broadcast = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = match sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return HttpResponse::NotFound().json(ApiResponse {
                ok: false, message: "Session not found".to_string()
            }),
        };

        if session.driver_token != driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid driver token".to_string()
            });
        }

        session.tablet_ws_url = ws_url.clone();
        push_event(session, "tablet", format!("Tablet registered tunnel: {}", ws_url));
        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    println!("[Tablet] Registered WS URL for session {}: {}", session_id, ws_url);
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Tablet registered".to_string() })
}

/// POST /api/tech-login
#[post("/api/tech-login")]
async fn tech_login(
    data: web::Data<AppState>,
    payload: web::Json<TechLoginRequest>,
) -> impl Responder {
    let org_id   = payload.org_id.trim().to_lowercase();
    let password = payload.password.trim();

    let org = {
        let registry = data.org_registry.lock().unwrap();
        registry.get(&org_id).cloned()
    };

    match org {
        Some(org) if org.password.as_str() == password => {
            let token = generate_token();
            let is_diesellynk = org.is_diesellynk;
            let tech_sess = TechSession {
                tech_auth_token: token.clone(),
                org_id: org_id.clone(),
                is_diesellynk,
            };
            data.tech_sessions.lock().unwrap().insert(token.clone(), tech_sess);
            println!("[Auth] Tech logged in: org={}", org_id);

            #[derive(Serialize)]
            struct LoginResponse { ok: bool, tech_auth_token: String, org_id: String, is_diesellynk: bool }
            HttpResponse::Ok().json(LoginResponse { ok: true, tech_auth_token: token, org_id, is_diesellynk })
        }
        _ => HttpResponse::Unauthorized().json(ApiResponse {
            ok: false, message: "Invalid org ID or password".to_string()
        }),
    }
}

/// POST /api/create-session
#[post("/api/create-session")]
async fn create_session(
    data: web::Data<AppState>,
    payload: web::Json<CreateSessionRequest>,
) -> impl Responder {
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

    data.sessions.lock().unwrap().insert(session_id, session);
    HttpResponse::Ok().json(response)
}

/// GET /api/session/{session_id}
#[get("/api/session/{session_id}")]
async fn get_session(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let sessions = data.sessions.lock().unwrap();
    match sessions.get(&path.into_inner()) {
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
        let tech_sessions = data.tech_sessions.lock().unwrap();
        match validate_tech_token(&tech_sessions, &payload.tech_auth_token) {
            Some(ts) => ts,
            None => return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid or expired token — please log in again".to_string()
            }),
        }
    };

    let sessions = data.sessions.lock().unwrap();
    let session  = match sessions.get(&session_id) {
        Some(s) => s,
        None    => return HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Session not found".to_string() }),
    };

    // Access control
    let can_access = if tech_sess.is_diesellynk {
        session.service_type == "diesellynk_tech" || session.service_type.is_empty()
    } else {
        session.org_id == tech_sess.org_id
    };

    if !can_access {
        return HttpResponse::Forbidden().json(ApiResponse {
            ok: false, message: "Access denied — session belongs to a different organization".to_string()
        });
    }

    #[derive(Serialize)]
    struct TechJoinResponse { session_id: String, tech_token: String, org_id: String }
    HttpResponse::Ok().json(TechJoinResponse {
        session_id: session.session_id.clone(),
        tech_token: session.tech_token.clone(),
        org_id: session.org_id.clone(),
    })
}

/// GET /api/active-sessions?tech_auth_token=...
#[get("/api/active-sessions")]
async fn active_sessions(
    data: web::Data<AppState>,
    query: web::Query<ActiveSessionsQuery>,
) -> impl Responder {
    let tech_sess = {
        let tech_sessions = data.tech_sessions.lock().unwrap();
        match query.tech_auth_token.as_deref().and_then(|t| validate_tech_token(&tech_sessions, t)) {
            Some(ts) => ts,
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
    }

    let summaries: Vec<SessionSummary> = sessions.values()
        .filter(|s| {
            if tech_sess.is_diesellynk {
                s.service_type == "diesellynk_tech" || (s.service_requested && s.org_id == "diesellynk")
            } else {
                s.org_id == tech_sess.org_id
            }
        })
        .map(|s| SessionSummary {
            session_id: s.session_id.clone(), org_id: s.org_id.clone(),
            status: s.status.clone(), service_requested: s.service_requested,
            service_type: s.service_type.clone(), nexiq_connected: s.nexiq_connected,
            fault_code_count: s.fault_codes.len(), truck_info: s.truck_info.clone(),
            service_request_time: s.service_request_time.clone(),
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

        if payload.status == "Technician Disconnected" {
            session.service_requested    = false;
            session.service_request_time = String::new();
            session.service_type         = String::new();
        }

        push_event(session, "tech_command", format!("Tech: '{}' | {}", session.command, session.priority));
        serialize_session(session)
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
        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Reply received".to_string() })
}

/// POST /api/service-request/{session_id}
#[post("/api/service-request/{session_id}")]
async fn service_request(
    path: web::Path<String>,
    data: web::Data<AppState>,
    payload: web::Json<ServiceRequest>,
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

        session.service_requested    = true;
        session.service_request_time = now_string();
        session.service_type         = payload.service_type.clone();
        session.status  = "Service Requested".to_string();
        session.command = "Connecting you to a technician — please stand by".to_string();
        session.priority = "action".to_string();

        push_event(session, "service_request", format!("Driver requested service — type: {}", session.service_type));
        serialize_session(session)
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

        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast { room: session_id.clone(), message: json_to_broadcast });
    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Nexiq status updated".to_string() })
}

/// POST /api/admin/login
#[post("/api/admin/login")]
async fn admin_login(
    data: web::Data<AppState>,
    payload: web::Json<AdminLoginRequest>,
) -> impl Responder {
    let (admin_user, admin_pass) = get_admin_credentials();
    if payload.username.trim() == admin_user && payload.password.trim() == admin_pass {
        let token = generate_token();
        data.admin_sessions.lock().unwrap().insert(token.clone(), AdminSession {
            token: token.clone(),
            created_at: now_string(),
        });
        println!("[Admin] Login successful");
        #[derive(Serialize)]
        struct R { ok: bool, admin_token: String }
        HttpResponse::Ok().json(R { ok: true, admin_token: token })
    } else {
        HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Invalid admin credentials".to_string() })
    }
}

/// GET /api/admin/orgs
#[get("/api/admin/orgs")]
async fn admin_get_orgs(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = {
        let sessions = data.admin_sessions.lock().unwrap();
        query.admin_token.as_deref().map(|t| validate_admin_token(&sessions, t)).unwrap_or(false)
    };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let registry  = data.org_registry.lock().unwrap();
    let sessions  = data.sessions.lock().unwrap();
    let tech_sess = data.tech_sessions.lock().unwrap();

    let orgs: Vec<serde_json::Value> = registry.values().map(|org| {
        let session_count = sessions.values().filter(|s| s.org_id == org.org_id).count();
        let tech_count    = tech_sess.values().filter(|t| t.org_id == org.org_id).count();
        serde_json::json!({
            "org_id":        org.org_id,
            "display_name":  org.display_name,
            "created_at":    org.created_at,
            "is_diesellynk": org.is_diesellynk,
            "session_count": session_count,
            "active_techs":  tech_count,
        })
    }).collect();

    HttpResponse::Ok().json(orgs)
}

/// POST /api/admin/orgs/add
#[post("/api/admin/orgs/add")]
async fn admin_add_org(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
    payload: web::Json<AdminAddOrgRequest>,
) -> impl Responder {
    let ok = {
        let sessions = data.admin_sessions.lock().unwrap();
        query.admin_token.as_deref().map(|t| validate_admin_token(&sessions, t)).unwrap_or(false)
    };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let org_id = payload.org_id.trim().to_lowercase();
    if org_id.is_empty() || payload.password.trim().is_empty() {
        return HttpResponse::BadRequest().json(ApiResponse { ok: false, message: "org_id and password required".to_string() });
    }

    let mut registry = data.org_registry.lock().unwrap();
    if registry.contains_key(&org_id) {
        return HttpResponse::Conflict().json(ApiResponse { ok: false, message: "Org already exists".to_string() });
    }

    registry.insert(org_id.clone(), OrgRecord {
        org_id:        org_id.clone(),
        password:      payload.password.trim().to_string(),
        display_name:  payload.display_name.trim().to_string(),
        created_at:    now_string(),
        is_diesellynk: false,
        tech_count:    0,
        session_count: 0,
    });

    println!("[Admin] New org added: {}", org_id);
    HttpResponse::Ok().json(ApiResponse { ok: true, message: format!("Org '{}' created", org_id) })
}

/// POST /api/admin/orgs/remove
#[post("/api/admin/orgs/remove")]
async fn admin_remove_org(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
    payload: web::Json<AdminRemoveOrgRequest>,
) -> impl Responder {
    let ok = {
        let sessions = data.admin_sessions.lock().unwrap();
        query.admin_token.as_deref().map(|t| validate_admin_token(&sessions, t)).unwrap_or(false)
    };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let org_id = payload.org_id.trim().to_lowercase();
    if org_id == "diesellynk" {
        return HttpResponse::BadRequest().json(ApiResponse { ok: false, message: "Cannot remove DieselLynk org".to_string() });
    }

    let mut registry = data.org_registry.lock().unwrap();
    if registry.remove(&org_id).is_some() {
        println!("[Admin] Org removed: {}", org_id);
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
    let ok = {
        let sessions = data.admin_sessions.lock().unwrap();
        query.admin_token.as_deref().map(|t| validate_admin_token(&sessions, t)).unwrap_or(false)
    };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let org_id = payload.org_id.trim().to_lowercase();
    let mut registry = data.org_registry.lock().unwrap();

    if let Some(org) = registry.get_mut(&org_id) {
        org.password = payload.password.trim().to_string();
        println!("[Admin] Password reset for org: {}", org_id);
        HttpResponse::Ok().json(ApiResponse { ok: true, message: format!("Password updated for '{}'", org_id) })
    } else {
        HttpResponse::NotFound().json(ApiResponse { ok: false, message: "Org not found".to_string() })
    }
}

/// GET /api/admin/sessions — all sessions across all orgs
#[get("/api/admin/sessions")]
async fn admin_all_sessions(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = {
        let admin_sessions = data.admin_sessions.lock().unwrap();
        query.admin_token.as_deref().map(|t| validate_admin_token(&admin_sessions, t)).unwrap_or(false)
    };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let sessions = data.sessions.lock().unwrap();

    #[derive(Serialize)]
    struct SessionSummary {
        session_id: String, org_id: String, status: String,
        service_requested: bool, service_type: String,
        nexiq_connected: bool, fault_code_count: usize,
        truck_info: Option<TruckInfo>, service_request_time: String,
        tablet_ws_url: String,
    }

    let list: Vec<SessionSummary> = sessions.values().map(|s| SessionSummary {
        session_id: s.session_id.clone(), org_id: s.org_id.clone(),
        status: s.status.clone(), service_requested: s.service_requested,
        service_type: s.service_type.clone(), nexiq_connected: s.nexiq_connected,
        fault_code_count: s.fault_codes.len(), truck_info: s.truck_info.clone(),
        service_request_time: s.service_request_time.clone(),
        tablet_ws_url: s.tablet_ws_url.clone(),
    }).collect();

    HttpResponse::Ok().json(list)
}

/// GET /api/admin/stats
#[get("/api/admin/stats")]
async fn admin_stats(
    data: web::Data<AppState>,
    query: web::Query<AdminAuthQuery>,
) -> impl Responder {
    let ok = {
        let admin_sessions = data.admin_sessions.lock().unwrap();
        query.admin_token.as_deref().map(|t| validate_admin_token(&admin_sessions, t)).unwrap_or(false)
    };
    if !ok { return HttpResponse::Unauthorized().json(ApiResponse { ok: false, message: "Unauthorized".to_string() }); }

    let sessions     = data.sessions.lock().unwrap();
    let tech_sess    = data.tech_sessions.lock().unwrap();
    let registry     = data.org_registry.lock().unwrap();
    let history      = data.session_history.lock().unwrap();

    let total_sessions      = sessions.len();
    let active_techs        = tech_sess.len();
    let service_requested   = sessions.values().filter(|s| s.service_requested).count();
    let nexiq_connected     = sessions.values().filter(|s| s.nexiq_connected).count();
    let total_orgs          = registry.len();
    let total_history       = history.len();

    HttpResponse::Ok().json(serde_json::json!({
        "total_sessions":    total_sessions,
        "active_techs":      active_techs,
        "service_requested": service_requested,
        "nexiq_connected":   nexiq_connected,
        "total_orgs":        total_orgs,
        "total_history":     total_history,
    }))
}

/// WebSocket endpoint
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
            .or_insert_with(|| default_session(session_id.clone(), org_id));
        serialize_session(session)
    };

    ws::start(WsSession::new(session_id, data.broadcaster.clone(), initial_state), &req, stream)
}

// ── MAIN ──────────────────────────────────────────────────────────────────────

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("╔═══════════════════════════════════════╗");
    println!("║     DieselLynk Server v0.3.0          ║");
    println!("║     Multi-Tenant Edition              ║");
    println!("╚═══════════════════════════════════════╝");

    let org_registry = load_org_registry();
    println!("[Orgs] {} org(s) loaded: {}", org_registry.len(),
        org_registry.keys().cloned().collect::<Vec<_>>().join(", "));

    let (admin_user, _) = get_admin_credentials();
    println!("[Admin] Admin username: {}", admin_user);

    let broadcaster = WsBroadcaster::new().start();
    let app_state   = web::Data::new(AppState {
        sessions:        Mutex::new(HashMap::new()),
        tech_sessions:   Mutex::new(HashMap::new()),
        org_registry:    Mutex::new(org_registry),
        admin_sessions:  Mutex::new(HashMap::new()),
        session_history: Mutex::new(Vec::new()),
        broadcaster,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            // Admin
            .service(admin_login)
            .service(admin_get_orgs)
            .service(admin_add_org)
            .service(admin_remove_org)
            .service(admin_reset_password)
            .service(admin_all_sessions)
            .service(admin_stats)
            // Tech auth
            .service(tech_login)
            .service(tablet_register)
            .service(create_session)
            .service(get_session)
            .service(active_sessions)
            .service(update_session)
            .service(tech_join)
            .service(customer_reply)
            .service(service_request)
            .service(nexiq_status)
            .route("/ws", web::get().to(ws_route))
            .service(Files::new("/", "./static").index_file("index.html"))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
