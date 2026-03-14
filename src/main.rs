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

// ── RANDOM TOKEN GENERATOR ────────────────────────────────────────────────────
// No external crate needed — uses system time + counter for unique tokens
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

// ── DATA STRUCTURES ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EventEntry {
    timestamp: String,
    kind: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FaultCode {
    spn: u32,           // Suspect Parameter Number (J1939)
    fmi: u8,            // Failure Mode Identifier
    source: String,     // ECU source address / module name
    description: String,
    active: bool,
    count: u8,          // occurrence count
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

    // ── Auth tokens (never sent to driver, only used server-side) ──
    #[serde(skip_serializing)]
    driver_token: String,
    #[serde(skip_serializing)]
    tech_token: String,

    // ── Session lifecycle ──
    status: String,
    command: String,
    requires_ack: bool,
    customer_ack: bool,
    customer_message: String,
    priority: String,
    show_modal: bool,

    // ── Nexiq / diagnostics ──
    nexiq_connected: bool,
    adapter_name: String,
    fault_codes: Vec<FaultCode>,
    truck_info: Option<TruckInfo>,
    live_data: Option<LiveData>,

    // ── Service request ──
    service_requested: bool,
    service_request_time: String,
    service_type: String,       // "own_tech" or "diesellynk_tech"

    // ── Event log ──
    event_log: Vec<EventEntry>,
}

// ── REQUEST / RESPONSE TYPES ──────────────────────────────────────────────────

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
struct CustomerReplyRequest {
    driver_token: String,
    customer_ack: bool,
    customer_message: String,
}

#[derive(Debug, Deserialize)]
struct ServiceRequest {
    driver_token: String,
    service_type: String,   // "own_tech" | "diesellynk_tech"
}

// Posted by Nexiq Windows agent running on the tablet
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
    driver_token: String,
    tech_token: String,
    tech_url: String,
    driver_url: String,
}

#[derive(Debug, Serialize)]
struct ApiResponse {
    ok: bool,
    message: String,
}

// ── APP STATE ─────────────────────────────────────────────────────────────────

struct AppState {
    sessions: Mutex<HashMap<String, SessionState>>,
    broadcaster: Addr<WsBroadcaster>,
}

// ── SESSION HELPERS ───────────────────────────────────────────────────────────

fn default_session(session_id: String) -> SessionState {
    SessionState {
        session_id: session_id.clone(),
        driver_token: generate_token(),
        tech_token: generate_token(),
        status: "Awaiting Technician".to_string(),
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
    if session.event_log.len() > 100 {
        session.event_log.remove(0);
    }
}

// ── WEBSOCKET BROADCASTER ─────────────────────────────────────────────────────

#[derive(Message)]
#[rtype(result = "usize")]
struct Connect {
    room: String,
    addr: Recipient<WsMessage>,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Disconnect {
    room: String,
    id: usize,
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct Broadcast {
    room: String,
    message: String,
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct WsMessage(pub String);

struct WsBroadcaster {
    rooms: HashMap<String, HashMap<usize, Recipient<WsMessage>>>,
    next_id: usize,
}

impl WsBroadcaster {
    fn new() -> Self {
        Self {
            rooms: HashMap::new(),
            next_id: 1,
        }
    }
}

impl Actor for WsBroadcaster {
    type Context = Context<Self>;
}

impl Handler<Connect> for WsBroadcaster {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.rooms
            .entry(msg.room)
            .or_insert_with(HashMap::new)
            .insert(id, msg.addr);
        id
    }
}

impl Handler<Disconnect> for WsBroadcaster {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        if let Some(room) = self.rooms.get_mut(&msg.room) {
            room.remove(&msg.id);
            if room.is_empty() {
                self.rooms.remove(&msg.room);
            }
        }
    }
}

impl Handler<Broadcast> for WsBroadcaster {
    type Result = ();

    fn handle(&mut self, msg: Broadcast, _: &mut Context<Self>) {
        if let Some(room) = self.rooms.get(&msg.room) {
            for recipient in room.values() {
                let _ = recipient.do_send(WsMessage(msg.message.clone()));
            }
        }
    }
}

// ── WEBSOCKET SESSION ─────────────────────────────────────────────────────────

struct WsSession {
    id: usize,
    room: String,
    hb: Instant,
    broadcaster: Addr<WsBroadcaster>,
    initial_state: String,
}

impl WsSession {
    fn new(room: String, broadcaster: Addr<WsBroadcaster>, initial_state: String) -> Self {
        Self { id: 0, room, hb: Instant::now(), broadcaster, initial_state }
    }

    fn start_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            if Instant::now().duration_since(act.hb) > Duration::from_secs(20) {
                act.broadcaster.do_send(Disconnect {
                    room: act.room.clone(),
                    id: act.id,
                });
                ctx.stop();
                return;
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
        self.broadcaster
            .send(Connect {
                room: self.room.clone(),
                addr: addr.recipient(),
            })
            .into_actor(self)
            .map(|res, act, ctx| {
                match res {
                    Ok(id) => { act.id = id; ctx.text(act.initial_state.clone()); }
                    Err(_) => ctx.stop(),
                }
            })
            .wait(ctx);
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        self.broadcaster.do_send(Disconnect {
            room: self.room.clone(),
            id: self.id,
        });
    }
}

impl Handler<WsMessage> for WsSession {
    type Result = ();
    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(msg)) => { self.hb = Instant::now(); ctx.pong(&msg); }
            Ok(ws::Message::Pong(_)) => { self.hb = Instant::now(); }
            Ok(ws::Message::Text(_)) => { self.hb = Instant::now(); }
            Ok(ws::Message::Binary(_)) => { self.hb = Instant::now(); }
            Ok(ws::Message::Close(reason)) => { ctx.close(reason); ctx.stop(); }
            Err(_) => ctx.stop(),
            _ => {}
        }
    }
}

// ── API ROUTES ────────────────────────────────────────────────────────────────

/// POST /api/create-session
/// Tech creates a session — gets back both tokens + URLs
#[post("/api/create-session")]
async fn create_session(data: web::Data<AppState>) -> impl Responder {
    let session_id = generate_session_id();
    let session = default_session(session_id.clone());

    let response = CreateSessionResponse {
        driver_token: session.driver_token.clone(),
        tech_token: session.tech_token.clone(),
        tech_url: format!("/tech.html?session={}&token={}", session_id, session.tech_token),
        driver_url: format!("/index.html?session={}&token={}", session_id, session.driver_token),
        session_id: session_id.clone(),
    };

    data.sessions.lock().unwrap().insert(session_id, session);
    HttpResponse::Ok().json(response)
}

/// GET /api/session/{session_id}
/// Returns session state — safe to expose (tokens stripped by serde skip)
#[get("/api/session/{session_id}")]
async fn get_session(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let session_id = path.into_inner();
    let sessions = data.sessions.lock().unwrap();

    match sessions.get(&session_id) {
        Some(session) => HttpResponse::Ok().json(session),
        None => HttpResponse::NotFound().json(ApiResponse {
            ok: false,
            message: "Session not found".to_string(),
        }),
    }
}

/// POST /api/update/{session_id}
/// Tech sends command to driver — requires tech_token
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
            None => return HttpResponse::NotFound().json(ApiResponse {
                ok: false, message: "Session not found".to_string()
            }),
        };

        // Verify tech token
        if session.tech_token != payload.tech_token {
            return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid tech token".to_string()
            });
        }

        session.status = payload.status.clone();
        session.command = payload.command.clone();
        session.requires_ack = payload.requires_ack;
        session.priority = payload.priority.clone();
        session.show_modal = payload.show_modal;
        session.customer_ack = false;
        session.customer_message = String::new();

        push_event(session, "tech_command", format!(
            "Tech: '{}' | {}", session.command, session.priority
        ));

        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast {
        room: session_id.clone(),
        message: json_to_broadcast,
    });

    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Updated".to_string() })
}

/// POST /api/customer-reply/{session_id}
/// Driver acknowledges tech command — requires driver_token
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
            None => return HttpResponse::NotFound().json(ApiResponse {
                ok: false, message: "Session not found".to_string()
            }),
        };

        // Verify driver token
        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid driver token".to_string()
            });
        }

        session.customer_ack = payload.customer_ack;
        session.customer_message = payload.customer_message.clone();

        if payload.customer_ack {
            session.requires_ack = false;
            session.show_modal = false;
        }

        push_event(session, "driver_reply", format!(
            "Driver: ack={} | '{}'",
            session.customer_ack, session.customer_message
        ));

        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast {
        room: session_id.clone(),
        message: json_to_broadcast,
    });

    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Reply received".to_string() })
}

/// POST /api/service-request/{session_id}
/// Driver taps "Request Service" — alerts tech
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
            None => return HttpResponse::NotFound().json(ApiResponse {
                ok: false, message: "Session not found".to_string()
            }),
        };

        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid driver token".to_string()
            });
        }

        session.service_requested = true;
        session.service_request_time = now_string();
        session.service_type = payload.service_type.clone();
        session.status = "Service Requested".to_string();
        session.command = "Connecting you to a technician — please stand by".to_string();
        session.priority = "action".to_string();

        push_event(session, "service_request", format!(
            "Driver requested service — type: {}", session.service_type
        ));

        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast {
        room: session_id.clone(),
        message: json_to_broadcast,
    });

    // TODO: Push notification to tech here (Phase 2)
    println!("SERVICE REQUEST for session {} — type: {}", session_id, payload.service_type);

    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Service request sent".to_string() })
}

/// POST /api/nexiq-status/{session_id}
/// Nexiq Windows agent posts adapter status, truck info, fault codes, live data
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
            None => return HttpResponse::NotFound().json(ApiResponse {
                ok: false, message: "Session not found".to_string()
            }),
        };

        if session.driver_token != payload.driver_token {
            return HttpResponse::Unauthorized().json(ApiResponse {
                ok: false, message: "Invalid driver token".to_string()
            });
        }

        let was_connected = session.nexiq_connected;
        session.nexiq_connected = payload.connected;
        session.adapter_name = payload.adapter_name.clone();

        if let Some(truck) = &payload.truck_info {
            session.truck_info = Some(truck.clone());
        }
        if let Some(codes) = &payload.fault_codes {
            session.fault_codes = codes.clone();
        }
        if let Some(live) = &payload.live_data {
            session.live_data = Some(live.clone());
        }

        // Log connection state change
        if payload.connected && !was_connected {
            push_event(session, "nexiq", format!(
                "Nexiq connected: {}", session.adapter_name
            ));
            if let Some(truck) = &session.truck_info {
                push_event(session, "truck_id", format!(
                    "VIN: {} | {} {} {} {}", truck.vin, truck.year, truck.make, truck.model, truck.engine
                ));
            }
        } else if !payload.connected && was_connected {
            push_event(session, "nexiq", "Nexiq disconnected".to_string());
        }

        if !session.fault_codes.is_empty() {
            push_event(session, "fault_codes", format!(
                "{} fault code(s) detected", session.fault_codes.len()
            ));
        }

        serialize_session(session)
    };

    data.broadcaster.do_send(Broadcast {
        room: session_id.clone(),
        message: json_to_broadcast,
    });

    HttpResponse::Ok().json(ApiResponse { ok: true, message: "Nexiq status updated".to_string() })
}

/// GET /api/active-sessions
/// Tech dashboard — lists all sessions with service requests pending
/// In production this would require admin auth
#[get("/api/active-sessions")]
async fn active_sessions(data: web::Data<AppState>) -> impl Responder {
    let sessions = data.sessions.lock().unwrap();

    #[derive(Serialize)]
    struct SessionSummary {
        session_id: String,
        status: String,
        service_requested: bool,
        service_type: String,
        nexiq_connected: bool,
        fault_code_count: usize,
        truck_info: Option<TruckInfo>,
        service_request_time: String,
    }

    let summaries: Vec<SessionSummary> = sessions.values().map(|s| SessionSummary {
        session_id: s.session_id.clone(),
        status: s.status.clone(),
        service_requested: s.service_requested,
        service_type: s.service_type.clone(),
        nexiq_connected: s.nexiq_connected,
        fault_code_count: s.fault_codes.len(),
        truck_info: s.truck_info.clone(),
        service_request_time: s.service_request_time.clone(),
    }).collect();

    HttpResponse::Ok().json(summaries)
}

/// WebSocket endpoint
async fn ws_route(
    req: HttpRequest,
    stream: web::Payload,
    query: web::Query<HashMap<String, String>>,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let session_id = query
        .get("session")
        .cloned()
        .unwrap_or_else(|| "DL-default".to_string());

    let initial_state = {
        let mut sessions = data.sessions.lock().unwrap();
        let session = sessions
            .entry(session_id.clone())
            .or_insert_with(|| default_session(session_id.clone()));
        serialize_session(session)
    };

    let ws = WsSession::new(session_id, data.broadcaster.clone(), initial_state);
    ws::start(ws, &req, stream)
}

// ── MAIN ──────────────────────────────────────────────────────────────────────

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("╔═══════════════════════════════════════╗");
    println!("║     DieselLynk Server v0.2.0          ║");
    println!("║     http://127.0.0.1:8080             ║");
    println!("╚═══════════════════════════════════════╝");

    let broadcaster = WsBroadcaster::new().start();

    let app_state = web::Data::new(AppState {
        sessions: Mutex::new(HashMap::new()),
        broadcaster,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            // Session management
            .service(create_session)
            .service(get_session)
            .service(active_sessions)
            // Tech commands
            .service(update_session)
            // Driver actions
            .service(customer_reply)
            .service(service_request)
            // Nexiq agent
            .service(nexiq_status)
            // WebSocket
            .route("/ws", web::get().to(ws_route))
            // Static files
            .service(Files::new("/", "./static").index_file("index.html"))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
