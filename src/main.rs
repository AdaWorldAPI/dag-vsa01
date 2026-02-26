//! DAG-VSA01 — Zero-Copy XOR Mirror Node (Shard A)
//!
//! RAID-1 Database Availability Group
//!   vsa01 = Mirror A | vsa02 = Mirror B | vsa03 = Mirror C
//!   XOR verifies mirrors are identical: A ⊕ B == 0
//!
//! Data flow: ladybug-rs (LanceDB) ─Arrow IPC─→ dag-vsa01/02/03 ─→ S3

use std::env;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

// ═══════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════

struct Config {
    node_id: String,
    port: u16,
    ladybug_url: String,
    peer_urls: Vec<String>,
    s3_bucket: String,
    s3_prefix: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            node_id: env::var("NODE_ID").unwrap_or_else(|_| "vsa01".into()),
            port: env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            ladybug_url: env::var("LADYBUG_URL")
                .unwrap_or_else(|_| "http://ladybug-rs.railway.internal:8080".into()),
            peer_urls: env::var("PEER_URLS")
                .unwrap_or_default()
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            s3_bucket: env::var("S3_BUCKET").unwrap_or_else(|_| "ada-dag-backup".into()),
            s3_prefix: env::var("S3_PREFIX").unwrap_or_else(|_| "dag/".into()),
        }
    }
}

// ═══════════════════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════════════════

struct AppState {
    config: Config,
    started: Instant,
    shard: RwLock<Vec<u8>>,
    checksum: RwLock<String>,
    sync_count: RwLock<u64>,
}

// ═══════════════════════════════════════════════════════════
// XOR — NANOSECOND ZERO-COPY OPERATION
// ═══════════════════════════════════════════════════════════

/// XOR two byte slices. Processes 8 bytes at a time for auto-vectorization.
#[inline]
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    let len = a.len().max(b.len());
    let mut out = vec![0u8; len];
    let min = a.len().min(b.len());

    // u64 chunks — compiler auto-vectorizes to SIMD
    let words = min / 8;
    for i in 0..words {
        let o = i * 8;
        let va = u64::from_le_bytes(a[o..o + 8].try_into().unwrap());
        let vb = u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
        out[o..o + 8].copy_from_slice(&(va ^ vb).to_le_bytes());
    }
    for i in words * 8..min {
        out[i] = a[i] ^ b[i];
    }
    if a.len() > min {
        out[min..].copy_from_slice(&a[min..]);
    } else if b.len() > min {
        out[min..].copy_from_slice(&b[min..]);
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

// ═══════════════════════════════════════════════════════════
// HANDLERS
// ═══════════════════════════════════════════════════════════

/// GET /health
async fn health(State(s): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let shard = s.shard.read().await;
    let cs = s.checksum.read().await;
    let sc = s.sync_count.read().await;
    Json(serde_json::json!({
        "node": s.config.node_id,
        "mode": "raid1_mirror",
        "status": "ok",
        "uptime_s": s.started.elapsed().as_secs(),
        "shard_bytes": shard.len(),
        "checksum": *cs,
        "syncs": *sc,
    }))
}

/// POST /write — receive raw Arrow IPC bytes (mirror copy from ladybug-rs)
async fn write_shard(State(s): State<Arc<AppState>>, body: Bytes) -> Json<serde_json::Value> {
    let cs = sha256_hex(&body);
    let len = body.len();
    *s.shard.write().await = body.to_vec();
    *s.checksum.write().await = cs.clone();
    *s.sync_count.write().await += 1;
    Json(serde_json::json!({"ok": true, "bytes": len, "checksum": cs}))
}

/// GET /shard — return raw shard bytes (zero-copy Arrow IPC)
async fn read_shard(State(s): State<Arc<AppState>>) -> Result<Vec<u8>, StatusCode> {
    let d = s.shard.read().await;
    if d.is_empty() {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(d.clone())
    }
}

/// POST /pull — pull data from ladybug-rs endpoint
async fn pull(
    State(s): State<Arc<AppState>>,
    Json(input): Json<PullReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let url = format!("{}{}", s.config.ladybug_url, input.endpoint);
    let client = reqwest::Client::new();
    let resp = match input.body {
        Some(b) => {
            client
                .post(&url)
                .header("Accept", "application/vnd.apache.arrow.stream")
                .json(&b)
                .send()
                .await
        }
        None => {
            client
                .get(&url)
                .header("Accept", "application/vnd.apache.arrow.stream")
                .send()
                .await
        }
    }
    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let cs = sha256_hex(&bytes);
    let len = bytes.len();
    *s.shard.write().await = bytes.to_vec();
    *s.checksum.write().await = cs.clone();
    *s.sync_count.write().await += 1;
    Ok(Json(
        serde_json::json!({"ok": true, "bytes": len, "checksum": cs}),
    ))
}

/// POST /mirror — push current shard to all peer mirrors
async fn mirror(
    State(s): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let data = s.shard.read().await.clone();
    if data.is_empty() {
        return Err((StatusCode::NOT_FOUND, "no shard data to mirror".into()));
    }
    let client = reqwest::Client::new();
    let mut results = Vec::new();
    for url in &s.config.peer_urls {
        let resp = client
            .post(format!("{}/write", url))
            .body(data.clone())
            .send()
            .await;
        match resp {
            Ok(r) => results.push(serde_json::json!({"peer": url, "status": r.status().as_u16()})),
            Err(e) => results.push(serde_json::json!({"peer": url, "error": e.to_string()})),
        }
    }
    Ok(Json(
        serde_json::json!({"ok": true, "mirrored_to": results}),
    ))
}

/// POST /xor — XOR two base64-encoded byte arrays
async fn xor_handler(
    Json(input): Json<XorReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let a = B64
        .decode(&input.a)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let b = B64
        .decode(&input.b)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let r = xor_bytes(&a, &b);
    Ok(Json(serde_json::json!({
        "result": B64.encode(&r),
        "bytes": r.len(),
        "checksum": sha256_hex(&r),
        "op": "xor"
    })))
}

/// POST /xor/verify — RAID-1 verify: A ⊕ B == 0 means identical mirrors
async fn xor_verify_handler(
    Json(input): Json<XorVerifyReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let a = B64
        .decode(&input.a)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let b = B64
        .decode(&input.b)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let check = xor_bytes(&a, &b);
    let zeros = check.iter().filter(|&&x| x == 0).count();
    let valid = zeros == check.len();
    Ok(Json(serde_json::json!({
        "valid": valid,
        "identical_bytes": zeros,
        "total_bytes": check.len(),
        "op": "xor_verify"
    })))
}

/// POST /dag/verify — verify this node matches a peer (fetch + XOR)
async fn dag_verify(
    State(s): State<Arc<AppState>>,
    Json(input): Json<PeerReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let local = s.shard.read().await.clone();
    if local.is_empty() {
        return Err((StatusCode::NOT_FOUND, "no local shard data".into()));
    }
    let client = reqwest::Client::new();
    let remote = client
        .get(format!("{}/shard", input.peer_url))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let check = xor_bytes(&local, &remote);
    let zeros = check.iter().filter(|&&x| x == 0).count();
    let valid = zeros == check.len();
    Ok(Json(serde_json::json!({
        "valid": valid,
        "identical_bytes": zeros,
        "total_bytes": check.len(),
        "local_checksum": sha256_hex(&local),
        "remote_checksum": sha256_hex(&remote),
        "op": "dag_verify"
    })))
}

/// POST /dag/recover — recover from a peer mirror
async fn recover(
    State(s): State<Arc<AppState>>,
    Json(input): Json<PeerReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let client = reqwest::Client::new();
    let bytes = client
        .get(format!("{}/shard", input.peer_url))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let cs = sha256_hex(&bytes);
    let len = bytes.len();
    *s.shard.write().await = bytes.to_vec();
    *s.checksum.write().await = cs.clone();
    *s.sync_count.write().await += 1;
    Ok(Json(
        serde_json::json!({"ok": true, "bytes": len, "checksum": cs, "op": "recover"}),
    ))
}

/// POST /s3/backup — upload current shard to S3
async fn s3_backup(
    State(s): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let data = s.shard.read().await.clone();
    if data.is_empty() {
        return Err((StatusCode::NOT_FOUND, "no shard data".into()));
    }
    let key = format!("{}{}/shard.arrow", s.config.s3_prefix, s.config.node_id);
    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&sdk);
    client
        .put_object()
        .bucket(&s.config.s3_bucket)
        .key(&key)
        .body(aws_sdk_s3::primitives::ByteStream::from(data.clone()))
        .send()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(
        serde_json::json!({"ok": true, "bytes": data.len(), "s3_key": key}),
    ))
}

/// POST /s3/restore — download shard from S3
async fn s3_restore(
    State(s): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let key = format!("{}{}/shard.arrow", s.config.s3_prefix, s.config.node_id);
    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&sdk);
    let resp = client
        .get_object()
        .bucket(&s.config.s3_bucket)
        .key(&key)
        .send()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_bytes()
        .to_vec();
    let cs = sha256_hex(&data);
    let len = data.len();
    *s.shard.write().await = data;
    *s.checksum.write().await = cs.clone();
    Ok(Json(
        serde_json::json!({"ok": true, "bytes": len, "checksum": cs, "s3_key": key}),
    ))
}

// ═══════════════════════════════════════════════════════════
// MODELS
// ═══════════════════════════════════════════════════════════

#[derive(Deserialize)]
struct PullReq {
    endpoint: String,
    body: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct XorReq {
    a: String,
    b: String,
}

#[derive(Deserialize)]
struct XorVerifyReq {
    a: String,
    b: String,
}

#[derive(Deserialize)]
struct PeerReq {
    peer_url: String,
}

// ═══════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::from_env();
    let addr = format!("0.0.0.0:{}", config.port);
    eprintln!("DAG-{} [raid1_mirror] → http://{}", config.node_id, addr);
    eprintln!("  ladybug: {}", config.ladybug_url);
    eprintln!("  peers:   {:?}", config.peer_urls);
    eprintln!("  s3:      s3://{}/{}", config.s3_bucket, config.s3_prefix);

    let state = Arc::new(AppState {
        config,
        started: Instant::now(),
        shard: RwLock::new(Vec::new()),
        checksum: RwLock::new(String::new()),
        sync_count: RwLock::new(0),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/write", post(write_shard))
        .route("/shard", get(read_shard))
        .route("/pull", post(pull))
        .route("/mirror", post(mirror))
        .route("/xor", post(xor_handler))
        .route("/xor/verify", post(xor_verify_handler))
        .route("/dag/verify", post(dag_verify))
        .route("/dag/recover", post(recover))
        .route("/s3/backup", post(s3_backup))
        .route("/s3/restore", post(s3_restore))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
