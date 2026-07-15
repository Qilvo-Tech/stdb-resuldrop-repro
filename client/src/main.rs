//! Repro client for: the `v3.bsatn.spacetimedb` WS protocol silently drops
//! ReducerResult messages under scheduled-reducer load, while `v2` does not.
//!
//! Raw WebSocket client built on `tokio-tungstenite` (the same transport the
//! official SpacetimeDB Rust SDK uses internally). The SDK itself hardcodes the
//! v2 subprotocol at compile time and cannot negotiate v3, so a raw client is
//! the only way to exercise the v3 wire path — that is the point of this binary.
//! Passing `v2` uses the exact protocol the official SDK speaks, so the v2 run
//! doubles as a control: identical client, identical workload, only the
//! subprotocol string differs.
//!
//! Usage:
//!   spacetime publish --server local -p .. resultdrop   # from client/
//!   spacetime login
//!   cargo run --release -- v3 15 80 17   # protocol gap_ms n_calls payload_kb
//!   cargo run --release -- v2 15 80 17

use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, sleep_until, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const DB: &str = "resultdrop";

/// `spacetime login show --token` → the JWT (first whitespace token starting `eyJ`).
fn get_token() -> String {
    let out = Command::new("spacetime")
        .args(["login", "show", "--token"])
        .output()
        .expect("failed to run `spacetime login show --token`");
    let text = String::from_utf8_lossy(&out.stdout);
    text.split_whitespace()
        .find(|t| t.starts_with("eyJ"))
        .expect("no JWT in `spacetime login show --token` output")
        .to_string()
}

/// 16 random bytes as hex, from a splitmix64 seeded by the wall clock (avoids a
/// `rand` dependency — uniqueness per connect is all the server needs).
fn connection_id_hex() -> String {
    let mut s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let mut next = || {
        s = s.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    };
    let mut hex = String::with_capacity(32);
    for _ in 0..2 {
        hex.push_str(&format!("{:016x}", next()));
    }
    hex
}

/// ClientMessage::CallReducer (tag 0x03): request_id u32le, flags u8,
/// reducer name (u32le len + utf8), args (u32le len + BSATN args).
/// Args for `ping(seq: u32, payload: Vec<u8>)`: seq u32le, then payload as a
/// u32le length prefix + bytes. seq == request_id here.
fn ping_frame(rid: u32, payload: &[u8]) -> Vec<u8> {
    let name = b"ping";
    let mut args = Vec::with_capacity(8 + payload.len());
    args.extend_from_slice(&rid.to_le_bytes());
    args.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    args.extend_from_slice(payload);

    let mut f = Vec::with_capacity(13 + name.len() + args.len());
    f.push(0x03);
    f.extend_from_slice(&rid.to_le_bytes());
    f.push(0); // CallReducerFlags::Default
    f.extend_from_slice(&(name.len() as u32).to_le_bytes());
    f.extend_from_slice(name);
    f.extend_from_slice(&(args.len() as u32).to_le_bytes());
    f.extend_from_slice(&args);
    f
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let proto_short = args.next().unwrap_or_else(|| "v3".into());
    let gap = Duration::from_millis(args.next().and_then(|s| s.parse().ok()).unwrap_or(15));
    let n: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(80);
    let payload_kb: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(17);

    let proto = format!("{proto_short}.bsatn.spacetimedb");
    let token = get_token();
    let url = format!(
        "ws://127.0.0.1:3000/v1/database/{DB}/subscribe\
         ?connection_id={}&compression=None&confirmed=false",
        connection_id_hex()
    );

    let mut req = url.into_client_request().expect("bad url");
    req.headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_str(&proto).unwrap());
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );

    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect failed");
    let (mut write, mut read) = ws.split();

    // ReducerResult request_ids seen. Server frame = [compression u8][msg_type u8]
    // [payload]; compression=None so the tag is 0, msg_type 0x06 = ReducerResult,
    // request_id is the u32le right after msg_type.
    let got: Arc<Mutex<HashSet<u32>>> = Arc::new(Mutex::new(HashSet::new()));
    let got_r = got.clone();
    let reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = read.next().await {
            if let Message::Binary(b) = msg {
                if b.len() >= 6 && b[0] == 0 && b[1] == 0x06 {
                    let rid = u32::from_le_bytes([b[2], b[3], b[4], b[5]]);
                    got_r.lock().unwrap().insert(rid);
                }
            }
        }
    });

    let payload = vec![0xABu8; payload_kb * 1024];
    let frames: Vec<Vec<u8>> = (0..n).map(|i| ping_frame(i, &payload)).collect();

    let start = Instant::now();
    for (i, frame) in frames.into_iter().enumerate() {
        sleep_until(start + gap * i as u32).await;
        write
            .send(Message::Binary(frame.into()))
            .await
            .expect("send failed");
    }

    // Wait up to 10 s for stragglers.
    for _ in 0..50 {
        if got.lock().unwrap().len() as u32 >= n {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
    reader.abort();

    let got = got.lock().unwrap();
    let missing: Vec<u32> = (0..n).filter(|r| !got.contains(r)).collect();
    println!(
        "proto={proto} gap={}ms n={n} payload={payload_kb}KB: results={}/{n} missing={missing:?}",
        gap.as_millis(),
        got.len(),
    );
}
