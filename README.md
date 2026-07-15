# SpacetimeDB repro: v3 WS protocol silently drops ReducerResult under scheduled-reducer load (v2 unaffected)

## Summary

On the `v3.bsatn.spacetimedb` WebSocket protocol, the server **executes** reducer
calls but silently never sends the corresponding `ReducerResult` message back to
the caller when calls arrive at a moderate rate (~15 ms apart) while a scheduled
reducer is saturating the host. Loss is severe (~60% of results in this repro) and
permanent — the results never arrive, they are not merely delayed. The identical
client speaking `v2.bsatn.spacetimedb` receives 100% of results under the same
load. A small residual loss (~2/80) is observable on v3 even **without** host
saturation.

Discovered via a community Godot SDK (which speaks v3). The official Rust SDK
2.6.1 pins `ws::v2::BIN_PROTOCOL` and is therefore unaffected — which may be why
this hasn't surfaced upstream.

## Environment where reproduced

- SpacetimeDB CLI / local standalone server **2.6.1**
  (commit 052c83fe984a4c4eb7bb4f9afa5c6b1903891d87)
- Windows 11 Pro
- Client: raw-WebSocket Rust (`tokio-tungstenite` — the same transport the
  official Rust SDK uses internally)

## Why a raw client (the official Rust SDK cannot reproduce this)

The reproducing client must negotiate the `v3` subprotocol, and the official
`spacetimedb-sdk` 2.6.1 **cannot** — it is compile-time-locked to v2:

- The subprotocol header is a hardcoded `const { ws::v2::BIN_PROTOCOL }`
  (`websocket.rs`), with no config option, feature flag, or env override.
- The entire send/receive pipeline is typed against concrete `ws::v2::*` types
  (`ClientMessage`, `ServerMessage`, the `SpacetimeModule` trait bounds); it is
  not generic over protocol version and never references `ws::v3`.

So even forcing the v3 header would make it deserialize v3 wire bytes as v2
structs and error, not "run on v3". Reproducing v3 requires a raw client — hence
this one. Passing `v2` uses the exact protocol the official SDK speaks, so the
**v2 run is a built-in control**: identical client, identical workload, only the
subprotocol string differs. (This is also why the bug went unnoticed upstream:
Clockwork's own SDKs are v2-only. The community Godot SDK that hit it ships a v3
subprotocol header on a v2-typed parser.)

## Contents

- `src/lib.rs` + `Cargo.toml` — minimal module:
  - `tick` — scheduled every **50 ms**, busy-spins ~100 ms of CPU so the scheduler
    is permanently saturated. Toggle at runtime:
    `spacetime call resultdrop set_spin false`
  - `ping(seq: u32, payload: Vec<u8>)` — trivial single-row upsert
- `client/` — raw-WebSocket Rust client (`tokio-tungstenite`). Sends N
  `CallReducer(ping)` frames (17 KB payload each) at precise `gap_ms` intervals
  and counts distinct `ReducerResult` request_ids received. The **only**
  difference between the passing and failing run is the negotiated subprotocol
  string, passed as the first CLI arg.

## Steps

```bash
spacetime start                                   # local server on :3000
spacetime login                                   # client uses the CLI token
spacetime publish --server local -p . resultdrop

cd client
cargo run --release -- v2 15 80 17    # protocol gap_ms n_calls payload_kb
cargo run --release -- v3 15 80 17
```

## Results

| protocol | spin (50 ms sched, ~100 ms burn) | results received / 80 |
|---|---|---|
| v2 | ON  | **80/80** (repeated ×5) |
| v3 | ON  | **32–36/80** (repeated ×6) |
| v3 | OFF | 78/80 |
| v2 | OFF | 80/80 |

Sample failing output:

```
proto=v3.bsatn.spacetimedb gap=15ms n=80 payload=17KB: results=33/80
missing=[4, 5, 6, 7, 8, 9, 12, 15, 16, 17, 18, 19, 20, 25, 26, 27, 28, 29, 30,
 35, 36, 37, 38, 39, 40, 41, 46, 47, 48, 49, 50, 51, 56, 57, 58, 59, 60, 61,
 67, 68, 69, 70, 71, 72, 77, 78, 79]
```

## Observations

- The missing v3 results are lost in **consecutive blocks** whose period matches
  the tick cadence — calls landing during the spin window lose their results;
  calls in the inter-tick gap get them.
- **All 80 reducer calls execute** in every run: after a lossy v3 run,
  `spacetime sql --server local resultdrop "SELECT seq FROM ping_row"` shows
  `seq = 79` (the last call's write landed). Only the result *delivery* is lost.
- Verified at the wire level with a transparent TCP proxy that reads the
  server-facing socket eagerly (no client backpressure possible): the missing
  `ReducerResult` frames are never written to the socket by the server.
- Waiting 10+ s changes nothing — the results are dropped, not delayed.

## Impact

Any v3 client that fires a burst of reducer calls while the module has a busy
scheduled reducer (e.g. a game physics tick) permanently loses per-call
request/response tracking — pending-call maps leak and callers time out, with no
error surfaced anywhere (no server log, no client-visible message).

## Real-world context

Hit in a game project: a Godot client (community SpacetimeDB SDK, v3) doing a
content-sync burst of ~80 reducer calls against a module with a 20 Hz physics
tick lost 10–19 results per run, failing the sync with spurious timeouts.
