//! Minimal repro module for: SpacetimeDB silently drops ReducerResult
//! messages to a WS caller when reducer calls arrive at high rate while a
//! scheduled reducer saturates the host (scheduled every 50ms, each run
//! takes ~100ms wall).

use spacetimedb::{reducer, table, ReducerContext, ScheduleAt, Table};

// one run burns ~100 ms wall on a modern x86 core (measured on 2.6.1)
const SPIN_ITERS: u64 = 40_000_000;

#[table(accessor = tick_timer, scheduled(tick))]
pub struct TickTimer {
    #[primary_key]
    #[auto_inc]
    scheduled_id: u64,
    scheduled_at: ScheduleAt,
}

#[table(accessor = ping_row)]
pub struct PingRow {
    #[primary_key]
    id: u32,
    seq: u32,
    payload_len: u32,
}

#[table(accessor = spin_config)]
pub struct SpinConfig {
    #[primary_key]
    id: u32,
    enabled: bool,
}

#[reducer(init)]
pub fn init(ctx: &ReducerContext) {
    ctx.db.spin_config().insert(SpinConfig { id: 0, enabled: true });
    ctx.db.tick_timer().insert(TickTimer {
        scheduled_id: 0,
        scheduled_at: ScheduleAt::Interval(std::time::Duration::from_millis(50).into()),
    });
}

/// Toggle host saturation: `spacetime call resultdrop set_spin true|false`.
#[reducer]
pub fn set_spin(ctx: &ReducerContext, enabled: bool) {
    ctx.db.spin_config().id().delete(0u32);
    ctx.db.spin_config().insert(SpinConfig { id: 0, enabled });
    log::info!("spin enabled = {}", enabled);
}

/// Burns ~100ms of deterministic CPU via a xorshift64 spin loop (when enabled).
/// The final value is logged so the loop can't be optimized away.
#[reducer]
pub fn tick(ctx: &ReducerContext, _timer: TickTimer) {
    let enabled = ctx
        .db
        .spin_config()
        .id()
        .find(0u32)
        .map(|c| c.enabled)
        .unwrap_or(true);
    if !enabled {
        return;
    }
    let mut x: u64 = 0x9E3779B97F4A7C15;
    for _ in 0..SPIN_ITERS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    log::info!("tick spin done, x={}", x);
}

/// Trivial reducer exercising high call rate with large argument frames
/// (payload can be sized ~17KB by the caller to match the real-world trigger).
#[reducer]
pub fn ping(ctx: &ReducerContext, seq: u32, payload: Vec<u8>) {
    let payload_len = payload.len() as u32;
    ctx.db.ping_row().id().delete(0u32);
    ctx.db.ping_row().insert(PingRow {
        id: 0,
        seq,
        payload_len,
    });
}
