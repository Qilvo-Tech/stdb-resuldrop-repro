"""Repro client for: v3 WS protocol drops ReducerResult under scheduled-reducer load.

Sends N CallReducer(ping) frames at precise gap_ms intervals over a raw
SpacetimeDB WebSocket connection and counts distinct ReducerResult request_ids.

Usage:
    pip install websockets
    python ping_burst.py v2 15 80 17     # protocol gap_ms n_calls payload_kb -> 80/80
    python ping_burst.py v3 15 80 17     # -> ~33/80, missing in tick-period blocks

Requires a local server on 127.0.0.1:3000 with the `resultdrop` module published
and `spacetime login` done (uses the CLI token).
"""
import asyncio, gzip, os, struct, subprocess, sys, time
import websockets

PROTO = (sys.argv[1] if len(sys.argv) > 1 else "v3") + ".bsatn.spacetimedb"
GAP = float(sys.argv[2]) / 1000.0 if len(sys.argv) > 2 else 0.015
N = int(sys.argv[3]) if len(sys.argv) > 3 else 80
KB = int(sys.argv[4]) if len(sys.argv) > 4 else 17
DB = "resultdrop"


def get_token() -> str:
    out = subprocess.run(["spacetime", "login", "show", "--token"],
                         capture_output=True, text=True, shell=True).stdout
    return next(t for t in out.split() if t.startswith("eyJ"))


def dec(fr):  # server frame: u8 compression tag (0 none, 2 gzip) + payload
    return fr[1:] if fr[0] == 0 else gzip.decompress(fr[1:])


def ping_frame(rid: int, payload: bytes) -> bytes:
    # ClientMessage::CallReducer (tag 0x03): request_id u32le, flags u8,
    # reducer name (u32le len + utf8), args (u32le len + BSATN args)
    args = struct.pack("<I", rid) + struct.pack("<I", len(payload)) + payload
    r = b"ping"
    return (b"\x03" + struct.pack("<I", rid) + b"\x00"
            + struct.pack("<I", len(r)) + r
            + struct.pack("<I", len(args)) + args)


async def main():
    token = get_token()
    payload = os.urandom(KB * 1024)
    frames = [ping_frame(i, payload) for i in range(N)]
    url = (f"ws://127.0.0.1:3000/v1/database/{DB}/subscribe"
           f"?connection_id={os.urandom(16).hex()}&compression=None&confirmed=false")
    got = set()
    async with websockets.connect(
            url, subprotocols=[PROTO],
            additional_headers={"Authorization": f"Bearer {token}"},
            max_size=1 << 27, compression=None) as ws:
        dec(await ws.recv())  # IdentityToken

        async def reader():
            while True:
                p = dec(await ws.recv())
                if p[0] == 0x06:  # ReducerResult: msg_type, request_id u32le, ...
                    got.add(struct.unpack_from("<I", p, 1)[0])

        rt = asyncio.create_task(reader())
        start = time.perf_counter()
        for i, f in enumerate(frames):
            deadline = start + i * GAP
            while (rem := deadline - time.perf_counter()) > 0:
                await asyncio.sleep(min(rem, 0.001))
            await ws.send(f)
        for _ in range(50):  # wait up to 10 s for stragglers
            if len(got) >= N:
                break
            await asyncio.sleep(0.2)
        rt.cancel()
    missing = sorted(set(range(N)) - got)
    print(f"proto={PROTO} gap={GAP*1000:.0f}ms n={N} payload={KB}KB: "
          f"results={len(got)}/{N} missing={missing}")

asyncio.run(main())
