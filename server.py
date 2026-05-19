#!/usr/bin/env python3
"""
Enigma Cipher Server — websockets 15.x / Python 3.9+
Serves static files (public/) and handles WebSocket rooms on a single port.
"""

import asyncio
import json
import mimetypes
import os
from pathlib import Path

from websockets.asyncio.server import serve, ServerConnection
from websockets.http11 import Request, Response
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed

PUBLIC = Path(__file__).parent / "public"

rooms: dict[str, dict[str, ServerConnection]] = {}
peer_counter = 0


# ── Broadcasting ──────────────────────────────────────────────────────────────

async def broadcast(room_code: str, msg: dict, exclude=None):
    if room_code not in rooms:
        return
    data = json.dumps(msg)
    dead = []
    for pid, ws in list(rooms[room_code].items()):
        if ws is exclude:
            continue
        try:
            await ws.send(data)
        except Exception:
            dead.append(pid)
    for pid in dead:
        rooms[room_code].pop(pid, None)


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def ws_handler(websocket: ServerConnection):
    global peer_counter
    peer_counter += 1
    peer_id = f"P{peer_counter:03d}"
    room_code = None

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")

            if mtype == "join":
                code = "".join(
                    c for c in str(msg.get("room", "")).upper() if c.isalnum()
                )[:8]
                if not code:
                    continue
                room_code = code
                rooms.setdefault(room_code, {})[peer_id] = websocket
                count = len(rooms[room_code])

                await websocket.send(json.dumps({
                    "type": "joined", "room": room_code,
                    "peerId": peer_id, "peers": count,
                }))
                await broadcast(room_code, {
                    "type": "peer_update", "peers": count,
                    "event": "joined", "peerId": peer_id,
                }, exclude=websocket)

            elif mtype in ("char", "delete", "clear", "typing") and room_code:
                await broadcast(room_code, {**msg, "peerId": peer_id},
                                exclude=websocket)

    except ConnectionClosed:
        pass
    finally:
        if room_code and room_code in rooms:
            rooms[room_code].pop(peer_id, None)
            size = len(rooms[room_code])
            if size == 0:
                del rooms[room_code]
            else:
                await broadcast(room_code, {
                    "type": "peer_update", "peers": size,
                    "event": "left", "peerId": peer_id,
                })


# ── Static file serving via process_request hook ──────────────────────────────

def serve_static(connection: ServerConnection, request: Request):
    """Return a Response for static files; return None to let WS handshake proceed."""
    # WebSocket upgrade requests: let websockets handle them
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None

    path = request.path.split("?")[0]
    if path == "/":
        path = "/index.html"

    file_path = PUBLIC / path.lstrip("/")

    # Security: stay inside public/
    try:
        file_path.resolve().relative_to(PUBLIC.resolve())
    except ValueError:
        return Response(403, "Forbidden", Headers(), b"Forbidden")

    if not file_path.exists() or not file_path.is_file():
        return Response(404, "Not Found", Headers(), b"Not Found")

    mime, _ = mimetypes.guess_type(str(file_path))
    mime = mime or "application/octet-stream"
    body = file_path.read_bytes()

    return Response(200, "OK", Headers([
        ("Content-Type", mime),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-cache"),
    ]), body)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    port = int(os.environ.get("PORT", 3001))
    print(f"\n  ENIGMA CIPHER SERVER")
    print(f"  Running at \033[92mhttp://localhost:{port}\033[0m")
    print(f"  Open on multiple devices (or tabs) to begin transmission\n")

    # origins=None allows cross-origin connections (required when frontend
    # is on a different host, e.g. Netlify, and backend is here on Render)
    async with serve(ws_handler, "0.0.0.0", port,
                     process_request=serve_static,
                     origins=None):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
