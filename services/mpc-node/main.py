"""
services/mpc-node/main.py

MPC Node Service

Produces a deterministic partial signature for a given payload.
The partial is SHA256(NODE_ID + sorted payload JSON bytes).
"""

import hashlib
import json
import logging
import os

from aiohttp import web

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

NODE_ID = os.environ.get("NODE_ID", "")
SERVICE = os.environ.get("SERVICE_NAME", "mpc-node")


def _compute_partial_signature(node_id: str, payload: dict) -> str:
    """Deterministic partial: SHA256(node_id + sorted JSON bytes)."""
    canonical = json.dumps(payload, sort_keys=True).encode()
    data = node_id.encode() + canonical
    return hashlib.sha256(data).hexdigest()


async def handle_sign(request: web.Request) -> web.Response:
    """Produce a deterministic partial signature for the payload."""
    if not NODE_ID:
        return web.json_response(
            {"error": "NODE_ID env var is not set"}, status=500,
        )

    try:
        payload = await request.json()
    except (json.JSONDecodeError, Exception):
        return web.json_response(
            {"error": "Invalid JSON body"}, status=400,
        )

    partial = _compute_partial_signature(NODE_ID, payload)
    log.info("Produced partial signature for node %s", NODE_ID)
    return web.json_response({
        "node_id": NODE_ID,
        "partial_signature": partial,
    })


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "service": SERVICE,
        "node_id": NODE_ID,
    })


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/sign", handle_sign)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    log.info("Starting %s (node=%s) on port %d", SERVICE, NODE_ID, port)
    web.run_app(create_app(), host="0.0.0.0", port=port)
