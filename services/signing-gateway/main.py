"""
services/signing-gateway/main.py

Signing Gateway Service

Receives signing requests and fans them out to MPC nodes.
Collects partial signatures and combines them once the
threshold ((N // 2) + 1) is met.
"""

import hashlib
import json
import logging
import os

from aiohttp import ClientSession, ClientTimeout, web

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

SERVICE = os.environ.get("SERVICE_NAME", "signing-gateway")
MPC_NODES_RAW = os.environ.get("MPC_NODES", "")


def _parse_mpc_nodes() -> list[str]:
    nodes = [n.strip() for n in MPC_NODES_RAW.split(",") if n.strip()]
    if not nodes:
        raise RuntimeError(
            "MPC_NODES env var is empty or unset. "
            "Expected comma-separated URLs, e.g. "
            "'http://mpc-node-1:8001,http://mpc-node-2:8001'"
        )
    return nodes


def _compute_threshold(total_nodes: int) -> int:
    return (total_nodes // 2) + 1


def _combine_signatures(partials: list[str]) -> str:
    """Combine partial signatures by hashing their sorted concatenation."""
    joined = "".join(sorted(partials))
    return hashlib.sha256(joined.encode()).hexdigest()


async def _collect_partial(
    session: ClientSession,
    node_url: str,
    payload: dict,
) -> str | None:
    """Request a partial signature from a single MPC node."""
    url = f"{node_url.rstrip('/')}/sign"
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                log.warning(
                    "Node %s returned status %d: %s",
                    node_url,
                    resp.status,
                    body,
                )
                return None
            data = await resp.json()
            return data.get("partial_signature")
    except Exception as exc:
        log.warning("Node %s unreachable: %s", node_url, exc)
        return None


async def handle_sign(request: web.Request) -> web.Response:
    """Fan out signing request to MPC nodes and collect partials."""
    try:
        body = await request.json()
    except (json.JSONDecodeError, Exception):
        return web.json_response(
            {"error": "Invalid JSON body"}, status=400,
        )

    transaction_id = body.get("transaction_id")
    payload = body.get("payload")
    if not transaction_id or payload is None:
        return web.json_response(
            {"error": "Missing 'transaction_id' or 'payload'"},
            status=400,
        )

    nodes = _parse_mpc_nodes()
    threshold = _compute_threshold(len(nodes))

    return await _fan_out_and_combine(
        nodes, threshold, transaction_id, payload,
    )


async def _fan_out_and_combine(
    nodes: list[str],
    threshold: int,
    transaction_id: str,
    payload: dict,
) -> web.Response:
    """Send payload to all nodes, gather partials, combine if met."""
    timeout = ClientTimeout(total=10)
    async with ClientSession(timeout=timeout) as session:
        partials: list[str] = []
        for node_url in nodes:
            result = await _collect_partial(session, node_url, payload)
            if result:
                partials.append(result)

    if len(partials) < threshold:
        log.error(
            "Threshold not met for txn %s: got %d/%d (need %d)",
            transaction_id,
            len(partials),
            len(nodes),
            threshold,
        )
        return web.json_response(
            {
                "error": "Signing threshold not met",
                "received": len(partials),
                "threshold": threshold,
                "total_nodes": len(nodes),
            },
            status=503,
        )

    combined = _combine_signatures(partials)
    log.info(
        "Signed txn %s: %d/%d partials collected",
        transaction_id,
        len(partials),
        len(nodes),
    )
    return web.json_response({
        "transaction_id": transaction_id,
        "signature": combined,
        "partials_collected": len(partials),
        "threshold": threshold,
    })


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": SERVICE})


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/sign", handle_sign)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8007))
    log.info("Starting %s on port %d", SERVICE, port)
    web.run_app(create_app(), host="0.0.0.0", port=port)
