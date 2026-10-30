"""swarmqtt MCP server: the same actions as the web UI, for AI agents (streamable HTTP on :8081/mcp)."""
from typing import Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

import app

mcp = MCPServer(
    "swarmqtt",
    instructions=(
        "Load test MQTT/MQTTS brokers with simulated IoT devices. "
        "Always call test_connection first, start small (e.g. 1000 devices) and check get_status "
        "before scaling up. To find a broker's limit, pass ramp_steps and let swarmqtt step up and judge each "
        "step. Every run is recorded: list_runs / get_run give the results and a PDF report link. "
        "Only target brokers the user owns or is allowed to test."
    ),
)


def _cfg(**kw):
    try:
        return app.parse_cfg(kw)
    except app.BadRequest as e:
        raise ToolError(str(e)) from None  # ToolError text reaches the agent; other errors are masked


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=True))
def test_connection(
    host: str,
    port: int = 8883,
    tls: bool = True,
    verify_server_cert: bool = True,
    mqtt_version: Literal["3", "4", "5"] = "5",
    username: str = "",
    password: str = "",
    ca_pem: str = "",
    client_cert_pem: str = "",
    client_key_pem: str = "",
) -> dict:
    """Open one MQTT(S) connection to the broker and report success, latency, or the error.

    PEM arguments are the file contents, not paths. Use host.docker.internal for a broker on the host machine.
    """
    return app.test_connection(_cfg(
        host=host, port=port, tls=tls, verify=verify_server_cert, version=mqtt_version,
        username=username, password=password, ca=ca_pem, cert=client_cert_pem, key=client_key_pem,
        total=1, rate=1, keepalive=10, interval=1000, size=1, qos=0, mode="conn"))


@mcp.tool(annotations=ToolAnnotations(destructiveHint=True, openWorldHint=True))
def start_load_test(
    host: str,
    devices: int,
    port: int = 8883,
    connections_per_sec: int = 500,
    mode: Literal["pub", "sub", "conn"] = "pub",
    topic: str = "iot/%i/telemetry",
    publish_interval_ms: int = 10000,
    payload_bytes: int = 256,
    payload: str = "",
    duration_sec: int = 600,
    ramp_steps: list[int] | None = None,
    hold_sec: int = 60,
    fail_pct: float = 1.0,
    subscribers: int = 0,
    sub_topic: str = "",
    qos: int = 0,
    keepalive: int = 60,
    tls: bool = True,
    tls_version: Literal["", "tlsv1.2", "tlsv1.3"] = "",
    mqtt_version: Literal["3", "4", "5"] = "5",
    client_id_prefix: str = "lt",
    username: str = "",
    password: str = "",
    client_cert_pem: str = "",
    client_key_pem: str = "",
) -> dict:
    """Start a load test with `devices` concurrent simulated IoT devices (replaces any running test).

    Spawns one emqtt-bench worker container per 25,000 devices. Each worker needs ~575 MB plus
    ~107 KB per TLS connection. Workers add no source ports: with Docker on macOS (OrbStack,
    Docker Desktop) all of them share the Mac's 16,384, so the test is refused above 15,000 devices
    there (get_status `load.max_devices`). Topic placeholders: %i = device number, %c = client ID.
    `payload` sets custom message content (e.g. JSON) instead of `payload_bytes` random bytes; it may use
    %TIMESTAMP%, %TIMESTAMPMS%, %UNIQUE% (unique number per message, not sequential) and %RANDOM% (payload_bytes random bytes).
    The broker certificate is NOT verified during the load test.
    The test stops by itself after `duration_sec` (0 = run until stop_load_test). It is refused if the
    estimated RAM does not fit the Docker host.

    Ramp: `ramp_steps` (e.g. [1000, 10000, 50000]) runs each device count in turn (replacing `devices` and
    `duration_sec`), holds it `hold_sec` once connected, and stops at the first step where connect failures
    exceed `fail_pct`% or under 99% connect. Each step reconnects all devices from zero.

    `subscribers` > 0 adds that many subscriber clients on `sub_topic` (default: the topic with %i/%c
    replaced by +) to measure received msg/s and publish-to-receive latency. This prepends an 8-byte binary
    timestamp to every payload.
    """
    cfg = _cfg(
        host=host, port=port, total=devices, rate=connections_per_sec, mode=mode, topic=topic,
        interval=publish_interval_ms, size=payload_bytes, payload=payload, duration=duration_sec,
        steps=ramp_steps or [], hold=hold_sec, fail_pct=fail_pct, subscribers=subscribers, sub_topic=sub_topic, qos=qos, keepalive=keepalive, tls=tls,
        tls_version=tls_version, version=mqtt_version, prefix=client_id_prefix, username=username,
        password=password, cert=client_cert_pem, key=client_key_pem)
    try:
        app.start(cfg)
    except app.BadRequest as e:
        raise ToolError(str(e)) from None
    return {"started": True, "run_id": app.current.record["id"], "devices": cfg["total"],
            "ramp_steps": cfg["steps"], "workers": len(app.plan(cfg["total"], connections_per_sec))}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_status() -> dict:
    """Live totals (connect_succ, connect_fail, pub, ...), worker containers and recent worker log lines.

    `run` has the current run's id, status, finished `steps` (with ok/reason) and recent samples.
    `latency_ms` is the subscribers' average publish-to-receive latency (needs subscribers > 0).
    `load` is the load generator itself: worker RAM (bytes) vs Docker's mem_total, and CPU in cores vs ncpu.
    If it is near the limit, failures come from the load machine, not the broker. `ends_in` is the
    seconds until the test stops by itself (null = no limit).

    The `pub` count from emqtt-bench is about 2x what the broker actually receives.
    """
    return app.status()


@mcp.tool(annotations=ToolAnnotations(destructiveHint=True, idempotentHint=True))
def stop_load_test() -> dict:
    """Stop the load test and remove all worker containers."""
    app.stop()
    return {"stopped": True}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_runs() -> list:
    """Past and current runs, newest first: id, broker, status, largest step and largest step that passed."""
    return app.list_runs()


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_run(run_id: str) -> dict:
    """One run's result: headline, settings (no secrets), per-step results, and links to the full data.

    The PDF report, CSV and JSON links are on the swarmqtt web UI (http://localhost:8080 unless UI_PORT is
    changed); give them to the user.
    """
    import report
    try:
        r = app.load_run(run_id)
    except app.BadRequest as e:
        raise ToolError(str(e)) from None
    if r is None:
        raise ToolError(f"run {run_id} not found")
    base = f"http://localhost:8080/api/runs/{run_id}"
    return {"id": r["id"], "status": r["status"], "headline": report.headline(r), "settings": r["cfg"],
            "steps": r["steps"], "samples": len(r["samples"]),
            "report_pdf": base + "/report.pdf", "csv": base + ".csv", "json": base + ".json"}


def serve():
    # localhost only: the port is published on 127.0.0.1, and Host/Origin checks block DNS rebinding
    mcp.run(
        "streamable-http", host="0.0.0.0", port=8081, stateless_http=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=["localhost:*", "127.0.0.1:*"],
            allowed_origins=["http://localhost:*", "http://127.0.0.1:*"]))
