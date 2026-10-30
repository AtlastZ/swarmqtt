---
name: swarmqtt-load-test
description: Load test, stress test, or benchmark an MQTT or MQTTS broker (EMQX, Mosquitto, HiveMQ, VerneMQ, AWS IoT, etc.) with swarmqtt, which simulates thousands to hundreds of thousands of concurrent IoT devices. Use this whenever the user wants to know how many devices/connections/messages a broker can handle, find a broker's breaking point, check capacity before a launch, reproduce connection failures under load, or test TLS / mutual-TLS connection storms, even if they don't say "swarmqtt" or "load test" (e.g. "can our broker take 100k sensors?", "ยิง mqtt 1 แสน connection").
---

# Load testing an MQTT broker with swarmqtt

swarmqtt runs emqtt-bench worker containers (max 25,000 devices each, so each worker gets its own IP and ~64k source ports) behind a small control server. You drive it through its MCP tools or its HTTP API, ramp the load in steps, watch the numbers, and stop when the broker (or the load machine) shows its limit.

## 1. Make sure swarmqtt is running

- **MCP tools available** (`test_connection`, `start_load_test`, `get_status`, `stop_load_test`): use them. Done.
- **Not available**: start it from the swarmqtt repo with `docker compose up -d --build`. The UI is `http://localhost:8080`, MCP is `http://localhost:8081/mcp` (`claude mcp add --transport http swarmqtt http://localhost:8081/mcp`). If adding MCP isn't possible this session, use the HTTP API in the appendix; it has the same actions.

The control server mounts the Docker socket and is published on 127.0.0.1 only. Keep it that way; anyone who can reach it can run containers.

## 2. Agree on the target and the plan before sending load

A load test against the wrong host is an attack, and against a shared production broker it is an outage. So before any `start_load_test`, confirm with the user:

- the broker host/port and that they own it or are allowed to test it (if it looks like production, say so and ask)
- the goal: a fixed target ("hold 100k devices") or "find the limit"
- the device profile: mode (`pub` telemetry, `sub`, or `conn` only), publish interval, payload size or a real sample message, QoS, TLS and client certs

If the broker or its consumers parse messages (rules engine, bridge to Kafka/DB, schema checks), random bytes won't exercise that path: pass a realistic sample as `payload`, e.g. `{"temp":24.5,"ts":%TIMESTAMPMS%,"seq":%UNIQUE%}`. Variables: `%TIMESTAMP%`, `%TIMESTAMPMS%`, `%UNIQUE%` (unique per message, not sequential), `%RANDOM%` (`payload_bytes` random bytes). There is no per-device variable in the payload; the device number goes in the topic (`%i`).

Broker on the same machine: use `host.docker.internal`, not `localhost`, because the workers run in containers.

Certificates go in as PEM **contents** (read the file and pass the text), not paths, since the server runs in a container and can't see the user's files.

## 3. Check the load machine can actually generate the load

The load generator runs out of memory long before a decent broker does, and a crashed Docker VM looks like a broker failure. Measured on emqtt-bench with TLS:

- ~575 MB per worker (fixed) + ~107 KB per TLS connection
- workers = ceil(devices / 25,000)
- 100k TLS devices ≈ 4 × 575 MB + 100k × 107 KB ≈ **13 GB**

Check what Docker has with `docker info --format '{{.MemTotal}} {{.NCPU}}'` (on Docker Desktop / colima this is the VM size, not the host's, and other containers already use part of it). Keep the estimate under ~70% of it. If it doesn't fit, tell the user the maximum this machine can do and offer options: a bigger VM, a Linux box closer to the broker, or several machines each running swarmqtt with a different `client_id_prefix` so client IDs don't collide.

## 4. Run it

1. **`test_connection`** with the real settings and `verify_server_cert=true` if they have the CA. Fix any failure here; it is much cheaper to debug one connection than 100k. Note the connect latency as a baseline.
2. **Smoke run**: ~100–1,000 devices. Wait until `connect_succ` reaches the target, then read `get_status` twice ~10 s apart. Any `connect_fail` or worker not `running` at this size is a config problem, not a capacity limit: stop and fix it.
3. **Ramp**: increase in steps (e.g. 1k → 10k → 25k → 50k → 100k, or 2x per step when looking for the limit). `start_load_test` replaces the running test, so each step reconnects everything from zero; that is fine and also tests reconnect storms. At each step, wait for connections to settle, then hold for at least 1–2 minutes (a few keepalive periods) and sample `get_status` every ~15–30 s.
4. Pick a sensible `connections_per_sec`: the TLS handshake is the expensive part for the broker. Start around 500–2,000/s total and raise it only if the goal is to test connection storms.

**Stop the ramp and record the last good step when any of these happens:**

- `connect_fail` above ~1% of the target, or still climbing
- `connect_succ` stops growing well short of the target for more than a minute
- workers show `exited` (read the log lines in the status: often out of memory or file descriptors on the load side)
- the broker's own metrics show CPU pinned, memory near the limit, or dropped connections (ask the user for them if you can't see them)
- the Docker daemon stops responding (load machine overloaded, not the broker)

## 5. Read the numbers correctly

- `connect_succ` / `connect_fail`: cumulative totals summed over all workers, with a per-second rate.
- `pub`: emqtt-bench reports roughly **2x** the messages the broker actually receives. Don't quote it as throughput; use broker-side metrics, or say the number is the client-side count.
- Expected publish rate = devices × 1000 / `publish_interval_ms` msg/s. Compare that with what the broker reports.
- Errors in the worker log, roughly: refused/timeout points at network, firewall, or listener limits; TLS alerts at the cert, SNI, or TLS version; not-authorized at credentials/ACLs; connections that open and then drop at broker connection limits or a load balancer idle timeout.
- The broker certificate is **not** verified during the load test (an emqtt-bench limitation); only `test_connection` verifies it. Mention this if the user cares about TLS correctness.

## 6. Always clean up

Call `stop_load_test` when finished, when aborting, and before handing back to the user if anything went wrong. Leftover workers keep hammering the broker and holding gigabytes of RAM. Confirm with `get_status` that `workers` is empty.

## 7. Report

Use this structure:

```
# MQTT load test: <broker host:port>
## Result
<one or two sentences: max stable devices / where it broke and the first symptom>
## Setup
broker, protocol (MQTT v5 / MQTTS + mTLS), device profile (mode, interval, payload, QoS), conn rate, load machine resources
## Steps
| Devices | Connected | Failed | Conn rate/s | Held for | Notes |
## Observations
what failed first and the evidence (log lines, broker metrics)
## Caveats
pub count ~2x, server cert not verified under load, load-machine limits, anything not measured
## Next steps
```

Be explicit about what was measured versus inferred: "the broker refused connections at 60k" and "the load machine ran out of memory at 60k" lead to very different conclusions.

## Appendix: HTTP API (when MCP isn't connected)

Base URL `http://localhost:8080`. All bodies are JSON.

| Method | Path | Notes |
|---|---|---|
| POST | `/api/test` | test one connection |
| POST | `/api/start` | start/replace the load test |
| GET | `/api/status` | `{running, workers[], totals{metric:{total,rate}}, logs[]}` |
| POST | `/api/stop` | stop and remove workers |

Fields for `/api/test` and `/api/start`: `host`, `port`, `version` ("3"/"4"/"5"), `username`, `password`, `prefix`, `tls` (bool), `verify` (bool, test only), `tls_version` ("", "tlsv1.2", "tlsv1.3"), `ca`, `cert`, `key` (PEM text), `mode` ("pub"/"sub"/"conn"), `topic` (`%i` device number, `%c` client ID), `qos`, `total` (devices), `rate` (total conn/s), `keepalive`, `interval` (ms), `size` (bytes), `payload` (custom message text, optional). `/api/test` validates the whole form too, so send the same full body as `/api/start`. Errors come back as HTTP 400 with `{"error": "..."}`.
