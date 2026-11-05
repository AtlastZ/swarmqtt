---
name: swarmqtt-load-test
description: Load test, stress test, or benchmark an MQTT or MQTTS broker (EMQX, Mosquitto, HiveMQ, VerneMQ, AWS IoT, etc.) with swarmqtt, which simulates thousands to hundreds of thousands of concurrent IoT devices. Use this whenever the user wants to know how many devices/connections/messages a broker can handle, find a broker's breaking point, check capacity before a launch, reproduce connection failures under load, or test TLS / mutual-TLS connection storms, even if they don't say "swarmqtt" or "load test" (e.g. "can our broker take 100k sensors?", "ยิง mqtt 1 แสน connection").
---

# Load testing an MQTT broker with swarmqtt

swarmqtt runs emqtt-bench worker containers (max 25,000 devices each, to spread CPU and RAM; workers add no source ports, since every connection leaves through the Docker host's address) behind a small control server. You drive it through its MCP tools or its HTTP API, ramp the load in steps, watch the numbers, and stop when the broker (or the load machine) shows its limit.

## 1. Make sure swarmqtt is running

- **MCP tools available** (`test_connection`, `start_load_test`, `get_status`, `stop_load_test`): use them. Done.
- **Not available**: start it with `docker compose pull && docker compose up -d` in the swarmqtt repo, or without the repo with the `docker run ... ghcr.io/kit09k/swarmqtt:latest` command in the README. The UI is `http://localhost:8080`, MCP is `http://localhost:8081/mcp` (`claude mcp add --transport http swarmqtt http://localhost:8081/mcp`). If adding MCP isn't possible this session, use the HTTP API in the appendix; it has the same actions.

The control server mounts the Docker socket and is published on 127.0.0.1 only. Keep it that way; anyone who can reach it can run containers.

## 2. Agree on the target and the plan before sending load

A load test against the wrong host is an attack, and against a shared production broker it is an outage. So before any `start_load_test`, confirm with the user:

- the broker host/port and that they own it or are allowed to test it (if it looks like production, say so and ask)
- the goal: a fixed target ("hold 100k devices") or "find the limit"
- the device profile: mode (`pub` telemetry, `sub`, or `conn` only), publish interval, payload size or a real sample message, QoS, TLS and client certs, `connect_retries` (retries before connect failure, 0-10, default 0), and `wait_before_publishing` (pub mode: each worker holds back publishing until all its connections have started; not synchronised across workers or machines)

If the broker or its consumers parse messages (rules engine, bridge to Kafka/DB, schema checks), random bytes won't exercise that path: pass a realistic sample as `payload`, e.g. `{"temp":%WALK:20.0:30.0:0.2%,"hum":%RANGE:40:60%,"ts":%TIMESTAMPMS%}`. Variables: `%TIMESTAMP%`, `%TIMESTAMPMS%`, `%UNIQUE%` (unique per message, not sequential), `%RANDOM%` (`payload_bytes` random bytes), `%RANGE:<min>:<max>%` (new random value in range on every message), and `%WALK:<min>:<max>:<step>%` (random walk, own value per device, moves a random amount of at most step per message). Decimals follow the most precise number written (`40:60` integers, `20.0:30.0` one decimal); typos are refused. WALK keeps per-device state, but there is still no device number/client ID variable in the payload; the device number goes in the topic (`%i`). The swarmqtt image contains the patched emqtt-bench and is also used for workers, so the published image `ghcr.io/kit09k/swarmqtt:latest` is all that is needed on every machine (controller and peers); workers use the image the control server runs from. `BENCH_IMAGE` in `.env` is optional and only for overriding the worker image (the stock `emqx/emqtt-bench:latest` refuses RANGE/WALK).

Broker on the same machine: use `host.docker.internal`, not `localhost`, because the workers run in containers.

Certificates go in as PEM **contents** (read the file and pass the text), not paths, since the server runs in a container and can't see the user's files.

## 3. Check the load machine can actually generate the load

The load generator runs out of memory long before a decent broker does, and a crashed Docker VM looks like a broker failure. Measured on emqtt-bench with TLS:

- ~575 MB per worker (fixed) + ~107 KB per TLS connection
- workers = ceil(devices / 25,000)
- 100k TLS devices ≈ 4 × 575 MB + 100k × 107 KB ≈ **13 GB**

Check what Docker has with `docker info --format '{{.MemTotal}} {{.NCPU}}'` (on Docker Desktop / colima this is the VM size, not the host's, and other containers already use part of it). Keep the estimate under ~70% of it. `start_load_test` refuses anything above 90% by itself, so treat that error as "this machine is too small", not a broker problem. During the run, `get_status` → `load` shows the workers' actual RAM (vs `mem_total`) and CPU in cores (vs `ncpu`): if either is near the limit, the load machine is the bottleneck and any failures at that step don't count against the broker. Plain MQTT is lighter (~40 KB per connection). If it doesn't fit, tell the user the maximum this machine can do and offer options: a bigger VM, a Linux box closer to the broker, or peers: other machines running swarmqtt that this one drives (`PEERS` + `PEER_TOKEN` in the repo README). With peers, the controller splits devices and Conn/sec by each machine's capacity (RAM, and the port cap on a Mac); the Machines list in the UI shows each share and lets you set weights by hand (or `machine_weights` in MCP). The connect rate is split with the same weights (at least 1 for every machine that gets devices) and `get_status` sums them; only this machine's RAM is checked up front, each peer refuses its own share if it won't fit.

Source ports are the other limit. Docker on macOS (OrbStack, Docker Desktop) reopens every container connection from the Mac itself, so all workers share the Mac's ephemeral ports (stock range 49152–65535, 16,384 ports). The default `load.max_devices` of 15,000 fits this stock range, and `start_load_test` refuses more per machine. To raise it, widen the Mac's port range with `sudo sysctl -w net.inet.ip.portrange.first=10000` (55,536 ports, reset at reboot), then set `MAC_MAX_DEVICES=50000` in `.env` and `docker compose up -d`; RAM then becomes the limit. With peers the cap is per machine because the controller splits devices by capacity. Above that or without tuning, use a Linux machine in the broker's network. On Linux the ceiling is about 64k connections per broker host:port.

## 4. Run it

1. **`test_connection`** with the real settings and `verify_server_cert=true` if they have the CA. Fix any failure here; it is much cheaper to debug one connection than 100k. Note the connect latency as a baseline.
2. **Smoke run**: ~100–1,000 devices. Wait until `connect_succ` reaches the target, then read `get_status` twice ~10 s apart. Any `connect_fail` or worker not `running` at this size is a config problem, not a capacity limit: stop and fix it.
3. **Ramp**: let swarmqtt do it: `start_load_test` with `ramp_steps` (e.g. `[1000, 10000, 25000, 50000, 100000]`, or repeats like `[100000, 100000]`; steps run in the order written and repeats are allowed), `hold_sec` of at least 60–120 (a few keepalive periods) and `fail_pct` (default 1). It runs each step, waits for connections to settle, holds, and stops at the first step that fails, recording why. Each step reconnects everything from zero, which also tests reconnect storms. Poll `get_status` every ~30 s meanwhile (`run.steps` fills in as steps finish) rather than tight-looping. For a fixed target instead of a limit, use `devices` + `duration_sec` (default 600, so a forgotten session can't leave load running; `ends_in` shows the seconds left).
   Add `subscribers` (1–2 is enough) whenever message throughput or latency matters: you get delivered msg/s (the real rate) and publish-to-receive latency per step. It prepends an 8-byte binary timestamp to every payload, so warn the user if real consumers parse those topics.
4. Pick a sensible `connections_per_sec`: Conn/sec is the total for all machines (the controller splits it across machines with the same weights, at least 1 each for machines getting devices). The TLS handshake is the expensive part for the broker. Start around 500/s total (the default) and raise it only if the goal is to test connection storms. From Docker on macOS keep it at 500/s or lower: faster storms stall OrbStack's proxy and show up as `pingresp_timeout` / `keepalive_timeout` that are not the broker's fault. Worker logs are saved to `logs/` on Stop, so failures can be counted afterwards.

**Stop the ramp and record the last good step when any of these happens:**

- `connect_fail` above ~1% of the target, or still climbing
- `connect_succ` stops growing well short of the target for more than a minute
- `load.mem` near `load.mem_total` or `load.cpu` near `ncpu` (the load machine's limit, not the broker's: report it as such)
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

Call `stop_load_test` when finished, when aborting, and before handing back to the user if anything went wrong. Leftover workers keep hammering the broker and holding gigabytes of RAM. Confirm with `get_status` that `workers` is empty. (A ramp cleans up by itself when it ends; stopping is still harmless.)

## 7. Report

Every run is recorded. Call `get_run(run_id)`: it gives a one-line `headline`, per-step results, and links to a **PDF report** (setup, steps table, charts, caveats), CSV and JSON on the web UI. Always give the user the PDF link. Then summarise in this structure, using the step results rather than your own sampling:

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
| GET | `/api/status` | `{running, workers[], totals{metric:{total,rate}}, load, latency_ms, run{id,status,steps[],samples[]}, ends_in, logs[]}` |
| GET | `/api/runs` | recorded runs, newest first |
| GET | `/api/runs/<id>` | one run (`.json` / `.csv` to download, `/report.pdf` for the PDF) |
| POST | `/api/stop` | stop and remove workers |

Fields for `/api/test` and `/api/start`: `host`, `port`, `version` ("3"/"4"/"5"), `username`, `password`, `prefix`, `tls` (bool), `verify` (bool, test only), `tls_version` ("", "tlsv1.2", "tlsv1.3"), `ca`, `cert`, `key` (PEM text), `mode` ("pub"/"sub"/"conn"), `wait_publish` (bool, pub mode), `topic` (`%i` device number, `%c` client ID), `qos`, `total` (devices), `rate` (total conn/s across machines), `keepalive`, `retries` (0-10 connect retries), `interval` (ms), `size` (bytes), `payload` (custom message text, optional), `duration` (seconds, 0 = until stopped; `/api/start` defaults to 0, so set it), `steps` ("1000,10000" or a list: ramp in written order, repeats allowed), `hold` (s per step), `fail_pct`, `subscribers`, `sub_topic`. `/api/start` returns `run_id`. `/api/test` validates the whole form too, so send the same full body as `/api/start`. Errors come back as HTTP 400 with `{"error": "..."}`.
