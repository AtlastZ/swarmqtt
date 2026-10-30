"""swarmqtt web UI: tests a connection with paho-mqtt and runs emqtt-bench worker containers."""
import collections
import json
import math
import os
import re
import ssl
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import paho.mqtt.client as mqtt

BENCH_IMAGE = os.environ.get("BENCH_IMAGE", "emqx/emqtt-bench:latest")
CERTS_VOLUME = os.environ.get("CERTS_VOLUME", "swarmqtt-certs")
CERTS_DIR = "/certs"
LABEL = "swarmqtt"
# Each worker container has its own IP, so it gets its own ~64k source ports to the broker.
PER_WORKER = 25000
MAX_TOTAL = 1_000_000
MAX_PAYLOAD = 256 * 1024
# measured with emqtt-bench + TLS; plain MQTT uses less, so this errs on the safe side
WORKER_MEM = 575 * 2**20
CONN_MEM = 107 * 2**10
MAX_MEM_SHARE = 0.9  # refuse above this share of Docker's RAM: the VM would crash mid-test
METRIC = re.compile(r"(\w+) total=(\d+) rate=([\d.]+)")
HOST_RE = re.compile(r"^[A-Za-z0-9.\-:\[\],]+$")

dk = None  # docker client, set in main()
mem_total = ncpu = 0  # Docker's RAM (bytes) and CPUs, set in main()
lock = threading.Lock()
stats = {}  # worker name -> {metric: {"total": int, "rate": float}}
usage = {}  # worker name -> {"mem": bytes, "cpu": cores}
run = {"id": 0, "timer": None, "ends_at": None}
logs = collections.deque(maxlen=200)


class BadRequest(Exception):
    pass


def _int(cfg, key, lo, hi, default=None):
    try:
        v = int(cfg.get(key, default))
    except (TypeError, ValueError):
        raise BadRequest(f"{key} must be a number")
    if not lo <= v <= hi:
        raise BadRequest(f"{key} must be between {lo} and {hi}")
    return v


def parse_cfg(raw):
    """Validate the form; everything below trusts the result."""
    host = str(raw.get("host", "")).strip()
    if not host or not HOST_RE.match(host):
        raise BadRequest("host is invalid")
    cfg = {
        "host": host,
        "port": _int(raw, "port", 1, 65535),
        "version": str(raw.get("version", "5")),
        "username": str(raw.get("username", "")),
        "password": str(raw.get("password", "")),
        "prefix": re.sub(r"[^A-Za-z0-9_\-]", "", str(raw.get("prefix", "lt"))) or "lt",
        "tls": bool(raw.get("tls")),
        "verify": bool(raw.get("verify")),
        "tls_version": str(raw.get("tls_version", "")),
        "ca": str(raw.get("ca", "")).strip(),
        "cert": str(raw.get("cert", "")).strip(),
        "key": str(raw.get("key", "")).strip(),
        "mode": str(raw.get("mode", "pub")),
        "topic": str(raw.get("topic", "iot/%i/telemetry")).strip(),
        "qos": _int(raw, "qos", 0, 2),
        "total": _int(raw, "total", 1, MAX_TOTAL),
        "rate": _int(raw, "rate", 1, 100_000),
        "keepalive": _int(raw, "keepalive", 5, 3600),
        "interval": _int(raw, "interval", 1, 3_600_000),
        "size": _int(raw, "size", 1, 1_048_576),
        "payload": str(raw.get("payload", "")),
        "duration": _int(raw, "duration", 0, 7 * 86400, 0),  # seconds, 0 = until stopped
    }
    if cfg["version"] not in ("3", "4", "5"):
        raise BadRequest("version must be 3, 4 or 5")
    if cfg["mode"] not in ("pub", "sub", "conn"):
        raise BadRequest("mode must be pub, sub or conn")
    if cfg["tls_version"] not in ("", "tlsv1.2", "tlsv1.3"):
        raise BadRequest("tls_version must be empty, tlsv1.2 or tlsv1.3")
    if len(cfg["payload"].encode()) > MAX_PAYLOAD:
        raise BadRequest(f"payload must be at most {MAX_PAYLOAD} bytes")
    if bool(cfg["cert"]) != bool(cfg["key"]):
        raise BadRequest("client certificate and client key must be given together")
    if cfg["mode"] != "conn" and (not cfg["topic"] or any(c.isspace() for c in cfg["topic"])):
        raise BadRequest("topic is invalid")
    return cfg


def write_certs(cfg, directory):
    paths = {}
    for name, fname in (("ca", "ca.crt"), ("cert", "client.crt"), ("key", "client.key"), ("payload", "payload.txt")):
        path = os.path.join(directory, fname)
        if cfg[name]:
            with open(path, "w") as f:
                f.write(cfg[name] if name == "payload" else cfg[name] + "\n")  # payload is sent byte for byte
            os.chmod(path, 0o644)  # bench runs as a different user in its own container
            paths[name] = path
        elif os.path.exists(path):
            os.remove(path)
    return paths


def tls_context(cfg, files):
    if cfg["verify"]:
        ctx = ssl.create_default_context(cafile=files.get("ca"))  # no CA given: system CAs
    else:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if cfg["tls_version"]:
        v = {"tlsv1.2": ssl.TLSVersion.TLSv1_2, "tlsv1.3": ssl.TLSVersion.TLSv1_3}[cfg["tls_version"]]
        ctx.minimum_version = ctx.maximum_version = v
    if "cert" in files:
        ctx.load_cert_chain(files["cert"], files["key"])
    return ctx


def test_connection(cfg):
    with tempfile.TemporaryDirectory() as d:
        files = write_certs(cfg, d)
        protocol = {"3": mqtt.MQTTv31, "4": mqtt.MQTTv311, "5": mqtt.MQTTv5}[cfg["version"]]
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{cfg['prefix']}_test", protocol=protocol)
        if cfg["username"]:
            c.username_pw_set(cfg["username"], cfg["password"] or None)
        if cfg["tls"]:
            try:
                c.tls_set_context(tls_context(cfg, files))
            except (OSError, ssl.SSLError) as e:  # unreadable PEM
                return {"ok": False, "message": f"Bad certificate/key: {type(e).__name__}: {e}"}
        result, done = {}, threading.Event()

        def on_connect(client, userdata, flags, reason_code, properties):
            result.update(ok=not reason_code.is_failure, rc=str(reason_code))
            done.set()

        c.on_connect = on_connect
        t0 = time.monotonic()
        try:
            c.connect(cfg["host"], cfg["port"], keepalive=10)
        except (OSError, ssl.SSLError, ValueError) as e:
            return {"ok": False, "message": f"{type(e).__name__}: {e}"}
        c.loop_start()
        try:
            if not done.wait(10):
                return {"ok": False, "message": "No CONNACK within 10s: check MQTT vs MQTTS for this port, "
                                                "or the broker may require a client certificate"}
            ms = round((time.monotonic() - t0) * 1000)
            if result["ok"]:
                proto = f"MQTTS, {c.socket().version()}" if cfg["tls"] else "MQTT"
                return {"ok": True, "message": f"Connected in {ms} ms ({proto})"}
            return {"ok": False, "message": f"Broker refused: {result['rc']}"}
        finally:
            c.disconnect()
            c.loop_stop()


def bench_args(cfg, count, start, rate):
    args = [cfg["mode"], "-h", cfg["host"], "-p", str(cfg["port"]), "-V", cfg["version"],
            "-c", str(count), "-R", str(rate), "-k", str(cfg["keepalive"]),
            "--prefix", f"{cfg['prefix']}_", "-n", str(start)]
    if cfg["username"]:
        args += ["-u", cfg["username"], "-P", cfg["password"]]
    if cfg["tls"]:
        args += ["--ssl"]
        if cfg["tls_version"]:
            args += ["--ssl-version", cfg["tls_version"]]
        if cfg["cert"]:
            args += ["--certfile", f"{CERTS_DIR}/client.crt", "--keyfile", f"{CERTS_DIR}/client.key"]
    if cfg["mode"] == "pub":
        args += ["-t", cfg["topic"], "-I", str(cfg["interval"]), "-s", str(cfg["size"]), "-q", str(cfg["qos"])]
        if cfg["payload"]:  # via a file so any content (JSON, quotes, newlines) survives
            args += ["-m", f"template://{CERTS_DIR}/payload.txt"]
    elif cfg["mode"] == "sub":
        args += ["-t", cfg["topic"], "-q", str(cfg["qos"])]
    return args


def plan(total, rate):
    """Split total connections (and total conn rate) across workers: [(count, start, rate), ...]."""
    n = math.ceil(total / PER_WORKER)
    per_rate = max(1, rate // n)
    return [(min(PER_WORKER, total - i * PER_WORKER), i * PER_WORKER, per_rate) for i in range(n)]


def estimate_mem(total):
    return math.ceil(total / PER_WORKER) * WORKER_MEM + total * CONN_MEM


def workers():
    return dk.containers.list(all=True, filters={"label": LABEL})


def follow(container):
    buf = b""
    try:
        for chunk in container.logs(stream=True, follow=True):
            buf += chunk
            *lines, buf = buf.split(b"\n")
            for line in lines:
                text = line.decode(errors="replace").strip()
                m = METRIC.search(text)
                with lock:
                    if m:
                        stats.setdefault(container.name, {})[m[1]] = {"total": int(m[2]), "rate": float(m[3])}
                    elif text:
                        logs.append(f"[{container.name}] {text}")
    except Exception as e:  # container removed mid-stream
        with lock:
            logs.append(f"[{container.name}] log stream ended: {e}")


def watch(container):
    """Track a worker's RAM and CPU from the Docker stats stream (about one sample per second)."""
    try:
        for s in container.stats(stream=True, decode=True):
            m, c, p = s.get("memory_stats", {}), s.get("cpu_stats", {}), s.get("precpu_stats", {})
            cpu_d = c.get("cpu_usage", {}).get("total_usage", 0) - p.get("cpu_usage", {}).get("total_usage", 0)
            sys_d = c.get("system_cpu_usage", 0) - p.get("system_cpu_usage", 0)
            ms = m.get("stats", {})
            mem = m.get("usage", 0) - ms.get("inactive_file", ms.get("cache", 0))  # what `docker stats` shows
            with lock:
                if not m:  # container stopped
                    usage.pop(container.name, None)
                    continue
                usage[container.name] = {"mem": max(mem, 0),
                                         "cpu": cpu_d / sys_d * c.get("online_cpus", 1) if sys_d > 0 else 0.0}
    except Exception:  # container removed
        pass
    with lock:
        usage.pop(container.name, None)


def attach(container):
    for target in (follow, watch):
        threading.Thread(target=target, args=(container,), daemon=True).start()


def stop():
    with lock:
        if run["timer"]:
            run["timer"].cancel()
        run.update(timer=None, ends_at=None)
    for c in workers():
        c.remove(force=True)
    with lock:
        stats.clear()
        usage.clear()


def expire(run_id):
    if run["id"] == run_id:  # a newer test may have replaced this one
        stop()
        with lock:
            logs.append("Duration reached: load test stopped")


def start(cfg):
    import docker
    need = estimate_mem(cfg["total"])
    if mem_total and need > mem_total * MAX_MEM_SHARE:
        raise BadRequest(f"{cfg['total']:,} devices need ~{need / 2**30:.1f} GB RAM but Docker has "
                         f"{mem_total / 2**30:.1f} GB: lower the devices, give Docker more RAM, or split across machines")
    stop()
    with lock:
        logs.clear()
        logs.append(f"Starting {cfg['total']} connections across {len(plan(cfg['total'], cfg['rate']))} worker(s)")
    write_certs(cfg, CERTS_DIR)
    try:
        dk.images.get(BENCH_IMAGE)
    except docker.errors.ImageNotFound:
        dk.images.pull(BENCH_IMAGE)
    for i, (count, first, rate) in enumerate(plan(cfg["total"], cfg["rate"])):
        c = dk.containers.run(
            BENCH_IMAGE, bench_args(cfg, count, first, rate), name=f"swarmqtt-worker-{i}",
            labels={LABEL: "worker"}, detach=True,
            volumes={CERTS_VOLUME: {"bind": CERTS_DIR, "mode": "ro"}},
            ulimits=[docker.types.Ulimit(name="nofile", soft=1048576, hard=1048576)],
            sysctls={"net.ipv4.ip_local_port_range": "1024 65535"},
            extra_hosts={"host.docker.internal": "host-gateway"})
        attach(c)
    with lock:
        run["id"] += 1
        if cfg["duration"]:
            run["timer"] = threading.Timer(cfg["duration"], expire, args=(run["id"],))
            run["timer"].daemon = True
            run["timer"].start()
            run["ends_at"] = time.time() + cfg["duration"]


def status():
    ws = [{"name": c.name, "status": c.status} for c in workers()]
    with lock:
        totals = {}
        for m in stats.values():
            for k, v in m.items():
                t = totals.setdefault(k, {"total": 0, "rate": 0.0})
                t["total"] += v["total"]
                t["rate"] = round(t["rate"] + v["rate"], 2)
        load = {"mem": sum(u["mem"] for u in usage.values()), "mem_total": mem_total,
                "cpu": round(sum(u["cpu"] for u in usage.values()), 2), "ncpu": ncpu}
        return {"running": any(w["status"] == "running" for w in ws), "workers": ws,
                "totals": totals, "load": load,
                # seconds left, not a timestamp: the Docker VM clock can drift from the viewer's
                "ends_in": run["ends_at"] and max(0, round(run["ends_at"] - time.time())), "logs": list(logs)[-50:]}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/":
            with open(os.path.join(os.path.dirname(__file__), "index.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        if self.path == "/api/status":
            return self._send(200, status())
        self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 1_000_000:
                raise BadRequest("request too large")
            raw = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/api/test":
                return self._send(200, test_connection(parse_cfg(raw)))
            if self.path == "/api/start":
                start(parse_cfg(raw))
                return self._send(200, {"ok": True})
            if self.path == "/api/stop":
                stop()
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not found"})
        except (BadRequest, json.JSONDecodeError) as e:
            self._send(400, {"error": str(e)})
        except Exception as e:
            self._send(500, {"error": f"{type(e).__name__}: {e}"})

    def log_message(self, *args):
        pass


def main():
    global dk, mem_total, ncpu
    import docker
    dk = docker.from_env()
    info = dk.info()
    mem_total, ncpu = info["MemTotal"], info["NCPU"]
    for c in workers():  # reattach after a web restart (the duration timer is not restored)
        attach(c)
    import mcp_server
    threading.Thread(target=mcp_server.serve, daemon=True).start()
    print("Listening on :8080 (UI) and :8081/mcp (MCP)")
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
