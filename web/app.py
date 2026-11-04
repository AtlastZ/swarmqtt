"""swarmqtt control server: web UI + API, runs emqtt-bench worker containers and records every run."""
import collections
import csv
import glob
import hmac
import io
import json
import math
import os
import re
import ssl
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import paho.mqtt.client as mqtt

BENCH_IMAGE = os.environ.get("BENCH_IMAGE", "swarmqtt:latest")
BENCH_ENTRYPOINT = ["/emqtt_bench/bin/emqtt_bench"]
CERTS_VOLUME = os.environ.get("CERTS_VOLUME", "swarmqtt-certs")
CERTS_DIR = "/certs"
LOGS_DIR = os.environ.get("LOGS_DIR", "/logs")
RUNS_DIR = os.path.join(os.environ.get("DATA_DIR", "/data"), "runs")
# other swarmqtt machines to spread the load over, e.g. "http://10.0.0.2:8082,http://10.0.0.3:8082"
PEERS = [p.strip().rstrip("/") for p in os.environ.get("PEERS", "").split(",") if p.strip()]
TOKEN = os.environ.get("PEER_TOKEN", "")
LABEL = "swarmqtt"
# Splitting across workers spreads CPU and RAM. It adds no source ports: connections leave through
# the Docker host's address, about 64k ports per broker host:port on Linux.
PER_WORKER = 25000
# Docker on macOS (OrbStack, Docker Desktop) reopens every container connection from the Mac itself,
# whose ephemeral range 49152-65535 holds only 16,384 ports for all workers together.
MAC_DOCKER = ("OrbStack", "Docker Desktop")
MAC_MAX_DEVICES = 15000
MAX_TOTAL = 1_000_000
MAX_PAYLOAD = 256 * 1024
# measured with emqtt-bench: per TLS connection ~107 KB; plain MQTT ~40 KB (600 conns), rounded up
WORKER_MEM = 575 * 2**20
CONN_MEM = 107 * 2**10
CONN_MEM_PLAIN = 50 * 2**10
MAX_MEM_SHARE = 0.9  # refuse above this share of Docker's RAM: the VM would crash mid-test
METRIC = re.compile(r"(\w+) total=(\d+) rate=([\d.]+)")
LATENCY = re.compile(r"publish_latency avg=(\d+)ms")  # printed by subscribers when payloads carry a timestamp
RUN_ID = re.compile(r"^\d{8}-\d{6}$")
SAMPLE_EVERY = 2  # seconds
SAMPLE_FIELDS = ["t", "devices", "connected", "failed", "conn_rate", "pub_rate", "recv_rate", "latency_ms",
                 "mem", "mem_total", "cpu", "ncpu"]
SECRET_FIELDS = ("password", "key", "cert", "ca")  # never written to run records
HOST_RE = re.compile(r"^[A-Za-z0-9.\-:\[\],]+$")
NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
STEP_RE = re.compile(r"^\d+(?:\.\d+)?$")
PLACEHOLDER_SPECS = {
    "%RANGE:": (NUM_RE, NUM_RE),
    "%WALK:": (NUM_RE, NUM_RE, STEP_RE),
}

dk = None  # docker client, set in main()
mem_total = ncpu = 0  # Docker's RAM (bytes) and CPUs, set in main()
docker_os = ""  # Docker's OperatingSystem, set in main()
lock = threading.Lock()
stats = {}  # worker name -> {metric: {"total": int, "rate": float}}
usage = {}  # worker name -> {"mem": bytes, "cpu": cores}
latency = {}  # subscriber worker name -> average publish-to-receive ms
current = None  # the latest Run
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


def _steps(v):
    if isinstance(v, str):
        v = [x for x in re.split(r"[,\s]+", v) if x]
    try:
        steps = sorted({int(x) for x in v or []})
    except (TypeError, ValueError):
        raise BadRequest("ramp steps must be device counts, e.g. 1000,10000,50000")
    if len(steps) > 20 or any(not 1 <= n <= MAX_TOTAL for n in steps):
        raise BadRequest(f"ramp steps: at most 20, each between 1 and {MAX_TOTAL:,}")
    return steps


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
        "steps": _steps(raw.get("steps")),  # ramp: device counts run one after another
        "hold": _int(raw, "hold", 10, 3600, 60),  # seconds each ramp step is held once connected
        "subscribers": _int(raw, "subscribers", 0, MAX_TOTAL, 0),  # extra clients that measure latency
        "sub_topic": str(raw.get("sub_topic", "")).strip(),
        "offset": _int(raw, "offset", 0, 10**9, 0),  # first client number (set by a controller for its peers)
    }
    try:
        cfg["fail_pct"] = float(raw.get("fail_pct", 1))
    except (TypeError, ValueError):
        raise BadRequest("fail_pct must be a number")
    if not 0 <= cfg["fail_pct"] <= 100:
        raise BadRequest("fail_pct must be between 0 and 100")
    if cfg["steps"]:
        cfg["total"] = cfg["steps"][-1]
    # subscribe to every device's topic: segments with %i / %c become +
    cfg["sub_topic"] = cfg["sub_topic"] or "/".join("+" if "%" in seg else seg for seg in cfg["topic"].split("/"))
    if cfg["version"] not in ("3", "4", "5"):
        raise BadRequest("version must be 3, 4 or 5")
    if cfg["mode"] not in ("pub", "sub", "conn"):
        raise BadRequest("mode must be pub, sub or conn")
    if cfg["tls_version"] not in ("", "tlsv1.2", "tlsv1.3"):
        raise BadRequest("tls_version must be empty, tlsv1.2 or tlsv1.3")
    if len(cfg["payload"].encode()) > MAX_PAYLOAD:
        raise BadRequest(f"payload must be at most {MAX_PAYLOAD} bytes")
    payload = cfg["payload"]
    has_range_walk = False
    i = 0
    n = len(payload)
    while i < n:
        if payload[i] == "%":
            known = next((p for p in ("%TIMESTAMPMS%", "%TIMESTAMPUS%", "%TIMESTAMPNS%", "%TIMESTAMP%", "%UNIQUE%", "%RANDOM%")
                          if payload.startswith(p, i)), None)
            if known:
                i += len(known)
                continue
            prefix = next((p for p in PLACEHOLDER_SPECS if payload.startswith(p, i)), None)
            if prefix:
                next_pct = payload.find("%", i + 1)
                text = (payload[i:next_pct + 1] if next_pct != -1 else payload[i:])[:40]
                if next_pct == -1:
                    raise BadRequest(f"bad payload placeholder '{text}' (malformed)")
                candidate = payload[i:next_pct + 1]
                args = candidate[len(prefix):-1].split(":")
                expected = PLACEHOLDER_SPECS[prefix]
                if len(args) != len(expected) or not all(r.match(a) for r, a in zip(expected, args)):
                    raise BadRequest(f"bad payload placeholder '{text}' (malformed)")
                if float(args[0]) > float(args[1]):
                    raise BadRequest(f"bad payload placeholder '{candidate}' (min > max)")
                has_range_walk = True
                i = next_pct + 1
                continue
        i += 1
    if has_range_walk and BENCH_IMAGE.startswith("emqx/emqtt-bench"):
        raise BadRequest("RANGE and WALK placeholders need the patched emqtt-bench image; set BENCH_IMAGE in .env")
    if bool(cfg["cert"]) != bool(cfg["key"]):
        raise BadRequest("client certificate and client key must be given together")
    if cfg["mode"] != "conn" and (not cfg["topic"] or any(c.isspace() for c in cfg["topic"])):
        raise BadRequest("topic is invalid")
    if cfg["subscribers"] and (cfg["mode"] != "pub" or any(c.isspace() for c in cfg["sub_topic"])):
        raise BadRequest("subscribers need publish mode and a valid subscribe topic")
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


def bench_args(cfg, count, start, rate, role=None):
    """role="sub": one of the latency subscribers, alongside publisher devices."""
    mode = role or cfg["mode"]
    prefix = f"{cfg['prefix']}_sub_" if role == "sub" else f"{cfg['prefix']}_"
    args = [mode, "-h", cfg["host"], "-p", str(cfg["port"]), "-V", cfg["version"],
            "-c", str(count), "-R", str(rate), "-k", str(cfg["keepalive"]),
            "--prefix", prefix, "-n", str(start)]
    if cfg["username"]:
        args += ["-u", cfg["username"], "-P", cfg["password"]]
    if cfg["tls"]:
        args += ["--ssl"]
        if cfg["tls_version"]:
            args += ["--ssl-version", cfg["tls_version"]]
        if cfg["cert"]:
            args += ["--certfile", f"{CERTS_DIR}/client.crt", "--keyfile", f"{CERTS_DIR}/client.key"]
    # an 8-byte send timestamp in front of each payload lets subscribers report latency
    stamp = ["--payload-hdrs", "ts"] if cfg["subscribers"] else []
    if mode == "pub":
        args += ["-t", cfg["topic"], "-I", str(cfg["interval"]), "-s", str(cfg["size"]), "-q", str(cfg["qos"])]
        if cfg["payload"]:  # via a file so any content (JSON, quotes, newlines) survives
            args += ["-m", f"template://{CERTS_DIR}/payload.txt"]
        args += stamp
    elif mode == "sub":
        args += ["-t", cfg["sub_topic"] if role else cfg["topic"], "-q", str(cfg["qos"])] + (stamp if role else [])
    return args


def plan(total, rate, offset=0):
    """Split total connections (and total conn rate) across workers: [(count, start, rate), ...]."""
    n = math.ceil(total / PER_WORKER)
    per_rate = max(1, rate // max(n, 1))
    out = []
    for i in range(n):
        count = min(PER_WORKER, total - i * PER_WORKER)
        # emqtt-bench crashes when its per-thread connect interval rounds to 0 ms; it runs min(clients, CPUs) threads
        out.append((count, offset + i * PER_WORKER, min(per_rate, 1000 * max(1, min(count, ncpu or 1)))))
    return out


def split(total, machines):
    """Devices per machine and each machine's first client number, so client IDs never collide."""
    shares = [total // machines + (i < total % machines) for i in range(machines)]
    return [(n, sum(shares[:i])) for i, n in enumerate(shares)]


def estimate_mem(total, tls=True):
    return math.ceil(total / PER_WORKER) * WORKER_MEM + total * (CONN_MEM if tls else CONN_MEM_PLAIN)


def max_devices():
    return MAC_MAX_DEVICES if any(name in docker_os for name in MAC_DOCKER) else None


def preflight(total, subscribers=0, tls=True):
    """Refuse what this machine can't open: total = devices on this machine (its share when there are peers)."""
    cap = max_devices()
    if cap and total + subscribers > cap:
        raise BadRequest(f"{total + subscribers:,} connections is more than Docker on macOS ({docker_os}) can open: all "
                         f"workers share the Mac's 16,384 source ports, so keep it at {cap:,} or less, run swarmqtt on "
                         f"a Linux machine, or add peers")
    need = estimate_mem(total, tls) + estimate_mem(subscribers, tls)  # subscribers get their own worker(s)
    if mem_total and need > mem_total * MAX_MEM_SHARE:
        raise BadRequest(f"{total + subscribers:,} connections on this machine need ~{need / 2**30:.1f} GB RAM but "
                         f"Docker has {mem_total / 2**30:.1f} GB: lower the devices, give Docker more RAM, or add peers")


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
                m, lat = METRIC.search(text), LATENCY.search(text)
                with lock:
                    if m:
                        stats.setdefault(container.name, {})[m[1]] = {"total": int(m[2]), "rate": float(m[3])}
                    elif lat:
                        latency[container.name] = int(lat[1])
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


def save_logs(containers):
    stamp = time.strftime("%Y%m%d-%H%M%S")
    saved = 0
    for c in containers:
        try:
            data = c.logs()
            with open(os.path.join(LOGS_DIR, f"{stamp}-{c.name}.log"), "wb") as f:
                f.write(data)
            saved += 1
        except Exception as e:
            with lock:
                logs.append(f"[{c.name}] log not saved: {e}")
    if saved:
        with lock:
            logs.append(f"Worker logs saved as logs/{stamp}-*.log")


def remove_workers():
    ws = workers()
    save_logs(ws)
    for c in ws:
        c.remove(force=True)
    with lock:
        stats.clear()
        usage.clear()
        latency.clear()


def launch(cfg, total, offset, subscribers, labels):
    """Replace this machine's workers with `total` publishers (and `subscribers` subscribers)."""
    import docker
    remove_workers()
    write_certs(cfg, CERTS_DIR)
    try:
        dk.images.get(BENCH_IMAGE)
    except docker.errors.ImageNotFound:
        dk.images.pull(BENCH_IMAGE)
    jobs = [(f"swarmqtt-worker-{i}", bench_args(cfg, *p)) for i, p in enumerate(plan(total, cfg["rate"], offset))]
    jobs += [(f"swarmqtt-sub-{i}", bench_args(cfg, *p, role="sub"))
             for i, p in enumerate(plan(subscribers, cfg["rate"]))]
    with lock:
        logs.append(f"Starting {total:,} devices" + (f" + {subscribers:,} subscribers" if subscribers else "")
                    + f" in {len(jobs)} worker(s)")
    for name, args in jobs:
        c = dk.containers.run(
            BENCH_IMAGE, args, name=name, labels={LABEL: "worker", **labels}, detach=True,
            entrypoint=BENCH_ENTRYPOINT,
            volumes={CERTS_VOLUME: {"bind": CERTS_DIR, "mode": "ro"}},
            ulimits=[docker.types.Ulimit(name="nofile", soft=1048576, hard=1048576)],
            sysctls={"net.ipv4.ip_local_port_range": "1024 65535"},
            extra_hosts={"host.docker.internal": "host-gateway"})
        attach(c)


# --- other machines running swarmqtt (PEERS) ---

def peer_call(peer, path, body=None):
    req = urllib.request.Request(peer + path, None if body is None else json.dumps(body).encode(),
                                 {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{peer}: {json.loads(e.read() or b'{}').get('error', e)}") from None


def local_status():
    ws = [{"name": c.name, "status": c.status} for c in workers()]
    with lock:
        totals = {}
        for name, m in stats.items():
            for k, v in m.items():
                # latency subscribers are not devices: keep their connects out of the device counts
                k = f"sub_{k}" if name.startswith("swarmqtt-sub-") and k != "recv" else k
                t = totals.setdefault(k, {"total": 0, "rate": 0.0})
                t["total"] += v["total"]
                t["rate"] = round(t["rate"] + v["rate"], 2)
        lat = list(latency.values())
        load = {"mem": sum(u["mem"] for u in usage.values()), "mem_total": mem_total,
                "cpu": round(sum(u["cpu"] for u in usage.values()), 2), "ncpu": ncpu,
                "max_devices": max_devices(), "docker_os": docker_os}
        return {"workers": ws, "totals": totals, "load": load,
                "latency_ms": round(sum(lat) / len(lat), 1) if lat else None, "logs": list(logs)[-50:]}


def merged_status():
    """This machine plus every peer, summed. A peer that doesn't answer is reported, not fatal."""
    s = local_status()
    for peer in PEERS:
        try:
            p = peer_call(peer, "/api/status")
        except Exception as e:
            s["logs"].append(f"[{peer}] unreachable: {e}")
            continue
        s["workers"] += [{**w, "name": f"{peer} {w['name']}"} for w in p["workers"]]
        for k, v in p["totals"].items():
            t = s["totals"].setdefault(k, {"total": 0, "rate": 0.0})
            t["total"] += v["total"]
            t["rate"] = round(t["rate"] + v["rate"], 2)
        for k in ("mem", "mem_total", "cpu", "ncpu"):
            s["load"][k] += p["load"][k]
        s["logs"] += [f"[{peer}] {line}" for line in p["logs"][-10:]]
    return s


# --- runs: one start = one run, recorded to DATA_DIR/runs/<id>.json ---

def judge(n, connected, failed, fail_pct, load_full):
    """Did a step of n devices hold? -> (ok, reason)."""
    if failed > n * fail_pct / 100:
        return False, f"{failed:,} connect failures (more than {fail_pct:g}% of {n:,})"
    if connected < n * 0.99:
        return False, f"only {connected:,} of {n:,} devices connected"
    if load_full:
        return False, "load machine reached its RAM limit, so this step measured the load generator, not the broker"
    return True, ""


def public_cfg(cfg):
    return {**{k: v for k, v in cfg.items() if k not in SECRET_FIELDS}, "has_cert": bool(cfg["cert"])}


class Run:
    def __init__(self, cfg, record=None):
        r = record or {"id": time.strftime("%Y%m%d-%H%M%S"), "started": time.time(), "cfg": public_cfg(cfg),
                       "status": "running", "reason": "", "steps": [], "samples": [],
                       "machines": 1 + len(PEERS), "mem_total": mem_total, "ncpu": ncpu}
        self.record, self.cfg = r, cfg
        self.cancel = threading.Event()
        self.ends_at = None
        self.thread = threading.Thread(target=self.execute, daemon=True)

    def save(self):
        os.makedirs(RUNS_DIR, exist_ok=True)
        path = os.path.join(RUNS_DIR, self.record["id"] + ".json")
        with open(path + ".tmp", "w") as f:
            json.dump(self.record, f)
        os.replace(path + ".tmp", path)

    def deploy(self, n, deadline):
        """Start n devices across this machine and the peers. Workers carry the deadline so a restart can honour it."""
        cfg, labels = self.cfg, {"swarmqtt.run": self.record["id"], "swarmqtt.ends_at": str(deadline or "")}
        (local, offset), *rest = split(n, 1 + len(PEERS))
        for peer, (share, first) in zip(PEERS, rest):
            # peers stop themselves at the deadline even if this controller dies
            left = max(1, round(deadline - time.time())) if deadline else 0
            peer_call(peer, "/api/start", {**cfg, "total": share, "offset": cfg["offset"] + first,
                                           "subscribers": 0, "steps": [], "duration": left})
        launch(cfg, local, cfg["offset"] + offset, cfg["subscribers"], labels)

    def sample(self, n):
        s = merged_status()
        t, L = s["totals"], s["load"]
        get = lambda k, f="total": t.get(k, {}).get(f, 0)
        row = {"t": round(time.time() - self.record["started"], 1), "devices": n,
               "connected": get("connect_succ"), "failed": get("connect_fail"),
               "conn_rate": get("connect_succ", "rate"), "pub_rate": get("pub", "rate"),
               # every subscriber gets every message: per subscriber = what the broker delivered
               "recv_rate": round(get("recv", "rate") / max(self.cfg["subscribers"], 1), 2), "latency_ms": s["latency_ms"], "mem": L["mem"],
               "mem_total": L["mem_total"], "cpu": L["cpu"], "ncpu": L["ncpu"]}
        self.record["samples"].append(row)
        if len(self.record["samples"]) % 5 == 0:  # every 10 s, so a restart loses little
            self.save()
        return row

    def step(self, n):
        """Run one load level until it settles and holds (ramp) or until the duration ends. -> ok?"""
        cfg, ramp = self.cfg, bool(self.cfg["steps"])
        t0 = time.time()
        settle = n / cfg["rate"] + 60 if ramp else 0
        hold = cfg["hold"] if ramp else cfg["duration"]
        self.ends_at = t0 + settle + hold if hold else None
        self.deploy(n, self.ends_at)
        settled_at, rows, every, ok, reason = (None if ramp else t0), [], [], True, ""
        while not self.cancel.wait(SAMPLE_EVERY):
            row = self.sample(n)
            every.append(row)
            now = time.time()
            if settled_at is not None:
                rows.append(row)
            elif row["connected"] >= n * 0.99 or now - t0 > settle:
                settled_at = now
            if ramp:
                ok, reason = judge(n, n, row["failed"], cfg["fail_pct"], False)
                if not ok:  # failures are cumulative, so this can't recover within the step
                    break
                if settled_at is not None and now - settled_at >= hold:
                    break
            elif self.ends_at and now >= self.ends_at:
                break
        last = rows[-1] if rows else (self.record["samples"][-1] if self.record["samples"] else {})
        peak = lambda k: max((r[k] or 0 for r in every), default=0)  # whole step, settled or not
        mem_full = bool(last) and last["mem_total"] and peak("mem") > last["mem_total"] * MAX_MEM_SHARE
        if ok:
            ok, reason = judge(n, last.get("connected", 0), last.get("failed", 0), cfg["fail_pct"], mem_full)
        lats = [r["latency_ms"] for r in rows if r["latency_ms"] is not None]
        self.record["steps"].append({
            "devices": n, "connected": last.get("connected", 0), "failed": last.get("failed", 0),
            "held_s": round(time.time() - settled_at) if settled_at else 0,
            "pub_rate": round(sum(r["pub_rate"] for r in rows) / len(rows), 1) if rows else 0,
            "recv_rate": round(sum(r["recv_rate"] for r in rows) / len(rows), 1) if rows else 0,
            "latency_ms": round(sum(lats) / len(lats), 1) if lats else None,
            "peak_mem": peak("mem"), "peak_cpu": peak("cpu"),
            "ok": ok and not self.cancel.is_set(), "reason": "stopped by user" if self.cancel.is_set() else reason})
        self.save()
        return ok and not self.cancel.is_set()

    def execute(self):
        r = self.record
        try:
            for n in self.cfg["steps"] or [self.cfg["total"]]:
                if not self.step(n):
                    break
            r["status"] = "stopped" if self.cancel.is_set() else "done"
        except Exception as e:
            r.update(status="failed", reason=f"{type(e).__name__}: {e}")
            with lock:
                logs.append(f"Run failed: {r['reason']}")
        finally:
            self.ends_at = None
            teardown()
            r["ended"] = time.time()
            self.save()
            with lock:
                logs.append(f"Run {r['id']} {r['status']}")

    def resume(self):
        """After a web restart: keep sampling the workers still running and stop them at their deadline."""
        n = self.record["samples"][-1]["devices"] if self.record["samples"] else 0
        while not self.cancel.wait(SAMPLE_EVERY) and not (self.ends_at and time.time() >= self.ends_at):
            self.sample(n)
        teardown()
        self.record.update(status="interrupted", reason="web server restarted during the run", ended=time.time())
        self.save()


def teardown():
    for peer in PEERS:
        try:
            peer_call(peer, "/api/stop", {})
        except Exception as e:
            with lock:
                logs.append(f"[{peer}] stop failed: {e}")
    remove_workers()


def stop():
    r = current
    if r and r.thread.is_alive():
        r.cancel.set()
        r.thread.join(60)
    else:
        teardown()


def start(cfg):
    global current
    biggest = max(cfg["steps"] or [cfg["total"]])
    preflight(split(biggest, 1 + len(PEERS))[0][0], cfg["subscribers"], cfg["tls"])
    stop()
    with lock:
        logs.clear()
    current = Run(cfg)
    current.save()
    current.thread.start()


def status():
    s = merged_status()
    r = current
    run = None
    if r:
        rec = r.record
        run = {k: rec[k] for k in ("id", "status", "reason", "steps")}
        run["samples"] = rec["samples"][-900:]  # last 30 min for the live chart
        run["active"] = r.thread.is_alive()
    s.update(running=any(w["status"] == "running" for w in s["workers"]), run=run,
             # seconds left, not a timestamp: the Docker VM clock can drift from the viewer's
             ends_in=r and r.ends_at and max(0, round(r.ends_at - time.time())))
    return s


def list_runs():
    out = []
    for path in sorted(glob.glob(os.path.join(RUNS_DIR, "*.json")), reverse=True)[:100]:
        try:
            with open(path) as f:
                r = json.load(f)
        except (OSError, ValueError):
            continue
        out.append({"id": r["id"], "started": r["started"], "status": r["status"], "reason": r["reason"],
                    "host": r["cfg"]["host"], "port": r["cfg"]["port"],
                    "devices": max([s["devices"] for s in r["steps"]] or [r["cfg"]["total"]]),
                    "max_ok": max([s["devices"] for s in r["steps"] if s["ok"]] or [0]),
                    "steps": len(r["steps"])})
    return out


def load_run(run_id):
    if not RUN_ID.match(run_id):
        raise BadRequest("bad run id")
    if current and current.record["id"] == run_id:
        return current.record
    try:
        with open(os.path.join(RUNS_DIR, run_id + ".json")) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def run_csv(record):
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=SAMPLE_FIELDS, extrasaction="ignore")
    w.writeheader()
    w.writerows(record["samples"])
    return out.getvalue().encode()


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json", filename=None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            if self.path == "/":
                with open(os.path.join(os.path.dirname(__file__), "index.html"), "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
            if self.path == "/api/status":
                return self._send(200, status())
            if self.path == "/api/runs":
                return self._send(200, list_runs())
            m = re.fullmatch(r"/api/runs/([\w-]+)(\.json|\.csv|/report\.pdf)?", self.path)
            if m:
                rec = load_run(m[1])
                if rec is None:
                    return self._send(404, {"error": "run not found"})
                if m[2] == ".csv":
                    return self._send(200, run_csv(rec), "text/csv", f"swarmqtt-{m[1]}.csv")
                if m[2] == "/report.pdf":
                    import report
                    return self._send(200, report.build(rec), "application/pdf", f"swarmqtt-{m[1]}.pdf")
                return self._send(200, rec, filename=f"swarmqtt-{m[1]}.json" if m[2] else None)
            self._send(404, {"error": "not found"})
        except BadRequest as e:
            self._send(400, {"error": str(e)})
        except Exception as e:
            self._send(500, {"error": f"{type(e).__name__}: {e}"})

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
                return self._send(200, {"ok": True, "run_id": current.record["id"]})
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


class PeerHandler(Handler):
    """The API for a controller on another machine. Needs PEER_TOKEN; the web UI port stays localhost-only."""

    def _authorized(self):
        if hmac.compare_digest(self.headers.get("Authorization", ""), f"Bearer {TOKEN}"):
            return True
        self._send(401, {"error": "bad or missing peer token"})
        return False

    def do_GET(self):
        if self.path == "/api/status" and self._authorized():
            super().do_GET()
        elif self.path != "/api/status":
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path in ("/api/start", "/api/stop") and self._authorized():
            super().do_POST()
        elif self.path not in ("/api/start", "/api/stop"):
            self._send(404, {"error": "not found"})


def main():
    global dk, mem_total, ncpu, docker_os, current
    import docker
    dk = docker.from_env()
    info = dk.info()
    mem_total, ncpu, docker_os = info["MemTotal"], info["NCPU"], info.get("OperatingSystem", "")
    left = workers()
    for c in left:  # reattach after a web restart
        attach(c)
    if left:
        rec = load_run(left[0].labels.get("swarmqtt.run", "")) if RUN_ID.match(left[0].labels.get("swarmqtt.run", "")) else None
        if rec:
            current = Run({**rec["cfg"], **{k: "" for k in SECRET_FIELDS}}, rec)
            ends = left[0].labels.get("swarmqtt.ends_at")
            current.ends_at = float(ends) if ends else None
            current.thread = threading.Thread(target=current.resume, daemon=True)
            current.thread.start()
    import mcp_server
    threading.Thread(target=mcp_server.serve, daemon=True).start()
    if TOKEN:
        threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", 8082), PeerHandler).serve_forever, daemon=True).start()
        print("Peer API on :8082")
    if PEERS and not TOKEN:
        print("PEERS is set but PEER_TOKEN is empty: peers will refuse requests")
    print("Listening on :8080 (UI) and :8081/mcp (MCP)")
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
