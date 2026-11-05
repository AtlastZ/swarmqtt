import os
import tempfile
import unittest
import unittest.mock

import app

BASE = {"host": "broker", "port": 8883, "total": 100000, "rate": 2000, "keepalive": 60,
        "interval": 10000, "size": 256, "qos": 0, "tls": True, "mode": "pub"}


class AppTest(unittest.TestCase):
    def test_deploy_splits_rate_across_peers(self):
        saved_peer_call, saved_launch, saved_peers = app.peer_call, app.launch, app.PEERS
        peer_calls, launches = [], []
        try:
            app.peer_call = lambda peer, path, body=None: peer_calls.append((peer, path, body))
            app.launch = lambda cfg, total, offset, subscribers, labels: launches.append((cfg, total, offset, subscribers, labels))
            app.PEERS = ["http://peer1:8082", "http://peer2:8082"]

            cfg = app.parse_cfg({**BASE, "rate": 500})
            run = app.Run(cfg)
            run.deploy(1000, None)
            local_rate = launches[-1][0]["rate"]
            peer_rates = [body["rate"] for _, _, body in peer_calls[-2:]]
            self.assertEqual(local_rate + sum(peer_rates), 500)
            self.assertEqual(sorted([local_rate] + peer_rates), [166, 167, 167])

            cfg2 = app.parse_cfg({**BASE, "rate": 2})
            run2 = app.Run(cfg2)
            run2.deploy(1000, None)
            local_rate2 = launches[-1][0]["rate"]
            peer_rates2 = [body["rate"] for _, _, body in peer_calls[-2:]]
            self.assertTrue(all(r >= 1 for r in [local_rate2] + peer_rates2))
        finally:
            app.peer_call, app.launch, app.PEERS = saved_peer_call, saved_launch, saved_peers

    def test_connect_retries(self):
        cfg3 = app.parse_cfg({**BASE, "retries": 3})
        self.assertEqual(cfg3["retries"], 3)
        for role in (None, "sub"):
            args3 = app.bench_args(cfg3, 10, 0, 10, role=role)
            self.assertIn("--num-retry-connect", args3)
            self.assertEqual(args3[args3.index("--num-retry-connect") + 1], "3")
        cfg_conn = app.parse_cfg({**BASE, "mode": "conn", "retries": 3})
        args_conn = app.bench_args(cfg_conn, 10, 0, 10)
        self.assertIn("--num-retry-connect", args_conn)
        self.assertEqual(args_conn[args_conn.index("--num-retry-connect") + 1], "3")

        cfg0 = app.parse_cfg({**BASE, "retries": 0})
        self.assertEqual(cfg0["retries"], 0)
        for role in (None, "sub"):
            args0 = app.bench_args(cfg0, 10, 0, 10, role=role)
            self.assertNotIn("--num-retry-connect", args0)

        for bad in (-1, 11):
            with self.assertRaises(app.BadRequest):
                app.parse_cfg({**BASE, "retries": bad})

        import inspect
        import mcp_server
        self.assertEqual(inspect.signature(mcp_server.start_load_test).parameters["connect_retries"].default, 0)
        saved_start, saved_current = app.start, app.current
        try:
            started = []
            class DummyRun:
                record = {"id": "dummy"}
            app.start = lambda c: started.append(c)
            app.current = DummyRun()
            mcp_server.start_load_test("broker", 100, connect_retries=3)
            self.assertEqual(started[0]["retries"], 3)
        finally:
            app.start, app.current = saved_start, saved_current

    def test_wait_before_publishing(self):
        cfg_on = app.parse_cfg({**BASE, "mode": "pub", "wait_publish": True})
        self.assertTrue(cfg_on["wait_publish"])
        args_pub = app.bench_args(cfg_on, 10, 0, 10)
        self.assertIn("--wait-before-publishing", args_pub)

        args_sub_role = app.bench_args(cfg_on, 10, 0, 10, role="sub")
        self.assertNotIn("--wait-before-publishing", args_sub_role)

        cfg_off = app.parse_cfg({**BASE, "mode": "pub", "wait_publish": False})
        self.assertFalse(cfg_off["wait_publish"])
        self.assertNotIn("--wait-before-publishing", app.bench_args(cfg_off, 10, 0, 10))

        cfg_sub = app.parse_cfg({**BASE, "mode": "sub", "wait_publish": True})
        self.assertNotIn("--wait-before-publishing", app.bench_args(cfg_sub, 10, 0, 10))

        import inspect
        import mcp_server
        self.assertFalse(inspect.signature(mcp_server.start_load_test).parameters["wait_before_publishing"].default)
        saved_start, saved_current = app.start, app.current
        try:
            started = []
            class DummyRun:
                record = {"id": "dummy"}
            app.start = lambda c: started.append(c)
            app.current = DummyRun()
            mcp_server.start_load_test("broker", 100, wait_before_publishing=True)
            self.assertTrue(started[0]["wait_publish"])
        finally:
            app.start, app.current = saved_start, saved_current

    def test_no_stale_publish_twice_caveat(self):
        report_path = os.path.join(os.path.dirname(app.__file__), "report.py")
        with open(report_path) as f:
            report_src = f.read()
        self.assertNotIn("published*", report_src)
        self.assertNotIn("about twice", report_src)
        self.assertIn("Connected is cumulative", report_src)

        html_path = os.path.join(os.path.dirname(app.__file__), "index.html")
        with open(html_path) as f:
            html_src = f.read()
        self.assertNotIn("published*", html_src)
        self.assertNotIn("about twice", html_src)

    def test_plan_covers_total_without_overlap(self):
        p = app.plan(100001, 2000)
        self.assertEqual(sum(c for c, _, _ in p), 100001)
        self.assertEqual([s for _, s, _ in p], [0, 25000, 50000, 75000, 100000])
        self.assertTrue(all(c <= app.PER_WORKER for c, _, _ in p))

    def test_plan_rates_proportional_to_device_counts(self):
        p = app.plan(33334, 167)
        self.assertEqual(len(p), 2)
        (c1, s1, r1), (c2, s2, r2) = p
        self.assertEqual((c1, c2), (25000, 8334))
        self.assertAlmostEqual(r1, 125, delta=1)
        self.assertAlmostEqual(r2, 42, delta=1)
        self.assertIn(r1 + r2, (166, 167, 168))
        t1, t2 = c1 / r1, c2 / r2
        self.assertLess(abs(t1 - t2) / max(t1, t2), 0.05)

    def test_bench_args_tls_with_client_cert(self):
        cfg = app.parse_cfg({**BASE, "cert": "C", "key": "K"})
        args = app.bench_args(cfg, 25000, 25000, 500)
        self.assertIn("--ssl", args)
        self.assertEqual(args[args.index("--certfile") + 1], "/certs/client.crt")
        self.assertEqual(args[args.index("-n") + 1], "25000")

    def test_custom_payload_uses_template_file(self):
        cfg = app.parse_cfg({**BASE, "payload": '{"t":%TIMESTAMPMS%}'})
        args = app.bench_args(cfg, 10, 0, 10)
        self.assertEqual(args[args.index("-m") + 1], "template:///certs/payload.txt")
        self.assertNotIn("-m", app.bench_args(app.parse_cfg(BASE), 10, 0, 10))
        with tempfile.TemporaryDirectory() as d:
            with open(app.write_certs(cfg, d)["payload"]) as f:
                self.assertEqual(f.read(), '{"t":%TIMESTAMPMS%}')

    def test_payload_range_walk_placeholders(self):
        raw_payload = '{"r":%RANGE:1:10%,"w":%WALK:20.0:30.0:0.2%}'
        with tempfile.TemporaryDirectory() as d:
            paths = app.write_certs({**BASE, "payload": raw_payload, "cert": "", "key": "", "ca": ""}, d)
            with open(paths["payload"]) as f:
                self.assertEqual(f.read(), raw_payload)

        for bad in ("%RANGE:a:b%", "%WALK:20:30%", "%WALK:0:10:-1%"):
            with self.assertRaises(app.BadRequest) as ctx:
                app.parse_cfg({**BASE, "payload": bad})
            self.assertIn(bad, str(ctx.exception))
            self.assertIn("(malformed)", str(ctx.exception))

        with self.assertRaises(app.BadRequest) as ctx:
            app.parse_cfg({**BASE, "payload": '{"r":%RANGE:1:10,"ts":%TIMESTAMPMS%}'})
        self.assertIn('%RANGE:1:10,"ts":%', str(ctx.exception))
        self.assertIn("(malformed)", str(ctx.exception))

        for bad in ("%RANGE:30:20%", "%WALK:30:20:1%"):
            with self.assertRaises(app.BadRequest) as ctx:
                app.parse_cfg({**BASE, "payload": bad})
            self.assertIn(bad, str(ctx.exception))
            self.assertIn("(min > max)", str(ctx.exception))

        saved_image = app.BENCH_IMAGE
        try:
            app.BENCH_IMAGE = "emqtt-bench:range-walk"
            for p in ('{"r":%RANGE:9.5:10%}', '%UNIQUE%%RANGE:1:5%'):
                cfg = app.parse_cfg({**BASE, "payload": p})
                args = app.bench_args(cfg, 10, 0, 10)
                self.assertEqual(args[args.index("-m") + 1], "template:///certs/payload.txt")

            app.BENCH_IMAGE = "emqx/emqtt-bench:latest"
            with self.assertRaises(app.BadRequest):
                app.parse_cfg({**BASE, "payload": '%RANGE:1:10%'})
            cfg_plain = app.parse_cfg({**BASE, "payload": '{"t":%TIMESTAMPMS%}'})
            self.assertEqual(cfg_plain["payload"], '{"t":%TIMESTAMPMS%}')
        finally:
            app.BENCH_IMAGE = saved_image

    def test_estimate_mem_and_refuse_when_too_big(self):
        self.assertEqual(app.estimate_mem(100000), 4 * app.WORKER_MEM + 100000 * app.CONN_MEM)
        self.assertLess(app.estimate_mem(100000, tls=False), app.estimate_mem(100000))
        app.mem_total = 4 * 2**30
        try:
            with self.assertRaises(app.BadRequest):
                app.start(app.parse_cfg(BASE))  # 100k devices ~13 GB, refused before touching Docker
        finally:
            app.mem_total = 0

    def test_mac_docker_caps_devices_by_source_ports(self):
        try:
            for name in ("OrbStack", "Docker Desktop"):
                app.docker_os = name
                self.assertEqual(app.max_devices(), app.MAC_MAX_DEVICES)
                app.preflight(app.MAC_MAX_DEVICES)
                with self.assertRaisesRegex(app.BadRequest, "source ports") as ctx:
                    app.preflight(app.MAC_MAX_DEVICES + 1)
                self.assertIn("MAC_MAX_DEVICES", str(ctx.exception))
                with self.assertRaisesRegex(app.BadRequest, "source ports") as ctx:  # subscribers use ports too
                    app.preflight(app.MAC_MAX_DEVICES, subscribers=1)
                self.assertIn("MAC_MAX_DEVICES", str(ctx.exception))
            app.docker_os = "Ubuntu 24.04.3 LTS"
            self.assertIsNone(app.max_devices())
            app.preflight(app.MAC_MAX_DEVICES + 1)
        finally:
            app.docker_os = ""

    def test_env_int(self):
        with unittest.mock.patch.dict(os.environ):
            os.environ.pop("TEST_VAR", None)
            self.assertEqual(app._env_int("TEST_VAR", 15000), 15000)
        with unittest.mock.patch.dict(os.environ, {"TEST_VAR": ""}):
            self.assertEqual(app._env_int("TEST_VAR", 15000), 15000)
        with unittest.mock.patch.dict(os.environ, {"TEST_VAR": "50000"}):
            self.assertEqual(app._env_int("TEST_VAR", 15000), 50000)
        for bad in ("0", "-1", "abc"):
            with unittest.mock.patch.dict(os.environ, {"TEST_VAR": bad}):
                with self.assertRaises(SystemExit) as ctx:
                    app._env_int("TEST_VAR", 15000)
                self.assertIn("TEST_VAR", str(ctx.exception))

    def test_local_status_machines(self):
        saved_workers, saved_peers = app.workers, app.PEERS
        try:
            app.workers = lambda: []
            app.PEERS = ["http://peer1:8082", "http://peer2:8082"]
            self.assertEqual(app.local_status()["load"]["machines"], 1 + len(app.PEERS))
        finally:
            app.workers, app.PEERS = saved_workers, saved_peers

    def test_stop_saves_worker_logs_before_removing(self):
        events = []

        broken = set()

        class Worker:
            def __init__(self, name):
                self.name = name

            def logs(self):
                events.append(("logs", self.name))
                if self.name in broken:
                    raise RuntimeError("log stream gone")
                return f"{self.name} done\n".encode()

            def remove(self, force):
                events.append(("remove", self.name))

        class Containers:
            def list(self, **kw):
                return [Worker("swarmqtt-worker-0"), Worker("swarmqtt-worker-1")]

        class Docker:
            containers = Containers()

        saved_dk, saved_dir = app.dk, app.LOGS_DIR
        with tempfile.TemporaryDirectory() as d:
            app.dk, app.LOGS_DIR = Docker(), d
            try:
                app.stop()
                files = sorted(os.listdir(d))
                self.assertEqual(len(files), 2)
                self.assertTrue(files[0].endswith("-swarmqtt-worker-0.log"))
                with open(os.path.join(d, files[0])) as f:
                    self.assertEqual(f.read(), "swarmqtt-worker-0 done\n")
                self.assertLess(events.index(("logs", "swarmqtt-worker-0")), events.index(("remove", "swarmqtt-worker-0")))

                with tempfile.TemporaryDirectory() as d2:
                    app.LOGS_DIR = d2
                    broken.add("swarmqtt-worker-0")
                    events.clear()
                    app.stop()
                    self.assertEqual([f.split("-", 2)[2] for f in os.listdir(d2)], ["swarmqtt-worker-1.log"])
                    self.assertIn(("remove", "swarmqtt-worker-0"), events)
                    broken.clear()

                events.clear()
                app.LOGS_DIR = os.path.join(d, "missing")
                app.stop()
                self.assertIn(("remove", "swarmqtt-worker-1"), events)
            finally:
                app.dk, app.LOGS_DIR = saved_dk, saved_dir

    def test_default_connection_rate_is_gentle(self):
        import inspect
        import mcp_server
        self.assertEqual(inspect.signature(mcp_server.start_load_test).parameters["connections_per_sec"].default, 500)
        with open(os.path.join(os.path.dirname(app.__file__), "index.html")) as f:
            self.assertIn('name="rate" type="number" min="1" value="500"', f.read())

    def test_duration_defaults_to_unlimited(self):
        self.assertEqual(app.parse_cfg(BASE)["duration"], 0)
        self.assertEqual(app.parse_cfg({**BASE, "duration": "600"})["duration"], 600)

    def test_ramp_steps_keep_order_and_repeats_and_set_total(self):
        cfg = app.parse_cfg({**BASE, "steps": "100000, 50000, 100000"})
        self.assertEqual((cfg["steps"], cfg["total"]), ([100000, 50000, 100000], 100000))

    def test_headline_highest_passing_step_any_order(self):
        import report
        cfg = app.parse_cfg({**BASE, "steps": "100000, 50000, 30000"})
        rec = {"id": "1", "started": 0, "cfg": cfg, "status": "stopped", "reason": "",
               "steps": [{"devices": 100000, "ok": True, "reason": ""},
                         {"devices": 50000, "ok": True, "reason": ""},
                         {"devices": 30000, "ok": False, "reason": "failed"}]}
        self.assertEqual(report.headline(rec), "Held 100,000 devices. At 30,000 devices: failed.")

        rec_done = {"id": "1", "started": 0, "cfg": cfg, "status": "done", "reason": "",
                    "steps": [{"devices": 100000, "ok": True, "reason": ""},
                              {"devices": 50000, "ok": True, "reason": ""}]}
        self.assertEqual(report.headline(rec_done), "Held 100,000 devices. Every step passed, so the limit is above 100,000.")

    def test_split_across_machines_without_id_overlap(self):
        self.assertEqual(app.split(10, 3), [(4, 0), (3, 4), (3, 7)])
        self.assertEqual([s for _, s, _ in app.plan(30000, 100, offset=7)], [7, 25007])

    def test_plan_caps_rate_emqtt_bench_can_handle(self):
        self.assertEqual(app.plan(1, 2000)[0][2], 1000)  # 1 client = 1 thread: over 1000/s crashes the bench

    def test_judge(self):
        self.assertEqual(app.judge(1000, 1000, 0, 1, False), (True, ""))
        self.assertFalse(app.judge(1000, 1000, 11, 1, False)[0])  # 1.1% failed
        self.assertFalse(app.judge(1000, 980, 0, 1, False)[0])  # under 99% connected
        self.assertFalse(app.judge(1000, 1000, 0, 1, True)[0])  # load machine out of RAM

    def test_latency_subscribers(self):
        cfg = app.parse_cfg({**BASE, "subscribers": 2})
        sub = app.bench_args(cfg, 2, 0, 10, role="sub")
        self.assertEqual(sub[0], "sub")
        self.assertEqual(sub[sub.index("-t") + 1], "iot/+/telemetry")
        self.assertEqual(sub[sub.index("--prefix") + 1], "lt_sub_")
        self.assertIn("--payload-hdrs", sub)
        self.assertIn("--payload-hdrs", app.bench_args(cfg, 10, 0, 10))
        self.assertNotIn("--payload-hdrs", app.bench_args(app.parse_cfg(BASE), 10, 0, 10))

    def test_report_and_csv(self):
        import report
        cfg = app.parse_cfg({**BASE, "steps": "100,200", "subscribers": 1})
        rec = {"id": "20261030-120000", "started": 0, "ended": 300, "cfg": app.public_cfg(cfg), "status": "done",
               "reason": "", "machines": 1, "mem_total": 4 * 2**30, "ncpu": 2,
               "samples": [{"t": t, "devices": 100, "connected": 100, "failed": 0, "conn_rate": 0, "pub_rate": 20,
                            "recv_rate": 10, "latency_ms": 3, "mem": 6e8, "mem_total": 4 * 2**30, "cpu": 0.1,
                            "ncpu": 2} for t in range(0, 300, 2)],
               "steps": [{"devices": 100, "connected": 100, "failed": 0, "held_s": 60, "pub_rate": 20,
                          "recv_rate": 10, "latency_ms": 3, "peak_mem": 6e8, "peak_cpu": 0.1, "ok": True, "reason": ""},
                         {"devices": 200, "connected": 150, "failed": 50, "held_s": 0, "pub_rate": 20,
                          "recv_rate": 10, "latency_ms": None, "peak_mem": 6e8, "peak_cpu": 0.1, "ok": False,
                          "reason": "50 connect failures (more than 1% of 200)"}]}
        self.assertNotIn("password", rec["cfg"])
        self.assertEqual(report.headline(rec), "Held 100 devices. At 200 devices: 50 connect failures (more than 1% of 200).")
        self.assertTrue(report.build(rec).startswith(b"%PDF"))
        self.assertTrue(app.run_csv(rec).startswith(b"t,devices,connected"))

    def test_parse_cfg_rejects_bad_input(self):
        for bad in ({"host": "a b"}, {"port": 0}, {"total": 0}, {"mode": "x"}, {"cert": "C"}, {"payload": "x" * (app.MAX_PAYLOAD + 1)},
                    {"duration": -1}, {"steps": "10,x"}, {"fail_pct": 101},
                    {"subscribers": 1, "mode": "conn"}):
            with self.assertRaises(app.BadRequest):
                app.parse_cfg({**BASE, **bad})

    def test_mcp_cfg_maps_args_and_rejects_bad_input(self):
        import mcp_server
        cfg = mcp_server._cfg(**{**BASE, "cert": "C", "key": "K"})
        self.assertEqual((cfg["cert"], cfg["total"]), ("C", 100000))
        with self.assertRaises(mcp_server.ToolError):
            mcp_server._cfg(**{**BASE, "host": "a b"})

    def test_launch_uses_bench_entrypoint(self):
        runs = []

        class FakeImages:
            def get(self, name):
                return None

        class FakeContainers:
            def run(self, *args, **kwargs):
                runs.append((args, kwargs))
                return object()

        class FakeDocker:
            images = FakeImages()
            containers = FakeContainers()

        saved_dk = app.dk
        saved_certs = app.CERTS_DIR
        saved_attach = app.attach
        saved_remove = app.remove_workers
        try:
            with tempfile.TemporaryDirectory() as d:
                app.dk = FakeDocker()
                app.CERTS_DIR = d
                app.attach = lambda c: None
                app.remove_workers = lambda: None
                cfg = app.parse_cfg(BASE)
                app.launch(cfg, 50000, 0, 0, {})
                self.assertTrue(runs)
                for args, kwargs in runs:
                    self.assertEqual(args[0], app.BENCH_IMAGE)
                    self.assertEqual(kwargs.get("entrypoint"), ["/emqtt_bench/bin/emqtt_bench"])
                    self.assertEqual(args[1][0], "pub")
        finally:
            app.dk = saved_dk
            app.CERTS_DIR = saved_certs
            app.attach = saved_attach
            app.remove_workers = saved_remove

    def test_workers_default_to_the_image_the_web_runs_from(self):
        class Container:
            attrs = {"Config": {"Image": "ghcr.io/kit09k/swarmqtt:latest"}}

        class Containers:
            def get(self, name):
                if name != "abc123":
                    raise RuntimeError("no such container")
                return Container()

        class Docker:
            containers = Containers()

        saved_dk = app.dk
        try:
            app.dk = Docker()
            with unittest.mock.patch("socket.gethostname", return_value="abc123"):
                self.assertEqual(app.own_image(), "ghcr.io/kit09k/swarmqtt:latest")
                with unittest.mock.patch.dict(os.environ, {"BENCH_IMAGE": ""}):
                    self.assertEqual(app.resolve_bench_image(), "ghcr.io/kit09k/swarmqtt:latest")
                with unittest.mock.patch.dict(os.environ, {"BENCH_IMAGE": "emqx/emqtt-bench:latest"}):
                    self.assertEqual(app.resolve_bench_image(), "emqx/emqtt-bench:latest")
            with unittest.mock.patch("socket.gethostname", return_value="not-a-container"):
                self.assertIsNone(app.own_image())
                with unittest.mock.patch.dict(os.environ, {"BENCH_IMAGE": ""}):
                    self.assertEqual(app.resolve_bench_image(), "swarmqtt:latest")
        finally:
            app.dk = saved_dk

    def test_deploy_peer_grace_duration(self):
        saved_peer_call, saved_launch, saved_peers = app.peer_call, app.launch, app.PEERS
        peer_calls, launches = [], []
        try:
            app.peer_call = lambda peer, path, body=None: peer_calls.append((peer, path, body))
            app.launch = lambda cfg, total, offset, subscribers, labels: launches.append((cfg, total, offset, subscribers, labels))
            app.PEERS = ["http://peer1:8082"]
            self.assertEqual(getattr(app, "PEER_GRACE", None), 30)
            cfg = app.parse_cfg(BASE)
            run = app.Run(cfg)
            deadline = app.time.time() + 100
            run.deploy(1000, deadline)
            duration = peer_calls[-1][2]["duration"]
            self.assertAlmostEqual(duration, 130, delta=2)
            run.deploy(1000, None)
            self.assertEqual(peer_calls[-1][2]["duration"], 0)
        finally:
            app.peer_call, app.launch, app.PEERS = saved_peer_call, saved_launch, saved_peers

    def test_merged_status_machines_running_and_sample(self):
        saved_local_status, saved_peer_call, saved_peers = app.local_status, app.peer_call, app.PEERS
        try:
            app.local_status = lambda: {
                "workers": [{"name": "swarmqtt-worker-0", "status": "running"}],
                "totals": {}, "load": {"mem": 1, "mem_total": 2, "cpu": 0.5, "ncpu": 2}, "latency_ms": None, "logs": []
            }
            app.PEERS = ["http://peer1:8082", "http://peer2:8082"]
            app.peer_call = lambda peer, path, body=None: {
                "running": (peer == "http://peer1:8082"),
                "workers": [], "totals": {}, "load": {"mem": 0, "mem_total": 0, "cpu": 0, "ncpu": 0}, "logs": []
            }
            s = app.merged_status()
            self.assertEqual(s["load"].get("machines_running"), 2)
            run = app.Run(app.parse_cfg(BASE))
            saved_merged = app.merged_status
            try:
                app.merged_status = lambda: s
                row = run.sample(100)
                self.assertEqual(row.get("machines"), 2)
            finally:
                app.merged_status = saved_merged
        finally:
            app.local_status, app.peer_call, app.PEERS = saved_local_status, saved_peer_call, saved_peers

    def test_judge_row_and_step_ignores_final_row_with_fewer_machines(self):
        self.assertTrue(hasattr(app, "judge_row"))
        self.assertEqual(app.judge_row([]), {})
        rows_plain = [{"connected": 100}, {"connected": 200}]
        self.assertEqual(app.judge_row(rows_plain), {"connected": 200})
        rows_ramp = [
            {"connected": 38000, "machines": 3},
            {"connected": 39636, "machines": 3},
            {"connected": 13334, "machines": 1},
        ]
        self.assertEqual(app.judge_row(rows_ramp), {"connected": 39636, "machines": 3})

        run = app.Run(app.parse_cfg({**BASE, "steps": [40000], "hold": 10}))
        run.deploy = lambda n, deadline: None
        sample_seq = [
            {"t": 1.0, "devices": 40000, "connected": 39636, "failed": 0, "conn_rate": 0, "pub_rate": 0,
             "recv_rate": 0, "latency_ms": None, "mem": 100, "mem_total": 1000, "cpu": 1.0, "ncpu": 1, "machines": 3},
            {"t": 2.0, "devices": 40000, "connected": 39636, "failed": 0, "conn_rate": 0, "pub_rate": 0,
             "recv_rate": 0, "latency_ms": None, "mem": 100, "mem_total": 1000, "cpu": 1.0, "ncpu": 1, "machines": 3},
            {"t": 3.0, "devices": 40000, "connected": 13334, "failed": 0, "conn_rate": 0, "pub_rate": 0,
             "recv_rate": 0, "latency_ms": None, "mem": 100, "mem_total": 1000, "cpu": 1.0, "ncpu": 1, "machines": 1},
        ]
        seq_idx = [0]
        def fake_sample(n):
            idx = min(seq_idx[0], len(sample_seq) - 1)
            seq_idx[0] += 1
            row = sample_seq[idx]
            run.record["samples"].append(row)
            return row
        run.sample = fake_sample
        saved_wait = run.cancel.wait
        run.cancel.wait = lambda timeout: seq_idx[0] >= 3
        try:
            ok = run.step(40000)
            self.assertTrue(ok)
            self.assertEqual(run.record["steps"][-1]["connected"], 39636)
            self.assertTrue(run.record["steps"][-1]["ok"])
        finally:
            run.cancel.wait = saved_wait

    def test_report_filter_samples_skips_incomplete_rows(self):
        import report
        filter_fn = report.filter_samples
        self.assertIsNotNone(filter_fn)
        rows_plain = [{"t": 0}, {"t": 2}]
        self.assertEqual(filter_fn(rows_plain), rows_plain)
        rows_incomplete = [
            {"t": 0, "machines": 3},
            {"t": 2, "machines": 3},
            {"t": 4, "machines": 1},
        ]
        self.assertEqual(filter_fn(rows_incomplete), [{"t": 0, "machines": 3}, {"t": 2, "machines": 3}])
        rows_mixed = [
            {"t": 0},
            {"t": 2, "machines": 3},
            {"t": 4, "machines": 1},
        ]
        self.assertEqual(filter_fn(rows_mixed), [{"t": 0}, {"t": 2, "machines": 3}])

    def test_capacity(self):
        c1 = app.capacity(4 * 2**30, None, True)
        c2 = app.capacity(8 * 2**30, None, True)
        self.assertGreater(c2, c1)
        self.assertGreater(app.capacity(4 * 2**30, None, False), c1)
        self.assertEqual(app.capacity(64 * 2**30, 15000, True), 15000)
        self.assertEqual(app.capacity(8 * 2**30, 5000, True), 5000)
        self.assertEqual(app.capacity(100 * 2**20, None, True), 0)
        self.assertEqual(app.capacity(0, None, True), 0)
        self.assertEqual(app.capacity(0, 15000, True), 0)
        self.assertEqual(app.capacity(-1, None, True), 0)

    def test_split_weighted(self):
        res = app.split_weighted(100000, [3, 1, 2])
        self.assertEqual(sum(c for c, _ in res), 100000)
        cur = 0
        for c, off in res:
            self.assertEqual(off, cur)
            cur += c
        self.assertEqual(app.split_weighted(10, [1, 1, 1]), [(4, 0), (3, 4), (3, 7)])
        res2 = app.split_weighted(1000, [2, 1])
        self.assertEqual(res2, [(667, 0), (333, 667)])
        self.assertLess(abs(res2[0][0] - 1000 * 2 / 3), 1)
        self.assertLess(abs(res2[1][0] - 1000 * 1 / 3), 1)
        self.assertEqual(app.split_weighted(10, [0, 0, 0]), app.split(10, 3))
        self.assertEqual(app.split_weighted(10, [0, 0]), [(5, 0), (5, 5)])
        self.assertEqual(app.split_weighted(10, []), [])
        self.assertEqual(app.split_weighted(0, [2, 1]), [(0, 0), (0, 0)])

    def test_parse_cfg_weights_validation(self):
        saved_peers = app.PEERS
        try:
            app.PEERS = ["http://peer1:8082"]
            cfg = app.parse_cfg({**BASE, "weights": [2, 3]})
            self.assertEqual(cfg["weights"], [2, 3])
            cfg_str = app.parse_cfg({**BASE, "weights": ["10", "20"]})
            self.assertEqual(cfg_str["weights"], [10, 20])
            self.assertIsNone(app.parse_cfg(BASE).get("weights"))
            self.assertIsNone(app.parse_cfg({**BASE, "weights": []}).get("weights"))
            self.assertIsNone(app.parse_cfg({**BASE, "weights": None}).get("weights"))
            self.assertIsNone(app.parse_cfg({**BASE, "weights": ""}).get("weights"))

            for bad in ([1], [1, 2, 3], [-1, 2], [0, 0], ["a", "1"], [1.5, 2], "1,2", [True, 1]):
                with self.assertRaises(app.BadRequest):
                    app.parse_cfg({**BASE, "weights": bad})
        finally:
            app.PEERS = saved_peers

    def test_local_and_merged_status_capacity_and_machines(self):
        saved_workers, saved_peers, saved_peer_call = app.workers, app.PEERS, app.peer_call
        saved_mem, saved_os = app.mem_total, app.docker_os
        try:
            app.workers = lambda: []
            app.mem_total = 8 * 2**30
            app.docker_os = "Linux"
            ls = app.local_status()
            self.assertIn("capacity", ls["load"])
            self.assertIn("capacity_plain", ls["load"])
            self.assertGreater(ls["load"]["capacity"], 0)
            self.assertGreater(ls["load"]["capacity_plain"], ls["load"]["capacity"])

            app.PEERS = ["http://peer1:8082", "http://peer2:8082", "http://peer3:8082"]
            def fake_peer_call(peer, path, body=None):
                if peer == "http://peer1:8082":
                    return {"workers": [], "totals": {}, "load": {"mem": 0, "mem_total": 4*2**30, "cpu": 0, "ncpu": 2, "capacity": 20000, "capacity_plain": 35000}, "logs": []}
                if peer == "http://peer2:8082":
                    return {"workers": [], "totals": {}, "load": {"mem": 0, "mem_total": 4*2**30, "cpu": 0, "ncpu": 2, "max_devices": 15000}, "logs": []}
                raise RuntimeError("connection refused")
            app.peer_call = fake_peer_call
            ms = app.merged_status()
            machines = ms.get("machines", [])
            self.assertEqual(len(machines), 4)
            self.assertEqual(machines[0]["name"], "this machine")
            self.assertTrue(machines[0]["ok"])
            self.assertEqual(machines[1]["name"], "http://peer1:8082")
            self.assertEqual(machines[1]["capacity"], 20000)
            self.assertEqual(machines[1]["capacity_plain"], 35000)
            self.assertTrue(machines[1]["ok"])
            self.assertEqual(machines[2]["name"], "http://peer2:8082")
            self.assertEqual(machines[2]["capacity"], 15000)
            self.assertTrue(machines[2]["ok"])
            self.assertEqual(machines[3]["name"], "http://peer3:8082")
            self.assertEqual(machines[3]["capacity"], 0)
            self.assertEqual(machines[3]["capacity_plain"], 0)
            self.assertFalse(machines[3]["ok"])
        finally:
            app.workers, app.PEERS, app.peer_call = saved_workers, saved_peers, saved_peer_call
            app.mem_total, app.docker_os = saved_mem, saved_os

    def test_machine_shares(self):
        saved_merged = app.merged_status
        try:
            fake_status = {
                "machines": [
                    {"name": "this machine", "capacity": 10000, "capacity_plain": 20000, "ok": True},
                    {"name": "http://peer1:8082", "capacity": 20000, "capacity_plain": 40000, "ok": True},
                ]
            }
            app.merged_status = lambda: fake_status
            cfg_tls = app.parse_cfg({**BASE, "tls": True})
            shares_tls = app.machine_shares(cfg_tls, 30000)
            self.assertEqual(shares_tls, [(10000, 0), (20000, 10000)])

            cfg_plain = app.parse_cfg({**BASE, "tls": False})
            shares_plain = app.machine_shares(cfg_plain, 30000)
            self.assertEqual(shares_plain, [(10000, 0), (20000, 10000)])

            saved_peers = app.PEERS
            try:
                app.PEERS = ["http://peer1:8082"]
                cfg_manual = app.parse_cfg({**BASE, "weights": [3, 1]})
                shares_manual = app.machine_shares(cfg_manual, 40000)
                self.assertEqual(shares_manual, [(30000, 0), (10000, 30000)])
            finally:
                app.PEERS = saved_peers

            fake_status_broken = {
                "machines": [
                    {"name": "this machine", "capacity": 10000, "capacity_plain": 20000, "ok": True},
                    {"name": "http://peer1:8082", "capacity": 0, "capacity_plain": 0, "ok": False},
                ]
            }
            app.merged_status = lambda: fake_status_broken
            with self.assertRaises(app.BadRequest) as ctx:
                app.machine_shares(cfg_tls, 30000)
            self.assertIn("http://peer1:8082", str(ctx.exception))

            saved_peers = app.PEERS
            try:
                app.PEERS = ["http://peer1:8082"]
                cfg_broken_manual = app.parse_cfg({**BASE, "weights": [1, 1]})
                self.assertEqual(app.machine_shares(cfg_broken_manual, 1000), [(500, 0), (500, 500)])
            finally:
                app.PEERS = saved_peers

            fake_status_zero = {
                "machines": [
                    {"name": "this machine", "capacity": 0, "capacity_plain": 0, "ok": True},
                    {"name": "http://peer1:8082", "capacity": 0, "capacity_plain": 0, "ok": True},
                ]
            }
            app.merged_status = lambda: fake_status_zero
            self.assertEqual(app.machine_shares(cfg_tls, 10000), [(5000, 0), (5000, 5000)])
        finally:
            app.merged_status = saved_merged

    def test_deploy_uses_machine_shares_and_skips_zero_share(self):
        saved_peer_call, saved_launch, saved_peers, saved_shares = app.peer_call, app.launch, app.PEERS, getattr(app, "machine_shares", None)
        peer_calls, launches = [], []
        try:
            app.peer_call = lambda peer, path, body=None: peer_calls.append((peer, path, body))
            app.launch = lambda cfg, total, offset, subscribers, labels: launches.append((cfg, total, offset, subscribers, labels))
            app.PEERS = ["http://peer1:8082", "http://peer2:8082"]

            cfg = app.parse_cfg({**BASE, "weights": [2, 1, 0], "rate": 300})
            run = app.Run(cfg)
            run.deploy(3000, None)

            self.assertEqual(len(launches), 1)
            self.assertEqual(launches[0][1], 2000)
            self.assertEqual(launches[0][0]["rate"], 200)

            start_calls = [(peer, path, body) for peer, path, body in peer_calls if path == "/api/start"]
            self.assertEqual(len(start_calls), 1)
            self.assertEqual(start_calls[0][0], "http://peer1:8082")
            self.assertEqual(start_calls[0][2]["total"], 1000)
            self.assertEqual(start_calls[0][2]["rate"], 100)
            controller_peers = app.PEERS
            app.PEERS = []
            try:
                self.assertEqual(app.parse_cfg(start_calls[0][2])["total"], 1000)
            finally:
                app.PEERS = controller_peers

            peer_calls.clear()
            app.teardown()
            stop_calls = [(peer, path, body) for peer, path, body in peer_calls if path == "/api/stop"]
            self.assertEqual(len(stop_calls), 2)
            self.assertEqual({c[0] for c in stop_calls}, {"http://peer1:8082", "http://peer2:8082"})

            launches.clear()
            peer_calls.clear()
            cfg_zero_local = app.parse_cfg({**BASE, "weights": [0, 2, 1], "rate": 300})
            run_zl = app.Run(cfg_zero_local)
            run_zl.deploy(3000, None)
            self.assertEqual(len(launches), 0)
            start_calls2 = [(peer, path, body) for peer, path, body in peer_calls if path == "/api/start"]
            self.assertEqual(len(start_calls2), 2)
        finally:
            app.peer_call, app.launch, app.PEERS = saved_peer_call, saved_launch, saved_peers
            if saved_shares is not None:
                app.machine_shares = saved_shares

    def test_start_preflights_weighted_local_share(self):
        saved_preflight, saved_shares, saved_stop, saved_run, saved_current = app.preflight, getattr(app, "machine_shares", None), app.stop, app.Run, app.current
        preflight_calls = []
        try:
            app.preflight = lambda total, subscribers=0, tls=True: preflight_calls.append((total, subscribers, tls))
            app.stop = lambda: None
            class DummyThread:
                def start(self):
                    pass
                def is_alive(self):
                    return False
            class DummyRun:
                def __init__(self, cfg):
                    self.record = {"id": "test"}
                    self.thread = DummyThread()
                def save(self):
                    pass
            app.Run = DummyRun
            app.machine_shares = lambda cfg, n: [(12345, 0), (67890, 12345)]
            cfg = app.parse_cfg({**BASE, "total": 80235, "steps": [80235]})
            app.start(cfg)
            self.assertEqual(len(preflight_calls), 1)
            self.assertEqual(preflight_calls[0][0], 12345)
        finally:
            app.preflight, app.stop, app.Run, app.current = saved_preflight, saved_stop, saved_run, saved_current
            if saved_shares is not None:
                app.machine_shares = saved_shares

    def test_mcp_start_load_test_machine_weights(self):
        import inspect
        import mcp_server
        sig = inspect.signature(mcp_server.start_load_test)
        self.assertIn("machine_weights", sig.parameters)
        self.assertIsNone(sig.parameters["machine_weights"].default)
        self.assertIn("machine_weights", mcp_server.start_load_test.__doc__)

        started = []
        saved_start, saved_current = app.start, app.current
        try:
            class DummyRun:
                record = {"id": "dummy"}
            app.start = lambda c: started.append(c)
            app.current = DummyRun()
            saved_peers = app.PEERS
            try:
                app.PEERS = ["http://peer1:8082"]
                mcp_server.start_load_test("broker", 100, machine_weights=[3, 1])
                self.assertEqual(started[-1]["weights"], [3, 1])
            finally:
                app.PEERS = saved_peers
        finally:
            app.start, app.current = saved_start, saved_current


if __name__ == "__main__":
    unittest.main()
