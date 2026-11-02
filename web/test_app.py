import os
import tempfile
import unittest

import app

BASE = {"host": "broker", "port": 8883, "total": 100000, "rate": 2000, "keepalive": 60,
        "interval": 10000, "size": 256, "qos": 0, "tls": True, "mode": "pub"}


class AppTest(unittest.TestCase):
    def test_plan_covers_total_without_overlap(self):
        p = app.plan(100001, 2000)
        self.assertEqual(sum(c for c, _, _ in p), 100001)
        self.assertEqual([s for _, s, _ in p], [0, 25000, 50000, 75000, 100000])
        self.assertTrue(all(c <= app.PER_WORKER for c, _, _ in p))

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

    def test_estimate_mem_and_refuse_when_too_big(self):
        self.assertEqual(app.estimate_mem(100000), 4 * app.WORKER_MEM + 100000 * app.CONN_MEM)
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
                with self.assertRaisesRegex(app.BadRequest, "source ports"):
                    app.preflight(app.MAC_MAX_DEVICES + 1)
            app.docker_os = "Ubuntu 24.04.3 LTS"
            self.assertIsNone(app.max_devices())
            app.preflight(app.MAC_MAX_DEVICES + 1)
        finally:
            app.docker_os = ""

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

    def test_parse_cfg_rejects_bad_input(self):
        for bad in ({"host": "a b"}, {"port": 0}, {"total": 0}, {"mode": "x"}, {"cert": "C"}, {"payload": "x" * (app.MAX_PAYLOAD + 1)},
                    {"duration": -1}):
            with self.assertRaises(app.BadRequest):
                app.parse_cfg({**BASE, **bad})

    def test_mcp_cfg_maps_args_and_rejects_bad_input(self):
        import mcp_server
        cfg = mcp_server._cfg(**{**BASE, "cert": "C", "key": "K"})
        self.assertEqual((cfg["cert"], cfg["total"]), ("C", 100000))
        with self.assertRaises(mcp_server.ToolError):
            mcp_server._cfg(**{**BASE, "host": "a b"})


if __name__ == "__main__":
    unittest.main()
