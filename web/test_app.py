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
