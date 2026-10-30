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
        self.assertLess(app.estimate_mem(100000, tls=False), app.estimate_mem(100000))
        app.mem_total = 4 * 2**30
        try:
            with self.assertRaises(app.BadRequest):
                app.start(app.parse_cfg(BASE))  # 100k devices ~13 GB, refused before touching Docker
        finally:
            app.mem_total = 0

    def test_duration_defaults_to_unlimited(self):
        self.assertEqual(app.parse_cfg(BASE)["duration"], 0)
        self.assertEqual(app.parse_cfg({**BASE, "duration": "600"})["duration"], 600)

    def test_ramp_steps_are_sorted_and_set_total(self):
        cfg = app.parse_cfg({**BASE, "steps": "1000, 50000,1000"})
        self.assertEqual((cfg["steps"], cfg["total"]), ([1000, 50000], 50000))

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


if __name__ == "__main__":
    unittest.main()
