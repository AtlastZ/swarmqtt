"""PDF report for one swarmqtt run (reportlab)."""
import io
import time

from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.shapes import Drawing, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

# validated categorical slots 1-2 (light mode) + neutral reference ink
BLUE, ORANGE = colors.HexColor("#2a78d6"), colors.HexColor("#eb6834")
INK, MUTED, GRID = colors.HexColor("#0b0b0b"), colors.HexColor("#52514e"), colors.HexColor("#e4e3df")
BRAND = colors.HexColor("#B45309")
MAX_POINTS = 400


def fmt_dur(s):
    s = int(s or 0)
    return f"{s // 60} min {s % 60} s" if s >= 60 else f"{s} s"


def gb(b):
    return f"{(b or 0) / 2**30:.1f} GB"


def headline(r):
    """One or two sentences: what held, where it broke and why."""
    steps, cfg = r["steps"], r["cfg"]
    if not steps:
        return "No load level completed" + (f": {r['reason']}." if r["reason"] else ".")
    good = [s for s in steps if s["ok"]]
    bad = next((s for s in steps if not s["ok"]), None)
    if cfg.get("steps"):
        text = f"Held {max(s['devices'] for s in good):,} devices." if good else "No ramp step held."
        if bad:
            text += f" At {bad['devices']:,} devices: {bad['reason']}."
        elif r["status"] == "done":
            text += f" Every step passed, so the limit is above {max(s['devices'] for s in steps):,}."
    else:
        s = steps[0]
        text = (f"{s['connected']:,} of {s['devices']:,} devices connected, {s['failed']:,} failed, "
                f"held for {fmt_dur(s['held_s'])}.")
        if bad and bad["reason"] and bad["reason"] != "stopped by user":
            text += f" {bad['reason'][0].upper()}{bad['reason'][1:]}."
    if r["status"] == "failed":
        text += f" The run ended with an error: {r['reason']}"
    elif r["status"] == "interrupted":
        text += " The web server restarted during the run, so later samples may be missing."
    return text


def setup_rows(r):
    c = r["cfg"]
    proto = f"MQTT v{ {'3': '3.1', '4': '3.1.1', '5': '5'}[c['version']] }"
    if c["tls"]:
        proto += f", TLS {c['tls_version'][4:] or '(auto)'}" + (", client certificate" if c.get("has_cert") else "")
    profile = {"pub": f"publish every {c['interval']:,} ms, "
                      + (f"custom payload ({len(c['payload'].encode()):,} bytes)" if c["payload"]
                         else f"{c['size']:,}-byte random payload") + f", QoS {c['qos']}",
               "sub": f"subscribe to {c['topic']}, QoS {c['qos']}", "conn": "connect only"}[c["mode"]]
    if c["steps"]:
        load = (f"ramp {' / '.join(f'{n:,}' for n in c['steps'])} devices, hold {c['hold']} s per step, "
                f"fail above {c['fail_pct']:g}% connect failures")
    else:
        load = f"{c['total']:,} devices, " + (f"stop after {fmt_dur(c['duration'])}" if c["duration"] else "until stopped")
    rows = [["Broker", f"{c['host']}:{c['port']}"], ["Protocol", proto],
            ["Devices", profile + (f", topic {c['topic']}" if c["mode"] == "pub" else "")], ["Load", load],
            ["Connection rate", f"{c['rate']:,} connections/s in total, keepalive {c['keepalive']} s"]]
    if c["subscribers"]:
        rows.append(["Latency subscribers", f"{c['subscribers']:,} on {c['sub_topic']}"])
    rows.append(["Load machines", f"{r['machines']} (this machine: {gb(r['mem_total'])} RAM, {r['ncpu']} CPUs for Docker)"])
    return rows


def downsample(rows):
    step = max(1, len(rows) // MAX_POINTS)
    return rows[::step]


def chart(title, rows, series, unit, width=170 * mm, height=55 * mm):
    """series: [(label, key, color, dashed, scale)]. Every series shares one y-axis."""
    d = Drawing(width, height + 12)
    d.add(String(0, height + 2, title, fontName="Helvetica-Bold", fontSize=9, fillColor=INK))
    p = LinePlot()
    p.x, p.y, p.width, p.height = 38, 26, width - 110, height - 34
    data = [[(r["t"] / 60, (r[key] or 0) * scale) for r in rows] for _, key, _, _, scale in series]
    p.data = data
    for i, (_, _, col, dashed, _) in enumerate(series):
        p.lines[i].strokeColor = col
        p.lines[i].strokeWidth = 1.5
        if dashed:
            p.lines[i].strokeDashArray = (3, 2)
    top = max((y for line in data for _, y in line), default=0)
    p.yValueAxis.valueMin, p.yValueAxis.valueMax = 0, top * 1.1 if top else 1
    p.xValueAxis.valueMin = 0
    for axis in (p.xValueAxis, p.yValueAxis):
        axis.labels.fontName, axis.labels.fontSize, axis.labels.fillColor, axis.strokeColor = "Helvetica", 7, MUTED, GRID
    p.yValueAxis.visibleGrid, p.yValueAxis.gridStrokeColor = True, GRID
    p.yValueAxis.labelTextFormat = lambda v: f"{v:,.0f}" if top >= 10 else f"{v:,.1f}"
    d.add(p)
    d.add(String(p.x + p.width / 2, 2, "minutes since start", fontName="Helvetica", fontSize=7, fillColor=MUTED, textAnchor="middle"))
    d.add(String(0, height - 8, unit, fontName="Helvetica", fontSize=7, fillColor=MUTED))
    # direct labels at each line's end (text stays in ink; the swatch carries the colour)
    used = []
    for i, (label, *_rest) in enumerate(series):
        if not data[i]:
            continue
        y = p.y + p.height * (data[i][-1][1] / p.yValueAxis.valueMax) - 3
        while any(abs(y - u) < 9 for u in used):  # nudge labels apart when lines end close together
            y -= 9
        used.append(y)
        d.add(String(p.x + p.width + 6, y, "—", fontName="Helvetica", fontSize=8, fillColor=series[i][2]))
        d.add(String(p.x + p.width + 16, y, label, fontName="Helvetica", fontSize=7, fillColor=INK))
    return d


def build(r):
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm, topMargin=18 * mm,
                            bottomMargin=18 * mm, title=f"swarmqtt report {r['id']}", author="swarmqtt")
    ss = getSampleStyleSheet()
    h1, h2, body, small = ss["Title"], ss["Heading3"], ss["BodyText"], ss["BodyText"].clone("small", fontSize=8,
                                                                                              textColor=MUTED)
    h1.alignment, h1.textColor = 0, INK
    c = r["cfg"]
    when = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["started"]))
    ran = fmt_dur((r.get("ended") or time.time()) - r["started"])
    story = [Paragraph('swar<font color="#B45309">mqtt</font> load test report', h1),
             Paragraph(f"{c['host']}:{c['port']} &middot; {when} &middot; ran {ran} &middot; run {r['id']} "
                       f"&middot; {r['status']}", small), Spacer(1, 6),
             Paragraph("Result", h2), Paragraph(headline(r), body), Paragraph("Setup", h2)]
    grid = TableStyle([("FONTSIZE", (0, 0), (-1, -1), 8), ("TEXTCOLOR", (0, 0), (-1, -1), INK),
                       ("LINEBELOW", (0, 0), (-1, -1), 0.25, GRID), ("VALIGN", (0, 0), (-1, -1), "TOP")])
    t = Table([[k, Paragraph(v, small.clone("cell", textColor=INK))] for k, v in setup_rows(r)],
              colWidths=[38 * mm, 132 * mm])
    t.setStyle(grid)
    story.append(t)

    if r["steps"]:
        story.append(Paragraph("Steps", h2))
        head = ["Devices", "Connected", "Failed", "Held", "Delivered/s", "Latency", "Peak RAM", "Result"]
        rows = [head] + [[f"{s['devices']:,}", f"{s['connected']:,}", f"{s['failed']:,}", fmt_dur(s["held_s"]),
                          f"{s['recv_rate']:,.0f}" if c["subscribers"] else "-",
                          f"{s['latency_ms']:g} ms" if s["latency_ms"] is not None else "-", gb(s["peak_mem"]),
                          "pass" if s["ok"] else Paragraph(f"<b>fail</b>: {s['reason']}", small.clone("c", textColor=INK))]
                         for s in r["steps"]]
        t = Table(rows, colWidths=[18 * mm, 19 * mm, 14 * mm, 18 * mm, 17 * mm, 15 * mm, 17 * mm, 52 * mm],
                  repeatRows=1)
        t.setStyle(TableStyle(grid.getCommands() + [("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                                                    ("TEXTCOLOR", (0, 0), (-1, 0), MUTED),
                                                    ("ALIGN", (0, 0), (6, -1), "RIGHT")]))
        story.append(t)

    rows = downsample(r["samples"])
    if len(rows) >= 2:
        story.append(Paragraph("Over time", h2))
        story.append(KeepTogether(chart("Devices connected", rows, [
            ("connected", "connected", BLUE, False, 1), ("target", "devices", MUTED, True, 1)], "devices")))
        msg = [("published*", "pub_rate", BLUE, False, 1)]
        if c["subscribers"]:
            msg.append(("delivered", "recv_rate", ORANGE, False, 1))
        story.append(KeepTogether(chart("Messages per second", rows, msg, "msg/s")))
        if any(x["latency_ms"] is not None for x in rows):
            story.append(KeepTogether(chart("Publish-to-receive latency (average)", rows,
                                            [("latency", "latency_ms", BLUE, False, 1)], "ms")))
        story.append(KeepTogether(chart("Load machine RAM", rows, [
            ("used", "mem", BLUE, False, 2**-30), ("available", "mem_total", MUTED, True, 2**-30)], "GB")))

    story += [Paragraph("Caveats", h2)] + [Paragraph(f"&bull; {x}", small) for x in [
        "* emqtt-bench counts each publish about twice: the published rate is roughly 2x what the broker "
        "receives. Use the delivered rate (what each latency subscriber received) or broker metrics for throughput.",
        "Connected is cumulative successful connects reported by emqtt-bench; drops after connecting are not "
        "subtracted. Check the broker's own connection count.",
        "The broker certificate is not verified during the load test (an emqtt-bench limitation).",
        "If load machine RAM or CPU reached its limit, failures at that point say more about the load "
        "generator than about the broker.",
    ] + (["Latency subscribers add an 8-byte binary timestamp to the front of every payload."]
         if c["subscribers"] else [])]
    doc.build(story)
    return buf.getvalue()
