#!/usr/bin/env python3
"""Build one interactive HTML report with all plots.

Inputs (default dir: results/):
  *.csv        subscriber latency logs (recv_time,topic,latency_ms), one file per subscriber
  stats.json   broker snapshot saved by topology.py / demo2.py   (optional but recommended)

    python3 plots.py                              # results/ -> results/report.html
    python3 plots.py --results results/local --open
    python3 plots.py --sweep 1x1,2x10,5x50,10x100 # also run a local scalability sweep

Charts use Chart.js from a CDN, so the page needs internet access when opened.
"""
import argparse
import csv
import glob
import json
import logging
import os
import socket
import sys
import threading
import time
import webbrowser

HERE = os.path.dirname(os.path.abspath(__file__))
MAX_POINTS = 1500


def pct(v, p):
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def stat(vals):
    v = sorted(vals)
    return {"n": len(v), "min": v[0], "mean": sum(v) / len(v), "p50": pct(v, 50),
            "p95": pct(v, 95), "p99": pct(v, 99), "max": v[-1]}


# ---- inputs ------------------------------------------------------------------
def load_csvs(d):
    """-> {sub: {topic: [(recv_time, latency_ms)]}}"""
    out = {}
    for path in sorted(glob.glob(os.path.join(d, "*.csv"))):
        sub = os.path.splitext(os.path.basename(path))[0]
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                try:
                    out.setdefault(sub, {}).setdefault(row["topic"], []).append(
                        (float(row["recv_time"]), float(row["latency_ms"])))
                except (KeyError, ValueError, TypeError):
                    continue                       # not one of our latency logs
    return out


def build(d, sweep):
    data = load_csvs(d)
    if not data:
        sys.exit(f"no latency CSVs found in {d} (run topology.py or demo2.py first)")
    t0 = min(t for tp in data.values() for pts in tp.values() for t, _ in pts)
    series, stats, cdf, edges, by_topic = [], [], [], [], {}
    for sub in sorted(data):
        for topic in sorted(data[sub]):
            pts = sorted(data[sub][topic])
            lats = [l for _, l in pts]
            stride = max(1, len(pts) // MAX_POINTS)
            series.append({"sub": sub, "topic": topic,
                           "pts": [[round(t - t0, 4), round(l, 3)] for t, l in pts[::stride]]})
            stats.append(dict(sub=sub, topic=topic, **stat(lats)))
            sl = sorted(lats)
            cdf.append({"sub": sub, "topic": topic,
                        "pts": [[round(pct(sl, p), 3), p] for p in range(0, 101, 2)]})
            edges.append({"sub": sub, "topic": topic, "n": len(lats)})
            by_topic.setdefault(topic, []).extend(lats)
    top = max(l for v in by_topic.values() for l in v) or 1.0
    bins = 20
    w = top / bins
    hist = {"labels": [f"{i * w:.2f}" for i in range(bins)], "topics": {}}
    for topic, v in sorted(by_topic.items()):
        counts = [0] * bins
        for l in v:
            counts[min(bins - 1, int(l / w))] += 1
        hist["topics"][topic] = counts
    allv = [l for v in by_topic.values() for l in v]
    snap = None
    sp = os.path.join(d, "stats.json")
    if os.path.exists(sp):
        with open(sp) as f:
            snap = json.load(f)
    return {"series": series, "stats": stats, "cdf": cdf, "hist": hist, "edges": edges,
            "overall": stat(allv), "snap": snap, "sweep": sweep,
            "meta": {"dir": d, "generated": time.strftime("%Y-%m-%d %H:%M:%S")}}


# ---- optional scalability sweep (local, in-process broker) --------------------
def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def sweep_once(npub, nsub, msgs, rate, size):
    sys.path.insert(0, HERE)
    import broker
    logging.getLogger().setLevel(logging.WARNING)
    port = free_port()
    threading.Thread(target=broker.Broker("127.0.0.1", port).serve, daemon=True).start()
    time.sleep(0.3)
    lats, socks = [], []

    def reader(sock):
        for line in sock.makefile("r", encoding="utf-8", newline="\n"):
            p = line.split("|", 3)
            if p[0] == "MSG":
                lats.append((time.time() - float(p[2])) * 1000)

    for i in range(nsub):
        s = socket.create_connection(("127.0.0.1", port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.sendall(f"HELLO|sub|s{i}\nSUBSCRIBE|t{i % npub}\n".encode())
        threading.Thread(target=reader, args=(s,), daemon=True).start()
        socks.append(s)
    time.sleep(0.3)
    payload = "x" * size

    def pub(i):
        s = socket.create_connection(("127.0.0.1", port))
        s.sendall(f"HELLO|pub|p{i}\n".encode())
        nxt = time.perf_counter()
        for k in range(msgs):
            s.sendall(f"PUBLISH|t{i}|{time.time():.6f}|{payload}\n".encode())
            nxt += 1 / rate
            time.sleep(max(0, nxt - time.perf_counter()))
        time.sleep(0.2)
        s.close()

    expected = msgs * nsub
    t0 = time.perf_counter()
    ths = [threading.Thread(target=pub, args=(i,)) for i in range(npub)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    end = time.time() + 5
    while len(lats) < expected and time.time() < end:
        time.sleep(0.05)
    el = time.perf_counter() - t0
    for s in socks:
        s.close()
    st = stat(lats) if lats else dict(mean=0, p50=0, p95=0, p99=0)
    return {"pubs": npub, "subs": nsub, "sent": npub * msgs, "delivered": len(lats),
            "tput": len(lats) / el, "mean": st["mean"], "p50": st["p50"], "p95": st["p95"],
            "p99": st["p99"], "loss": 100 * (1 - len(lats) / max(expected, 1))}


def run_sweep(spec, msgs, rate, size):
    rows = []
    for pair in spec.split(","):
        npub, nsub = map(int, pair.lower().split("x"))
        print(f"sweep {npub} pubs x {nsub} subs ...", flush=True)
        rows.append(sweep_once(npub, nsub, msgs, rate, size))
    return rows


# ---- HTML ----------------------------------------------------------------------
TEMPLATE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pub/Sub Demo 2 report</title>
<style>
:root{--bg:#f7f7f5;--card:#fff;--fg:#1f2328;--muted:#656d76;--grid:#e3e5e8;--border:#d8dbdf}
@media (prefers-color-scheme:dark){:root{--bg:#14171a;--card:#1c2024;--fg:#e6e8ea;--muted:#9aa2aa;--grid:#2d3238;--border:#2d3238}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,-apple-system,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:22px;margin:0 0 2px} h2{font-size:17px;margin:34px 0 2px}
.cap{color:var(--muted);margin:0 0 12px;font-size:13px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:18px 0 4px}
.card,.box{background:var(--card);border:1px solid var(--border);border-radius:10px}
.card{padding:12px 14px} .k{font-size:12px;color:var(--muted)} .v{font-size:22px;font-weight:600}
.box{padding:14px;overflow-x:auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:14px}
.box canvas{max-height:340px}
table{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
th,td{padding:5px 8px;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap}
th:first-child,td:first-child,th.l,td.l{text-align:left} th{color:var(--muted);font-weight:500}
svg text{fill:var(--fg);font-size:13px} svg .m{fill:var(--muted);font-size:11px}
[hidden]{display:none!important}
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
</head><body><main>
<h1>Pub/Sub Demo 2 report</h1>
<div class="cap" id="meta"></div>
<div class="cards" id="cards"></div>

<h2>Latency</h2>
<p class="cap">One-way latency measured by each subscriber (publisher timestamp to receive time).</p>
<div class="grid">
  <div class="box"><canvas id="c-time"></canvas></div>
  <div class="box"><canvas id="c-cdf"></canvas></div>
  <div class="box"><canvas id="c-hist"></canvas></div>
  <div class="box"><canvas id="c-pct"></canvas></div>
</div>
<div class="box" style="margin-top:14px"><table id="t-lat"></table></div>

<section id="s-traffic">
<h2>Broker traffic</h2>
<p class="cap" id="cap-traffic">From the broker's statistics snapshot.</p>
<div class="grid">
  <div class="box"><canvas id="c-topic"></canvas></div>
  <div class="box"><canvas id="c-client"></canvas></div>
  <div class="box"><canvas id="c-bytes"></canvas></div>
</div>
<div class="box" style="margin-top:14px"><table id="t-topic"></table></div>
</section>

<h2>Pub/sub table</h2>
<p class="cap">Publishers, topics and subscribers. Line width = messages delivered to that subscriber.</p>
<div class="box"><svg id="graph" width="100%"></svg></div>

<section id="s-sweep" hidden>
<h2>Scalability sweep</h2>
<p class="cap">Local run: one topic per publisher, each subscriber listens to one topic. No simulated network delay.</p>
<div class="grid">
  <div class="box"><canvas id="c-sw-lat"></canvas></div>
  <div class="box"><canvas id="c-sw-tp"></canvas></div>
</div>
<div class="box" style="margin-top:14px"><table id="t-sweep"></table></div>
</section>

<script>
const D = __DATA__;
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const PAL = ["#2f81f7","#f0883e","#3fb950","#a371f7","#d29922","#1fb6c1","#e5534b","#8b949e"];
const col = i => PAL[i % PAL.length];
const $ = id => document.getElementById(id);
const f2 = (v, d = 2) => v == null ? "–" : Number(v).toFixed(d);
const ax = (t, o = {}) => Object.assign({title: {display: true, text: t}}, o);
const mk = (id, cfg) => new Chart($(id), cfg);
const row = (cells, tag = "td") => "<tr>" + cells.map((c, i) => `<${tag}${i === 0 ? ' class="l"' : ""}>${c}</${tag}>`).join("") + "</tr>";
const title = t => ({title: {display: true, text: t}});
if (window.Chart) {
  Chart.defaults.color = css("--muted"); Chart.defaults.borderColor = css("--grid");
  Chart.defaults.font.family = "system-ui,sans-serif";
}

// summary cards
const snap = D.snap, o = D.overall;
const subs = [...new Set(D.stats.map(s => s.sub))], topics = [...new Set(D.stats.map(s => s.topic))];
const cards = [["Published", snap ? snap.broker.published : "–"],
  ["Delivered", snap ? snap.broker.delivered : D.stats.reduce((a, s) => a + s.n, 0)],
  ["Mean latency", f2(o.mean) + " ms"], ["p95 latency", f2(o.p95) + " ms"], ["Max latency", f2(o.max) + " ms"],
  ["Subscribers", subs.length], ["Topics", topics.length]];
$("cards").innerHTML = cards.map(c => `<div class="card"><div class="k">${c[0]}</div><div class="v">${c[1]}</div></div>`).join("");
$("meta").textContent = `source: ${D.meta.dir} · generated ${D.meta.generated}` + (snap ? "" : " · no stats.json found: broker charts limited to subscriber data");

if (window.Chart) {
  // latency over time
  mk("c-time", {type: "scatter", data: {datasets: D.series.map((s, i) => ({
      label: s.sub + " · " + s.topic, data: s.pts.map(p => ({x: p[0], y: p[1]})), borderColor: col(i),
      backgroundColor: col(i), pointRadius: 2.5, showLine: true, borderWidth: 1}))},
    options: {scales: {x: ax("seconds since first message"), y: ax("latency (ms)", {beginAtZero: true})},
      plugins: {legend: {position: "bottom"}, ...title("Latency over time")}}});
  // CDF
  mk("c-cdf", {type: "scatter", data: {datasets: D.cdf.map((s, i) => ({
      label: s.sub + " · " + s.topic, data: s.pts.map(p => ({x: p[0], y: p[1]})), borderColor: col(i),
      backgroundColor: col(i), pointRadius: 0, showLine: true, borderWidth: 2}))},
    options: {scales: {x: ax("latency (ms)", {beginAtZero: true}), y: ax("% of messages ≤ x", {min: 0, max: 100})},
      plugins: {legend: {position: "bottom"}, ...title("Latency CDF")}}});
  // histogram
  mk("c-hist", {type: "bar", data: {labels: D.hist.labels, datasets: Object.keys(D.hist.topics).map((t, i) => ({
      label: t, data: D.hist.topics[t], backgroundColor: col(i)}))},
    options: {scales: {x: ax("latency bin start (ms)"), y: ax("messages", {beginAtZero: true})},
      plugins: {legend: {position: "bottom"}, ...title("Latency distribution by topic")}}});
  // percentiles
  mk("c-pct", {type: "bar", data: {labels: D.stats.map(s => s.sub + " · " + s.topic),
      datasets: [["mean", "mean"], ["p50", "p50"], ["p95", "p95"], ["max", "max"]].map((k, i) => ({
        label: k[0], data: D.stats.map(s => s[k[1]]), backgroundColor: col(i)}))},
    options: {scales: {y: ax("latency (ms)", {beginAtZero: true})},
      plugins: {legend: {position: "bottom"}, ...title("Latency summary per subscriber and topic")}}});
}
$("t-lat").innerHTML = row(["Subscriber · topic", "N", "min", "mean", "p50", "p95", "p99", "max"], "th") +
  D.stats.map(s => row([s.sub + " · " + s.topic, s.n, f2(s.min), f2(s.mean), f2(s.p50), f2(s.p95), f2(s.p99), f2(s.max)])).join("");

// broker traffic
if (!snap) {
  $("cap-traffic").textContent = "No stats.json: showing deliveries counted from subscriber logs only.";
  const dl = {}; D.stats.forEach(s => dl[s.topic] = (dl[s.topic] || 0) + s.n);
  if (window.Chart) mk("c-topic", {type: "bar", data: {labels: Object.keys(dl), datasets: [{label: "delivered", data: Object.values(dl), backgroundColor: col(1)}]},
    options: {plugins: {...title("Deliveries per topic")}, scales: {y: ax("messages", {beginAtZero: true})}}});
  ["c-client", "c-bytes", "t-topic"].forEach(id => $(id).closest(".box").hidden = true);
} else if (window.Chart) {
  const T = snap.topics, C = snap.clients;
  mk("c-topic", {type: "bar", data: {labels: T.map(t => t.topic), datasets: [
      {label: "messages in", data: T.map(t => t.msgs), backgroundColor: col(0)},
      {label: "delivered (fan-out)", data: T.map(t => t.deliveries), backgroundColor: col(1)}]},
    options: {scales: {y: ax("messages", {beginAtZero: true})}, plugins: {legend: {position: "bottom"}, ...title("Messages in vs delivered, per topic")}}});
  mk("c-client", {type: "bar", data: {labels: C.map(c => c.name), datasets: [
      {label: "msgs in (published)", data: C.map(c => c.msgs_in), backgroundColor: col(0)},
      {label: "msgs out (received)", data: C.map(c => c.msgs_out), backgroundColor: col(2)}]},
    options: {scales: {y: ax("messages", {beginAtZero: true})}, plugins: {legend: {position: "bottom"}, ...title("Messages per client")}}});
  mk("c-bytes", {type: "bar", data: {labels: C.map(c => c.name), datasets: [
      {label: "bytes in", data: C.map(c => c.bytes_in), backgroundColor: col(0)},
      {label: "bytes out", data: C.map(c => c.bytes_out), backgroundColor: col(2)}]},
    options: {scales: {y: ax("bytes", {beginAtZero: true})}, plugins: {legend: {position: "bottom"}, ...title("Bytes per client")}}});
  $("t-topic").innerHTML = row(["Topic", "msgs", "bytes", "delivered", "msg/s", "lat n", "mean ms", "p95 ms", "max ms"], "th") +
    T.map(t => row([t.topic, t.msgs, t.bytes, t.deliveries, f2(t.rate, 1), t.latency ? t.latency.n : 0,
      f2(t.latency && t.latency.mean), f2(t.latency && t.latency.p95), f2(t.latency && t.latency.max)])).join("");
}

// pub/sub graph
(function () {
  const pubs = [], tps = [], sb = [];
  const add = (a, x) => { if (!a.includes(x)) a.push(x); };
  if (snap) snap.topics.forEach(t => { add(tps, t.topic); t.publishers.forEach(p => add(pubs, p)); });
  D.edges.forEach(e => { add(tps, e.topic); add(sb, e.sub); });
  if (snap) snap.topics.forEach(t => t.subscribers.forEach(s => add(sb, s)));
  const rows = Math.max(pubs.length, tps.length, sb.length, 1), H = rows * 54 + 30, W = 900;
  const X = [120, 450, 780], y = (i, n) => 30 + (i + 0.5) * ((H - 30) / n) - 8;
  const maxn = Math.max(...D.edges.map(e => e.n), 1);
  let s = `<g>`;
  const tc = t => col(tps.indexOf(t));
  if (snap) snap.topics.forEach(t => t.publishers.forEach(p => {
    const y1 = y(pubs.indexOf(p), pubs.length), y2 = y(tps.indexOf(t.topic), tps.length);
    s += `<path d="M${X[0] + 50},${y1} C${X[0] + 170},${y1} ${X[1] - 170},${y2} ${X[1] - 50},${y2}" fill="none" stroke="${tc(t.topic)}" stroke-width="2" opacity=".6"/>`;
  }));
  D.edges.forEach(e => {
    const y1 = y(tps.indexOf(e.topic), tps.length), y2 = y(sb.indexOf(e.sub), sb.length), w = 1.5 + 8 * e.n / maxn;
    s += `<path d="M${X[1] + 50},${y1} C${X[1] + 170},${y1} ${X[2] - 170},${y2} ${X[2] - 50},${y2}" fill="none" stroke="${tc(e.topic)}" stroke-width="${w}" opacity=".6"/>`;
    s += `<text class="m" x="${X[2] - 54}" y="${y2 - 5}" text-anchor="end">${e.n}</text>`;
  });
  const node = (x, yy, label, sub) => `<rect x="${x - 50}" y="${yy - 16}" width="100" height="32" rx="8" fill="var(--card)" stroke="var(--border)"/>` +
    `<text x="${x}" y="${yy + 4}" text-anchor="middle">${label}</text>` + (sub ? `<text class="m" x="${x}" y="${yy + 28}" text-anchor="middle">${sub}</text>` : "");
  pubs.forEach((p, i) => s += node(X[0], y(i, pubs.length), p));
  tps.forEach((t, i) => { const st = snap && snap.topics.find(x => x.topic === t); s += node(X[1], y(i, tps.length), t, st ? st.msgs + " in" : ""); });
  sb.forEach((p, i) => s += node(X[2], y(i, sb.length), p));
  s += `<text class="m" x="${X[0]}" y="14" text-anchor="middle">PUBLISHERS</text><text class="m" x="${X[1]}" y="14" text-anchor="middle">TOPICS</text><text class="m" x="${X[2]}" y="14" text-anchor="middle">SUBSCRIBERS</text></g>`;
  const g = $("graph"); g.setAttribute("viewBox", `0 0 ${W} ${H}`); g.innerHTML = s;
  if (!snap) $("graph").insertAdjacentHTML("afterend", "");
})();

// sweep
if (D.sweep && D.sweep.length) {
  $("s-sweep").hidden = false;
  const lbl = D.sweep.map(r => r.pubs + "p × " + r.subs + "s");
  if (window.Chart) {
    mk("c-sw-lat", {type: "line", data: {labels: lbl, datasets: [["mean", "mean"], ["p50", "p50"], ["p95", "p95"], ["p99", "p99"]].map((k, i) => ({
        label: k[0], data: D.sweep.map(r => r[k[1]]), borderColor: col(i), backgroundColor: col(i), tension: .2}))},
      options: {scales: {x: ax("publishers × subscribers"), y: ax("latency (ms)", {beginAtZero: true})}, plugins: {legend: {position: "bottom"}, ...title("Latency vs load")}}});
    mk("c-sw-tp", {type: "line", data: {labels: lbl, datasets: [{label: "deliveries/s", data: D.sweep.map(r => r.tput), borderColor: col(2), backgroundColor: col(2), tension: .2}]},
      options: {scales: {x: ax("publishers × subscribers"), y: ax("deliveries per second", {beginAtZero: true})}, plugins: {...title("Throughput vs load")}}});
  }
  $("t-sweep").innerHTML = row(["Load", "sent", "delivered", "deliv/s", "mean ms", "p50", "p95", "p99", "loss %"], "th") +
    D.sweep.map((r, i) => row([lbl[i], r.sent, r.delivered, f2(r.tput, 0), f2(r.mean), f2(r.p50), f2(r.p95), f2(r.p99), f2(r.loss, 1)])).join("");
}
</script></main></body></html>
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", default=os.path.join(HERE, "results"), help="dir with *.csv and stats.json")
    ap.add_argument("--out", help="output HTML (default <results>/report.html)")
    ap.add_argument("--sweep", help="also run a local scalability sweep, e.g. 1x1,2x10,5x50,10x100")
    ap.add_argument("--msgs", type=int, default=100, help="messages per publisher in the sweep")
    ap.add_argument("--rate", type=float, default=50, help="msgs/s per publisher in the sweep")
    ap.add_argument("--size", type=int, default=100, help="payload bytes in the sweep")
    ap.add_argument("--open", action="store_true", help="open the report in a browser")
    a = ap.parse_args()
    sweep = run_sweep(a.sweep, a.msgs, a.rate, a.size) if a.sweep else None
    data = build(a.results, sweep)
    out = a.out or os.path.join(a.results, "report.html")
    html = TEMPLATE.replace("__DATA__", json.dumps(data).replace("</", "<\\/"))
    with open(out, "w") as f:
        f.write(html)
    print(f"wrote {out}  ({len(data['series'])} latency series, "
          f"stats.json {'found' if data['snap'] else 'missing'}"
          f"{', sweep included' if sweep else ''})")
    if a.open:
        webbrowser.open("file://" + os.path.abspath(out))


if __name__ == "__main__":
    main()
