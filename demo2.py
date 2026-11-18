#!/usr/bin/env python3
"""Demo 2 on localhost (no Mininet/root needed): same scenario as topology.py.

    pub1->news  pub2->alerts  pub3->news
    sub1: news+alerts   sub2: news   sub3: alerts   (sub2 later also subscribes to alerts)

Prints the broker's pub/sub table and statistics at the end.
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def run(script, *args, **kw):
    return subprocess.Popen([PY, os.path.join(HERE, script), *map(str, args)], cwd=HERE, **kw)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--interval", type=float, default=0.3)
    a = ap.parse_args()
    p = str(a.port)
    out = os.path.join(HERE, "results", "local")      # keeps Mininet results/ untouched
    os.makedirs(out, exist_ok=True)

    broker = run("broker.py", "--port", p, "--host", "127.0.0.1")
    time.sleep(1)
    dur = a.count * a.interval + 3
    subs = []
    for i, topics in enumerate((["news", "alerts"], ["news"], ["alerts"]), 1):
        subs.append(run("subscriber.py", "--port", p, "--topics", *topics, "--duration", dur,
                        "--name", f"sub{i}", "--quiet", "--log", f"results/local/sub{i}.csv",
                        stdin=subprocess.PIPE))
    time.sleep(1)
    pubs = [run("publisher.py", "--port", p, "--topic", t, "--count", a.count,
                "--interval", a.interval, "--name", f"pub{i}")
            for i, t in enumerate(("news", "alerts", "news"), 1)]
    for x in pubs:
        x.wait()
    print("\n--- live snapshot while subscribers are still connected ---")
    subprocess.run([PY, os.path.join(HERE, "stats.py"), "table", "--port", p], cwd=HERE)
    for x in subs:
        x.wait()
    print("\n--- final snapshot (after subscribers sent their latency reports) ---")
    subprocess.run([PY, os.path.join(HERE, "stats.py"), "--port", p], cwd=HERE)
    with open(os.path.join(out, "stats.json"), "w") as f:
        subprocess.run([PY, os.path.join(HERE, "stats.py"), "--port", p, "--json"], cwd=HERE, stdout=f)
    broker.terminate()
    broker.wait()
    print("\n--- plots ---")
    subprocess.run([PY, os.path.join(HERE, "plots.py"), "--results", out], cwd=HERE)
