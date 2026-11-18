#!/usr/bin/env python3
r"""
D1 topology + automated end-to-end test for the Pub-Sub / SDN project.

    pub1 pub2 pub3            sub1 sub2 sub3
        \  |  /                   \  |  /
          s1 ------- s2 ------- s3
                      |
                    broker

Run (controller must already be running in another terminal):
    sudo python3 topology.py            # automated test, then exit
    sudo python3 topology.py --cli      # automated test, then Mininet CLI
"""
import argparse
import os
import time
from functools import partial

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
BROKER_IP = "10.0.0.100"
PORT = 5000


def build():
    net = Mininet(controller=None,
                  switch=partial(OVSKernelSwitch, protocols="OpenFlow13"),
                  link=TCLink, autoSetMacs=True)
    net.addController("c0", controller=RemoteController,
                      ip="127.0.0.1", port=6653)

    s1 = net.addSwitch("s1")
    s2 = net.addSwitch("s2")
    s3 = net.addSwitch("s3")
    broker = net.addHost("broker", ip=BROKER_IP + "/24")
    pubs = []
    subs = []
    for i in (1, 2, 3):
        pubs.append(net.addHost("pub%d" % i, ip="10.0.0.%d/24" % (10 + i)))
        subs.append(net.addHost("sub%d" % i, ip="10.0.0.%d/24" % (20 + i)))

    for p in pubs:
        net.addLink(p, s1, bw=10)
    net.addLink(s1, s2, bw=10)
    net.addLink(broker, s2, bw=10)
    net.addLink(s2, s3, bw=10)
    for s in subs:
        net.addLink(s, s3, bw=10)
    return net


def run_test(net):
    os.makedirs(RESULTS, exist_ok=True)
    broker = net.get("broker")
    pubs = [net.get("pub%d" % i) for i in (1, 2, 3)]
    subs = [net.get("sub%d" % i) for i in (1, 2, 3)]

    print("\n=== Connectivity check ===")
    net.pingAll()

    print("\n=== Starting broker ===")
    broker.cmd("python3 %s/broker.py --port %d > /tmp/broker.log 2>&1 &" % (HERE, PORT))
    time.sleep(1)

    sub_topics = [(subs[0], "news alerts"), (subs[1], "news"), (subs[2], "alerts")]
    print("=== Starting subscribers ===")
    for i, (h, topics) in enumerate(sub_topics, 1):
        h.cmd("python3 %s/subscriber.py --broker %s --port %d --topics %s "
              "--duration 10 --name sub%d --log %s/sub%d.csv > /tmp/sub%d.out 2>&1 &"
              % (HERE, BROKER_IP, PORT, topics, i, RESULTS, i, i))
    time.sleep(2)

    print("=== Starting publishers ===")
    pub_topics = [(pubs[0], "news"), (pubs[1], "alerts"), (pubs[2], "news")]
    for i, (h, topic) in enumerate(pub_topics, 1):
        h.cmd("python3 %s/publisher.py --broker %s --port %d --topic %s "
              "--count 10 --interval 0.3 --name pub%d > /tmp/pub%d.out 2>&1 &"
              % (HERE, BROKER_IP, PORT, topic, i, i))

    print("=== Waiting for test to finish (~10 s) ===")
    time.sleep(11)

    print("\n=== RESULTS ===")
    for i in (1, 2, 3):
        print("--- sub%d output (last lines) ---" % i)
        with open("/tmp/sub%d.out" % i) as f:
            print("".join(f.readlines()[-6:]))
    print("Expected: sub1 gets 30 msgs (news 20 + alerts 10), "
          "sub2 gets 20 (news), sub3 gets 10 (alerts).")
    print("Latency CSVs saved in %s/" % RESULTS)
    print("\n=== Demo 2: broker pub/sub table + statistics ===")
    print(pubs[0].cmd("python3 %s/stats.py --broker %s --port %d" % (HERE, BROKER_IP, PORT)))
    # saved for plots.py (python3 plots.py -> results/report.html)
    pubs[0].cmd("python3 %s/stats.py --broker %s --port %d --json > %s/stats.json"
                % (HERE, BROKER_IP, PORT, RESULTS))
    print("\n=== Flow table on s1 (what the controller installed) ===")
    print(net.get("s1").cmd("ovs-ofctl -O OpenFlow13 dump-flows s1"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cli", action="store_true", help="open Mininet CLI after test")
    a = ap.parse_args()
    setLogLevel("warning")
    net = build()
    net.start()
    try:
        run_test(net)
        if a.cli:
            CLI(net)
    finally:
        net.get("broker").cmd("pkill -f broker.py")
        net.stop()
