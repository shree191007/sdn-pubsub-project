# SDN Pub/Sub project (base: github.com/saimahendra2007/sdn-pubsub-project)

Demo 1 (unchanged idea): Mininet topology, TCP broker, publishers, subscribers, latency CSVs.
Demo 2 (added): broker pub/sub table, cleaner output, statistics integrated in the broker.

## Demo 2 changes
- **Pub/sub table** (`broker.py`): per topic, which publishers have published and which
  subscribers are currently subscribed, plus messages in / delivered.
- **Statistics** (`broker.py`, `report.py`): broker counters (published, delivered,
  send failures, uptime), per topic (msgs, bytes, deliveries, msg/s) and per client
  (msgs/bytes in/out). Records survive disconnects (shown as `gone`).
- **Latency integration**: subscribers measure one-way latency and send `REPORT|<json>` to the
  broker every 2 s and on exit; the broker merges them into the topic/client tables.
  (Topic P95 = worst subscriber's P95; mean is message-weighted.)
- **Output tuning**: aligned tables; timestamped subscriber lines; `--quiet`; per-topic latency
  summary at subscriber exit; publisher prints msgs/bytes/rate; broker logs name-based
  connect/subscribe events.
- **Protocol additions** (backward compatible): `HELLO|role|name`, `REPORT|json`, `STATS|` -> `STATS|json`.

## Run
```
python3 broker.py --port 5000 [--table-interval 5]      # prints tables every 5 s and on Ctrl+C
python3 subscriber.py --topics news alerts --name sub1 [--quiet]   # type: sub <t> / unsub <t>
python3 publisher.py --topic news --count 10 --name pub1 [-v]
python3 stats.py [table|stats|all] [--watch 2] [--json]   # query a running broker
python3 demo2.py                                          # whole scenario on localhost, no root (also builds the report)
python3 plots.py [--results results] [--sweep 1x1,2x10,5x50] [--open]   # HTML report with all plots
sudo python3 topology.py                                  # Mininet version (needs Linux + controller)
```
