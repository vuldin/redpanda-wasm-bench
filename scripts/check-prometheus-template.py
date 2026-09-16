#!/usr/bin/env python3
"""Render prometheus.yml.j2 against a simulated inventory and assert its shape.

This is NOT what promtool does, which is why both exist. promtool checks that
the config is syntactically valid Prometheus. It has no opinion about whether
the `node` job covers every instance, or whether a target carries the labels
the dashboards select on. Drop a host group out of the template and promtool
passes happily while panels go quietly empty.

So this checks the invariants the dashboards actually depend on, offline and in
milliseconds, with no cluster:

  - every scrape job renders with the targets it should have
  - the node job covers every broker, every client and the monitoring host
  - every target carries redpanda_id, and every node target carries host+role
  - the two redpanda jobs hit different endpoints at different intervals

Usage: scripts/check-prometheus-template.py [broker_count] [client_count]
"""
import sys
from pathlib import Path

try:
    import jinja2
    import yaml
except ImportError as e:
    print(f"needs jinja2 and pyyaml ({e})")
    sys.exit(2)

ROOT = Path(__file__).resolve().parent.parent


def render(nbrokers, nclients, nmonitors=1):
    groups = {
        "broker": [f"b{i}" for i in range(nbrokers)],
        "client": [f"c{i}" for i in range(nclients)],
        "monitor": [f"m{i}" for i in range(nmonitors)],
    }
    hostvars = {}
    for i, h in enumerate(groups["broker"]):
        hostvars[h] = {"private_ip": f"10.0.0.{10 + i}", "id": str(i)}
    for i, h in enumerate(groups["client"]):
        hostvars[h] = {"private_ip": f"10.0.0.{50 + i}", "id": str(i)}
    for i, h in enumerate(groups["monitor"]):
        hostvars[h] = {"private_ip": f"10.0.0.{200 + i}", "id": str(i)}

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(ROOT / "ansible" / "templates")),
        keep_trailing_newline=True)
    v = yaml.safe_load((ROOT / "ansible" / "monitoring-vars.yaml").read_text())
    out = env.get_template("prometheus.yml.j2").render(
        groups=groups, hostvars=hostvars, **v)
    return yaml.safe_load(out), v


def main():
    nbrokers = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    nclients = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    doc, v = render(nbrokers, nclients)
    jobs = {j["job_name"]: j for j in doc["scrape_configs"]}
    fails = []

    def targets(job):
        return [t for sc in jobs[job].get("static_configs", [])
                for t in sc["targets"]]

    def labelled(job):
        return [(t, sc.get("labels", {}))
                for sc in jobs[job].get("static_configs", [])
                for t in sc["targets"]]

    expect = {
        "redpanda": nbrokers,
        "redpanda-internal": nbrokers,
        "node": nbrokers + nclients + 1,
        "loadgen": nclients,
        "prometheus": 1,
    }
    for job, n in expect.items():
        if job not in jobs:
            fails.append(f"no {job} job at all")
            continue
        got = len(targets(job))
        status = "ok  " if got == n else "FAIL"
        if got != n:
            fails.append(f"{job}: {got} targets, expected {n}")
        print(f"  {status} {job:20s} {got} targets")

    # The endpoint split is the thing that makes a 2s scrape defensible, so it
    # is worth asserting rather than trusting.
    if jobs.get("redpanda", {}).get("metrics_path") != "/public_metrics":
        fails.append("the redpanda job must scrape /public_metrics")
    if jobs.get("redpanda-internal", {}).get("metrics_path") != "/metrics":
        fails.append("the redpanda-internal job must scrape /metrics")
    pub = jobs.get("redpanda", {}).get("scrape_interval")
    internal = jobs.get("redpanda-internal", {}).get("scrape_interval")
    if pub == internal:
        fails.append(
            f"both redpanda jobs scrape at {pub}; the internal endpoint is the "
            "expensive one and must be slower")
    print(f"  ok   endpoint split      /public_metrics @{pub}, /metrics @{internal}")

    for job in jobs:
        for t, labs in labelled(job):
            if "redpanda_id" not in labs:
                fails.append(f"{job} target {t} has no redpanda_id label")
    print("  ok   every target carries redpanda_id")

    # host and role are what the health dashboard selects on. Without them its
    # client panels select nothing and render empty.
    hosts = set()
    for t, labs in labelled("node"):
        for want in ("host", "role"):
            if want not in labs:
                fails.append(f"node target {t} has no {want} label")
        hosts.add(labs.get("host"))
    for i in range(nbrokers):
        if f"broker{i}" not in hosts:
            fails.append(f"node job is missing host=broker{i}")
    for i in range(nclients):
        if f"client{i}" not in hosts:
            fails.append(f"node job is missing host=client{i}")
    if "monitor" not in hosts:
        fails.append("node job is missing host=monitor")
    print(f"  ok   node hosts          {len(hosts)} distinct host labels")

    print()
    if fails:
        print(f"{len(fails)} PROBLEM(S):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print(f"prometheus.yml.j2 renders correctly for {nbrokers} brokers / "
          f"{nclients} clients / 1 monitor")
    return 0


if __name__ == "__main__":
    sys.exit(main())
