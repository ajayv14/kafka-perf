#!/usr/bin/env python3
"""
Export Prometheus metrics for a measured experiment window.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path("/Users/ajay/Workspace/kafka-perf")
EXPORT_ROOT = ROOT / "monitoring-dash" / "experiment_export" / "runs"

QUERY_SPECS = {
    "cluster_messages_per_sec": "kafka_cluster_messages_per_sec",
    "cluster_bytes_in_kb_per_sec": "kafka_cluster_bytes_in_kb_per_sec",
    "cluster_bytes_out_kb_per_sec": "kafka_cluster_bytes_out_kb_per_sec",
    "cluster_cpu_percent": "kafka_cluster_cpu_percent",
    "cluster_memory_gb": "kafka_cluster_container_memory_gb",
    "audit_committed_total": 'sum(audit_outcomes_total{outcome="COMMITTED"})',
    "audit_replay_observed_total": 'sum(audit_outcomes_total{outcome="REPLAY_OBSERVED"})',
    "audit_estimated_failed_total": 'sum(audit_outcomes_total{outcome="ESTIMATED_FAILED"})',
    "audit_late_commit_total": 'sum(audit_outcomes_total{outcome="LATE_COMMIT"})',
    "audit_batches_seen_total": "sum(audit_batches_seen_total)",
}


def prom_query_range(base_url: str, query: str, start_ts: float, end_ts: float, step: int) -> dict:
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": f"{start_ts:.3f}",
            "end": f"{end_ts:.3f}",
            "step": str(step),
        }
    )
    url = f"{base_url.rstrip('/')}/api/v1/query_range?{params}"
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed for {query}: {payload}")
    return payload


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_csv(path: Path, payload: dict) -> None:
    results = payload.get("data", {}).get("result", [])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["series", "timestamp", "value"])
        for index, series in enumerate(results):
            metric = series.get("metric", {})
            series_name = json.dumps(metric, sort_keys=True) if metric else f"series_{index}"
            for timestamp, value in series.get("values", []):
                writer.writerow([series_name, timestamp, value])


def export_run(run_label: str, start_ts: float, end_ts: float, prom_url: str, step: int) -> Path:
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(end_ts))
    out_dir = EXPORT_ROOT / f"{run_label}-{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "run_label": run_label,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "prometheus_url": prom_url,
        "step_seconds": step,
        "queries": QUERY_SPECS,
    }
    write_json(out_dir / "metadata.json", metadata)
    (out_dir / "promql.txt").write_text(
        "\n".join(f"{name} = {query}" for name, query in QUERY_SPECS.items()) + "\n",
        encoding="utf-8",
    )

    for name, query in QUERY_SPECS.items():
        payload = prom_query_range(prom_url, query, start_ts, end_ts, step)
        write_json(out_dir / f"{name}.json", payload)
        write_csv(out_dir / f"{name}.csv", payload)

    return out_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Prometheus metrics for an experiment run.")
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--start-ts", type=float, required=True)
    parser.add_argument("--end-ts", type=float, required=True)
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--step-secs", type=int, default=10)
    args = parser.parse_args()

    if args.end_ts <= args.start_ts:
        print("end-ts must be greater than start-ts", file=sys.stderr)
        return 2

    out_dir = export_run(args.run_label, args.start_ts, args.end_ts, args.prom_url, args.step_secs)
    print(out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
