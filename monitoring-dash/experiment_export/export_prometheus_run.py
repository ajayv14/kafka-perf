#!/usr/bin/env python3
"""
Export Prometheus metrics for a measured experiment window.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXPORT_ROOT = ROOT / "monitoring-dash" / "experiment_export" / "runs"

QUERY_SPECS = {
    "cluster_messages_per_sec": "kafka_cluster_messages_per_sec",
    "cluster_bytes_in_kb_per_sec": "kafka_cluster_bytes_in_kb_per_sec",
    "cluster_bytes_out_kb_per_sec": "kafka_cluster_bytes_out_kb_per_sec",
    "cluster_cpu_host_percent": "kafka_cluster_cpu_host_percent",
    "cluster_cpu_cores": "kafka_cluster_cpu_cores",
    "cluster_cpu_core_percent": "kafka_cluster_cpu_percent",
    "cluster_memory_gb": "kafka_cluster_container_memory_gb",
    "audit_committed_total": 'sum(audit_outcomes_total{outcome="COMMITTED"})',
    "audit_replay_observed_total": 'sum(audit_outcomes_total{outcome="REPLAY_OBSERVED"})',
    "audit_estimated_failed_total": 'sum(audit_outcomes_total{outcome="ESTIMATED_FAILED"})',
    "audit_late_commit_total": 'sum(audit_outcomes_total{outcome="LATE_COMMIT"})',
    "audit_batches_seen_total": "sum(audit_batches_seen_total)",
}


def prom_query(base_url: str, query: str) -> dict:
    params = urllib.parse.urlencode({"query": query})
    url = f"{base_url.rstrip('/')}/api/v1/query?{params}"
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus instant query failed for {query}: {payload}")
    return payload


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


def sanitize_filename(name: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return sanitized or "metric"


def fetch_all_metric_names(prom_url: str) -> list[str]:
    payload = prom_query(prom_url, "label_values(__name__)")
    result = payload.get("data", {}).get("result", [])
    return sorted(name for name in result if isinstance(name, str) and name)


def export_query_set(
        out_dir: Path,
        queries: dict[str, str],
        prom_url: str,
        start_ts: float,
        end_ts: float,
        step: int) -> None:
    (out_dir / "promql.txt").write_text(
        "\n".join(f"{name} = {query}" for name, query in queries.items()) + "\n",
        encoding="utf-8",
    )

    for name, query in queries.items():
        payload = prom_query_range(prom_url, query, start_ts, end_ts, step)
        write_json(out_dir / f"{name}.json", payload)
        write_csv(out_dir / f"{name}.csv", payload)


def export_run(
        run_label: str,
        start_ts: float,
        end_ts: float,
        prom_url: str,
        step: int,
        export_all_metrics: bool = False) -> Path:
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
        "export_all_metrics": export_all_metrics,
    }
    write_json(out_dir / "metadata.json", metadata)
    selected_dir = out_dir / "selected"
    selected_dir.mkdir(exist_ok=True)
    export_query_set(selected_dir, QUERY_SPECS, prom_url, start_ts, end_ts, step)

    if export_all_metrics:
        metric_names = fetch_all_metric_names(prom_url)
        all_queries = {sanitize_filename(name): name for name in metric_names}
        all_dir = out_dir / "all_metrics"
        all_dir.mkdir(exist_ok=True)
        write_json(all_dir / "metric_names.json", {"metric_names": metric_names})
        export_query_set(all_dir, all_queries, prom_url, start_ts, end_ts, step)

    return out_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Prometheus metrics for an experiment run.")
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--start-ts", type=float, required=True)
    parser.add_argument("--end-ts", type=float, required=True)
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--step-secs", type=int, default=10)
    parser.add_argument("--all-metrics", action="store_true")
    args = parser.parse_args()

    if args.end_ts <= args.start_ts:
        print("end-ts must be greater than start-ts", file=sys.stderr)
        return 2

    out_dir = export_run(
        args.run_label,
        args.start_ts,
        args.end_ts,
        args.prom_url,
        args.step_secs,
        export_all_metrics=args.all_metrics,
    )
    print(out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
