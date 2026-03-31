#!/usr/bin/env python3
"""
Timed baseline experiment runner.

Schedule:
- start docker-master services
- wait 2 minutes for containers to stabilize
- start 3 PostgresSinkConsumer processes
- wait 2 more minutes
- start BaselineProducer
- stop the experiment after 30 minutes total from the end of infra startup

Exports Prometheus metrics for the measured producer window by default.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
DOCKER_MASTER = ROOT / "docker-master.py"
EXPORT_SCRIPT = ROOT / "monitoring-dash" / "experiment_export" / "export_prometheus_run.py"


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen


def build_consumer_command() -> list[str]:
    return [
        "mvn",
        "-q",
        "exec:java",
        "-Dexec.mainClass=com.kafka.perf.baseline.PostgresSinkConsumer",
    ]


def build_producer_command() -> list[str]:
    return [
        "mvn",
        "-q",
        "exec:java",
        "-Dexec.mainClass=com.kafka.perf.baseline.BaselineProducer",
    ]


def run_command(command: list[str], cwd: Path) -> None:
    result = subprocess.run(command, cwd=str(cwd), text=True)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def start_process(name: str, command: list[str], env: dict[str, str]) -> ManagedProcess:
    process = subprocess.Popen(
        command,
        cwd=str(APP_DIR),
        env=env,
        start_new_session=True,
    )
    print(f"[start] {name} pid={process.pid}")
    return ManagedProcess(name=name, process=process)


def stop_process(proc: ManagedProcess, grace_seconds: int) -> None:
    if proc.process.poll() is not None:
        print(f"[stop] {proc.name} already exited rc={proc.process.returncode}")
        return

    print(f"[stop] terminating {proc.name} pid={proc.process.pid}")
    try:
        os.killpg(proc.process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if proc.process.poll() is not None:
            print(f"[stop] {proc.name} exited rc={proc.process.returncode}")
            return
        time.sleep(1)

    print(f"[stop] killing {proc.name} pid={proc.process.pid}")
    try:
        os.killpg(proc.process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    proc.process.wait(timeout=5)


def sleep_with_updates(seconds: int, label: str) -> None:
    remaining = seconds
    while remaining > 0:
        step = min(30, remaining)
        print(f"[wait] {label}: {remaining}s remaining")
        time.sleep(step)
        remaining -= step


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a timed 30-minute baseline experiment.")
    parser.add_argument("--total-secs", type=int, default=1800)
    parser.add_argument("--stabilize-secs", type=int, default=120)
    parser.add_argument("--consumer-warmup-secs", type=int, default=120)
    parser.add_argument("--consumer-count", type=int, default=3)
    parser.add_argument("--group-id", default="baseline-sink-group")
    parser.add_argument("--topic", default="eos-topic")
    parser.add_argument("--brokers", default="localhost:9092,localhost:9093,localhost:9094")
    parser.add_argument("--postgres-url", default="jdbc:postgresql://localhost:5432/eos_sink")
    parser.add_argument("--postgres-user", default="eos")
    parser.add_argument("--postgres-password", default="eos")
    parser.add_argument("--shutdown-grace-secs", type=int, default=20)
    parser.add_argument("--export-prometheus", action="store_true", default=True)
    parser.add_argument("--all-prometheus-metrics", action="store_true", default=True)
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--prom-step-secs", type=int, default=5)
    parser.add_argument("--run-label", default="baseline-30min")
    parser.add_argument("--shutdown-infra", action="store_true")
    args = parser.parse_args()

    measured_window_secs = args.total_secs - args.stabilize_secs - args.consumer_warmup_secs
    if measured_window_secs <= 0:
        print("total-secs must be greater than stabilize-secs + consumer-warmup-secs", file=sys.stderr)
        return 2

    if not APP_DIR.exists():
        print(f"app directory not found: {APP_DIR}", file=sys.stderr)
        return 1

    managed: list[ManagedProcess] = []
    measured_start_ts: float | None = None
    measured_end_ts: float | None = None
    infra_started = False

    def cleanup() -> None:
        for proc in reversed(managed):
            stop_process(proc, args.shutdown_grace_secs)
        if infra_started and args.shutdown_infra:
            print("[infra] stopping docker stack")
            run_command(["python3", str(DOCKER_MASTER), "down"], ROOT)

    def handle_signal(signum, _frame) -> None:
        print(f"[signal] received {signum}, stopping experiment")
        cleanup()
        raise SystemExit(130)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    consumer_env = os.environ.copy()
    consumer_env.update(
        {
            "KAFKA_BROKERS": args.brokers,
            "KAFKA_TOPIC": args.topic,
            "KAFKA_GROUP_ID": args.group_id,
            "POSTGRES_URL": args.postgres_url,
            "POSTGRES_USER": args.postgres_user,
            "POSTGRES_PASSWORD": args.postgres_password,
        }
    )
    producer_env = os.environ.copy()

    print("[experiment] baseline timed run")
    print(f"[experiment] total wall-clock window: {args.total_secs}s")
    print(f"[experiment] stabilization wait: {args.stabilize_secs}s")
    print(f"[experiment] consumer-only warmup: {args.consumer_warmup_secs}s")
    print(f"[experiment] measured producer window: {measured_window_secs}s")
    print(f"[experiment] consumer count: {args.consumer_count}")

    try:
        print("[infra] starting docker stack via docker-master.py")
        run_command(
            ["python3", str(DOCKER_MASTER), "up", "--create-topics", "--create-tables"],
            ROOT,
        )
        infra_started = True

        sleep_with_updates(args.stabilize_secs, "after docker stack start")

        for index in range(args.consumer_count):
            managed.append(
                start_process(
                    f"consumer-{index + 1}",
                    build_consumer_command(),
                    consumer_env,
                )
            )
            time.sleep(1)

        sleep_with_updates(args.consumer_warmup_secs, "after consumers start, before producer")

        managed.append(start_process("producer", build_producer_command(), producer_env))
        measured_start_ts = time.time()

        sleep_with_updates(measured_window_secs, "measured run")
        measured_end_ts = time.time()
        return 0
    finally:
        cleanup()
        if measured_start_ts is not None and args.export_prometheus:
            measured_end_ts = measured_end_ts or time.time()
            export_cmd = [
                "python3",
                str(EXPORT_SCRIPT),
                "--run-label",
                args.run_label,
                "--start-ts",
                str(measured_start_ts),
                "--end-ts",
                str(measured_end_ts),
                "--prom-url",
                args.prom_url,
                "--step-secs",
                str(args.prom_step_secs),
            ]
            if args.all_prometheus_metrics:
                export_cmd.append("--all-metrics")
            print("[export] exporting Prometheus metrics for measured window")
            result = subprocess.run(export_cmd, cwd=str(ROOT), text=True, capture_output=True)
            if result.returncode == 0:
                print(f"[export] metrics saved to {result.stdout.strip()}")
            else:
                print("[export] metrics export failed")
                if result.stdout.strip():
                    print(result.stdout.strip())
                if result.stderr.strip():
                    print(result.stderr.strip(), file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
