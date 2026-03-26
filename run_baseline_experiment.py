#!/usr/bin/env python3
"""
Timed host-local experiment runner for the baseline PostgreSQL sink setup.

Default schedule:
- wait 2 minutes
- start 3 PostgresSinkConsumer processes
- wait 3 more minutes
- start BaselineProducer
- run the measured window for 25 minutes
- stop all child processes
"""

from __future__ import annotations

import argparse
import os
import signal
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent
APP_DIR = ROOT / "app"
EXPORT_SCRIPT = ROOT / "monitoring-dash" / "experiment_export" / "export_prometheus_run.py"


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen


def build_consumer_command(maven_cmd: list[str]) -> list[str]:
    return [
        *maven_cmd,
        "-q",
        "exec:java",
        "-Dexec.mainClass=com.kafka.perf.baseline.PostgresSinkConsumer",
    ]


def build_producer_command(maven_cmd: list[str]) -> list[str]:
    return [
        *maven_cmd,
        "-q",
        "exec:java",
        "-Dexec.mainClass=com.kafka.perf.baseline.BaselineProducer",
    ]


def resolve_maven_command() -> list[str]:
    """Return an available Maven command or fail with a clear message."""
    mvnw = APP_DIR / "mvnw"
    if mvnw.exists():
        return [str(mvnw)]

    if shutil.which("mvn"):
        return ["mvn"]

    raise RuntimeError(
        "Maven executable not found. Install Maven (mvn) or add Maven Wrapper at app/mvnw."
    )


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
    parser = argparse.ArgumentParser(description="Run a timed baseline sink experiment.")
    parser.add_argument("--initial-wait-secs", type=int, default=120)
    parser.add_argument("--consumer-warmup-secs", type=int, default=180)
    parser.add_argument("--measured-window-secs", type=int, default=1500)
    parser.add_argument("--consumer-count", type=int, default=3)
    parser.add_argument("--group-id", default="baseline-sink-group")
    parser.add_argument("--topic", default="eos-topic")
    parser.add_argument("--brokers", default="localhost:9092,localhost:9093,localhost:9094")
    parser.add_argument("--postgres-url", default="jdbc:postgresql://localhost:5432/eos_sink")
    parser.add_argument("--postgres-user", default="eos")
    parser.add_argument("--postgres-password", default="eos")
    parser.add_argument("--shutdown-grace-secs", type=int, default=20)
    parser.add_argument("--export-prometheus", action="store_true", default=True)
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--prom-step-secs", type=int, default=10)
    parser.add_argument("--run-label", default="baseline-run")
    args = parser.parse_args()

    if not APP_DIR.exists():
        print(f"app directory not found: {APP_DIR}", file=sys.stderr)
        return 1

    try:
        maven_cmd = resolve_maven_command()
    except RuntimeError as err:
        print(str(err), file=sys.stderr)
        return 1

    managed: list[ManagedProcess] = []
    measured_start_ts: float | None = None
    measured_end_ts: float | None = None

    def cleanup() -> None:
        for proc in reversed(managed):
            stop_process(proc, args.shutdown_grace_secs)

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

    total_window = args.initial_wait_secs + args.consumer_warmup_secs + args.measured_window_secs
    print("[experiment] baseline timed run")
    print(f"[experiment] total wall-clock window: {total_window}s")
    print(f"[experiment] initial idle wait: {args.initial_wait_secs}s")
    print(f"[experiment] consumer-only warmup: {args.consumer_warmup_secs}s")
    print(f"[experiment] measured producer window: {args.measured_window_secs}s")
    print(f"[experiment] consumer count: {args.consumer_count}")

    try:
        sleep_with_updates(args.initial_wait_secs, "before starting consumers")

        for index in range(args.consumer_count):
            managed.append(
                start_process(
                    f"consumer-{index + 1}",
                    build_consumer_command(maven_cmd),
                    consumer_env,
                )
            )
            time.sleep(1)

        sleep_with_updates(args.consumer_warmup_secs, "after consumers start, before producer")

        managed.append(start_process("producer", build_producer_command(maven_cmd), producer_env))
        measured_start_ts = time.time()

        sleep_with_updates(args.measured_window_secs, "measured run")
        measured_end_ts = time.time()
        return 0
    finally:
        cleanup()
        if measured_start_ts is not None:
            measured_end_ts = measured_end_ts or time.time()
            if args.export_prometheus:
                export_cmd = [
                    sys.executable,
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
