#!/usr/bin/env python3
"""
Run paper experiments repeatedly with full environment reset between runs.

Features:
- cleans Docker containers and volumes before every repetition
- starts infra through docker-master.py
- runs baseline, transactional, or audit experiment processes
- exports Prometheus metrics for the measured window
- captures Grafana screenshots on macOS and Linux when desktop tools are available
- stores logs, screenshots, and metrics in separate per-run folders
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
DOCKER_MASTER = ROOT / "docker-master.py"
EXPORT_SCRIPT = ROOT / "monitoring-dash" / "experiment_export" / "export_prometheus_run.py"
BENCHMARK_PROPERTIES = APP_DIR / "src" / "main" / "resources" / "benchmark.properties"
FAULTS_PROPERTIES = APP_DIR / "src" / "main" / "resources" / "faults.properties"
ARTIFACT_ROOT = ROOT / "artifacts" / "experiment_campaigns"

GRAFANA_BASE = "http://localhost:3000"
BASELINE_DASHBOARD_UID = "k-dash"
AUDIT_DASHBOARD_UID = "audit-outcomes"


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen
    log_path: Path


@dataclass(frozen=True)
class ExperimentDefinition:
    name: str
    run_label: str
    consumer_main: str
    extra_processes: tuple[tuple[str, list[str], Path], ...]
    benchmark_overrides: dict[str, str]
    dashboards: tuple[tuple[str, str], ...]
    notes: str


EXPERIMENTS: dict[str, ExperimentDefinition] = {
    "baseline": ExperimentDefinition(
        name="baseline",
        run_label="baseline",
        consumer_main="com.kafka.perf.baseline.PostgresSinkConsumer",
        extra_processes=(),
        benchmark_overrides={
            "transaction.enabled": "false",
            "enable.idempotence": "false",
            "acks": "1",
            "retries": "0",
            "consumer.isolation.level": "read_uncommitted",
        },
        dashboards=(("baseline", BASELINE_DASHBOARD_UID),),
        notes="Plain sink baseline without Kafka transactions.",
    ),
    "transactional": ExperimentDefinition(
        name="transactional",
        run_label="transactional",
        consumer_main="com.kafka.perf.baseline.PostgresSinkConsumer",
        extra_processes=(),
        benchmark_overrides={
            "transaction.enabled": "true",
            "enable.idempotence": "true",
            "acks": "all",
            "retries": "5",
            "consumer.isolation.level": "read_committed",
        },
        dashboards=(("baseline", BASELINE_DASHBOARD_UID),),
        notes="Transactional producer with PostgreSQL sink consumers.",
    ),
    "audit": ExperimentDefinition(
        name="audit",
        run_label="audit",
        consumer_main="com.kafka.perf.faults.FaultInjectorWithAuditConsumer",
        extra_processes=(
            (
                "audit-aggregator",
                ["java", "-cp", "target/kafka-perf-1.0.0.jar", "com.kafka.perf.audit.AuditAggregator"],
                APP_DIR,
            ),
        ),
        benchmark_overrides={
            "transaction.enabled": "true",
            "enable.idempotence": "true",
            "acks": "all",
            "retries": "5",
            "consumer.isolation.level": "read_committed",
        },
        dashboards=(("baseline", BASELINE_DASHBOARD_UID), ("audit-outcomes", AUDIT_DASHBOARD_UID)),
        notes="Transactional producer with audit-enabled consumer path and Kafka Streams aggregator.",
    ),
}


def resolve_maven_command() -> list[str]:
    mvnw = APP_DIR / "mvnw"
    if mvnw.exists():
        return [str(mvnw)]
    if shutil.which("mvn"):
        return ["mvn"]
    raise RuntimeError("Maven executable not found. Install mvn or add app/mvnw.")


def run_command(
    command: list[str],
    *,
    cwd: Path,
    log_path: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=True,
    )
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        body = []
        body.append(f"$ {' '.join(command)}")
        if result.stdout:
            body.append("\n[stdout]\n" + result.stdout.rstrip() + "\n")
        if result.stderr:
            body.append("\n[stderr]\n" + result.stderr.rstrip() + "\n")
        log_path.write_text("\n".join(body).strip() + "\n", encoding="utf-8")
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed with rc={result.returncode}: {' '.join(command)}\n"
            f"{result.stderr.strip()}"
        )
    return result


def build_java_exec_command(maven_cmd: list[str], main_class: str) -> list[str]:
    return [*maven_cmd, "-q", "exec:java", f"-Dexec.mainClass={main_class}"]


def start_process(
    name: str,
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
) -> ManagedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w", encoding="utf-8")
    handle.write(f"$ {' '.join(command)}\n")
    handle.flush()
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return ManagedProcess(name=name, process=process, log_path=log_path)


def stop_process(proc: ManagedProcess, grace_seconds: int) -> None:
    if proc.process.poll() is not None:
        return
    try:
        os.killpg(proc.process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if proc.process.poll() is not None:
            return
        time.sleep(1)

    try:
        os.killpg(proc.process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    proc.process.wait(timeout=5)


def sleep_with_updates(seconds: int, label: str, run_log: Path) -> None:
    remaining = seconds
    while remaining > 0:
        message = f"[wait] {label}: {remaining}s remaining"
        print(message)
        append_line(run_log, message)
        step = min(30, remaining)
        time.sleep(step)
        remaining -= step


def append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def replace_property_lines(text: str, overrides: dict[str, str]) -> str:
    lines = text.splitlines()
    seen: set[str] = set()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in overrides:
            lines[index] = f"{key}={overrides[key]}"
            seen.add(key)

    for key, value in overrides.items():
        if key not in seen:
            lines.append(f"{key}={value}")

    return "\n".join(lines) + "\n"


def apply_properties(path: Path, original_text: str, overrides: dict[str, str]) -> None:
    path.write_text(replace_property_lines(original_text, overrides), encoding="utf-8")


def restore_properties(path: Path, original_text: str) -> None:
    path.write_text(original_text, encoding="utf-8")


def capture_grafana_screenshot(url: str, output_path: Path, delay_seconds: int = 12) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "darwin":
        apple_script = f"""
        tell application "Safari"
            activate
            if (count of windows) = 0 then
                make new document
            end if
            set URL of front document to "{url}"
            delay {delay_seconds}
            set bounds of front window to {{40, 60, 1440, 980}}
            activate
        end tell
        delay 1
        """
        subprocess.run(["osascript", "-e", apple_script], check=True)
        subprocess.run(
            ["screencapture", "-x", "-R40,60,1400,920", str(output_path)],
            check=True,
        )
        return

    browser = None
    for candidate in ("google-chrome", "chromium", "chromium-browser", "firefox"):
        if shutil.which(candidate):
            browser = candidate
            break
    if browser is None:
        raise RuntimeError(
            "No supported browser found for screenshot capture. Install chrome/chromium/firefox "
            "or run on macOS with Safari."
        )

    if shutil.which("gnome-screenshot"):
        proc = subprocess.Popen([browser, "--new-window", url])
        try:
            time.sleep(delay_seconds)
            subprocess.run(["gnome-screenshot", "-f", str(output_path)], check=True)
        finally:
            proc.terminate()
            proc.wait(timeout=10)
        return

    if shutil.which("scrot"):
        proc = subprocess.Popen([browser, "--new-window", url])
        try:
            time.sleep(delay_seconds)
            subprocess.run(["scrot", str(output_path)], check=True)
        finally:
            proc.terminate()
            proc.wait(timeout=10)
        return

    raise RuntimeError(
        "Linux screenshot capture requires gnome-screenshot or scrot. "
        "Install one of them to capture Grafana browser screenshots."
    )


def grafana_dashboard_url(uid: str, from_ts_ms: int, to_ts_ms: int) -> str:
    return (
        f"{GRAFANA_BASE}/d/{uid}/{uid}"
        f"?orgId=1&from={from_ts_ms}&to={to_ts_ms}&kiosk"
    )


def ensure_build(run_log: Path, maven_cmd: list[str]) -> None:
    append_line(run_log, "[build] packaging app jar for experiment")
    run_command(
        [*maven_cmd, "-q", "-DskipTests", "package"],
        cwd=APP_DIR,
        log_path=run_log.parent / "logs" / "build.log",
    )


def refresh_runtime_resources(run_dir: Path, maven_cmd: list[str]) -> None:
    run_command(
        [*maven_cmd, "-q", "resources:resources", "compile"],
        cwd=APP_DIR,
        log_path=run_dir / "logs" / "refresh_runtime_resources.log",
    )


def export_prometheus(
    run_label: str,
    measured_start_ts: float,
    measured_end_ts: float,
    run_dir: Path,
    prom_url: str,
    step_secs: int,
    export_all_metrics: bool,
) -> Path:
    command = [
        sys.executable,
        str(EXPORT_SCRIPT),
        "--run-label",
        run_label,
        "--start-ts",
        str(measured_start_ts),
        "--end-ts",
        str(measured_end_ts),
        "--prom-url",
        prom_url,
        "--step-secs",
        str(step_secs),
    ]
    if export_all_metrics:
        command.append("--all-metrics")

    result = run_command(
        command,
        cwd=ROOT,
        log_path=run_dir / "logs" / "prometheus_export.log",
    )
    source_dir = Path(result.stdout.strip())
    metrics_dir = run_dir / "metrics"
    if metrics_dir.exists():
        shutil.rmtree(metrics_dir)
    shutil.copytree(source_dir, metrics_dir)
    return metrics_dir


def capture_service_logs(run_dir: Path) -> None:
    logs_dir = run_dir / "logs" / "docker"
    service_names = ("kafka", "monitoring", "sink")
    for service in service_names:
        run_command(
            ["python3", str(DOCKER_MASTER), "logs", "--service", service, "--tail", "10000"],
            cwd=ROOT,
            log_path=logs_dir / f"{service}.log",
            check=False,
        )


def write_metadata(run_dir: Path, payload: dict) -> None:
    metadata_path = run_dir / "metadata.json"
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_single_experiment(
    experiment: ExperimentDefinition,
    repetition: int,
    args: argparse.Namespace,
    benchmark_original: str,
    faults_original: str,
    maven_cmd: list[str],
    campaign_dir: Path,
) -> None:
    run_name = f"{experiment.name}-run-{repetition:02d}"
    run_dir = campaign_dir / experiment.name / run_name
    logs_dir = run_dir / "logs"
    screenshots_dir = run_dir / "screenshots"
    run_log = logs_dir / "runner.log"
    managed: list[ManagedProcess] = []
    measured_start_ts: float | None = None
    measured_end_ts: float | None = None

    run_dir.mkdir(parents=True, exist_ok=True)
    append_line(run_log, f"[run] starting {run_name}")
    append_line(run_log, f"[run] notes: {experiment.notes}")

    consumer_env = os.environ.copy()
    consumer_env.update(
        {
            "KAFKA_BROKERS": args.brokers,
            "KAFKA_TOPIC": args.topic,
            "KAFKA_GROUP_ID": f"{args.group_id_prefix}-{experiment.name}",
            "POSTGRES_URL": args.postgres_url,
            "POSTGRES_USER": args.postgres_user,
            "POSTGRES_PASSWORD": args.postgres_password,
        }
    )

    producer_env = os.environ.copy()
    audit_env = os.environ.copy()
    audit_env.update(
        {
            "BOOTSTRAP_SERVERS": args.audit_bootstrap_servers,
            "AUDIT_TOPIC": args.audit_topic,
            "TRANSACTION_TIMEOUT_MS": str(args.audit_timeout_ms),
        }
    )

    def cleanup() -> None:
        for proc in reversed(managed):
            append_line(run_log, f"[stop] {proc.name}")
            stop_process(proc, args.shutdown_grace_secs)

    try:
        apply_properties(BENCHMARK_PROPERTIES, benchmark_original, experiment.benchmark_overrides)
        if args.keep_faults_as_is:
            append_line(run_log, "[config] leaving faults.properties unchanged")
        else:
            restore_properties(FAULTS_PROPERTIES, faults_original)
            append_line(run_log, "[config] restored faults.properties to repo version")
        refresh_runtime_resources(run_dir, maven_cmd)

        write_metadata(
            run_dir,
            {
                "experiment": experiment.name,
                "repetition": repetition,
                "notes": experiment.notes,
                "benchmark_overrides": experiment.benchmark_overrides,
                "group_id": consumer_env["KAFKA_GROUP_ID"],
                "topic": args.topic,
                "break_after_seconds": args.break_secs,
                "measured_window_seconds": args.measured_window_secs,
            },
        )

        append_line(run_log, "[infra] docker clean")
        run_command(
            ["python3", str(DOCKER_MASTER), "clean"],
            cwd=ROOT,
            log_path=logs_dir / "docker_clean.log",
        )
        append_line(run_log, "[infra] docker up")
        run_command(
            ["python3", str(DOCKER_MASTER), "up", "--create-topics", "--create-tables"],
            cwd=ROOT,
            log_path=logs_dir / "docker_up.log",
        )

        sleep_with_updates(args.stabilize_secs, "after docker stack start", run_log)

        if experiment.name == "audit":
            for extra_name, command, cwd in experiment.extra_processes:
                env = audit_env if extra_name == "audit-aggregator" else os.environ.copy()
                proc = start_process(
                    extra_name,
                    command,
                    cwd=cwd,
                    env=env,
                    log_path=logs_dir / f"{extra_name}.log",
                )
                managed.append(proc)
                append_line(run_log, f"[start] {extra_name} pid={proc.process.pid}")
                time.sleep(2)

        for index in range(args.consumer_count):
            proc = start_process(
                f"consumer-{index + 1}",
                build_java_exec_command(maven_cmd, experiment.consumer_main),
                cwd=APP_DIR,
                env=consumer_env,
                log_path=logs_dir / f"consumer-{index + 1}.log",
            )
            managed.append(proc)
            append_line(run_log, f"[start] consumer-{index + 1} pid={proc.process.pid}")
            time.sleep(1)

        sleep_with_updates(args.consumer_warmup_secs, "after consumers start, before producer", run_log)

        producer = start_process(
            "producer",
            build_java_exec_command(maven_cmd, "com.kafka.perf.baseline.BaselineProducer"),
            cwd=APP_DIR,
            env=producer_env,
            log_path=logs_dir / "producer.log",
        )
        managed.append(producer)
        append_line(run_log, f"[start] producer pid={producer.process.pid}")
        measured_start_ts = time.time()

        sleep_with_updates(args.measured_window_secs, "measured run", run_log)
        measured_end_ts = time.time()

        export_prometheus(
            f"{args.campaign_name}-{run_name}",
            measured_start_ts,
            measured_end_ts,
            run_dir,
            args.prom_url,
            args.prom_step_secs,
            args.all_prometheus_metrics,
        )

        from_ts_ms = int(measured_start_ts * 1000)
        to_ts_ms = int(measured_end_ts * 1000)
        for label, uid in experiment.dashboards:
            screenshot_path = screenshots_dir / f"{run_name}-{label}.png"
            append_line(run_log, f"[screenshot] capturing {label}")
            capture_grafana_screenshot(
                grafana_dashboard_url(uid, from_ts_ms, to_ts_ms),
                screenshot_path,
                delay_seconds=args.screenshot_wait_secs,
            )

        capture_service_logs(run_dir)
    finally:
        cleanup()
        run_command(
            ["python3", str(DOCKER_MASTER), "down"],
            cwd=ROOT,
            log_path=logs_dir / "docker_down.log",
            check=False,
        )
        restore_properties(BENCHMARK_PROPERTIES, benchmark_original)
        if not args.keep_faults_as_is:
            restore_properties(FAULTS_PROPERTIES, faults_original)


def parse_experiment_list(names: str) -> Iterable[ExperimentDefinition]:
    for name in [part.strip() for part in names.split(",") if part.strip()]:
        if name not in EXPERIMENTS:
            raise ValueError(f"Unknown experiment '{name}'. Available: {', '.join(sorted(EXPERIMENTS))}")
        yield EXPERIMENTS[name]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run repeated Kafka paper experiments.")
    parser.add_argument(
        "--experiments",
        default="baseline,transactional,audit",
        help="Comma-separated experiment names. Default: baseline,transactional,audit",
    )
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--break-secs", type=int, default=7200)
    parser.add_argument("--campaign-name", default=time.strftime("paper-campaign-%Y%m%d-%H%M%S"))
    parser.add_argument("--stabilize-secs", type=int, default=120)
    parser.add_argument("--consumer-warmup-secs", type=int, default=180)
    parser.add_argument("--measured-window-secs", type=int, default=1500)
    parser.add_argument("--consumer-count", type=int, default=3)
    parser.add_argument("--group-id-prefix", default="paper-run")
    parser.add_argument("--topic", default="eos-topic")
    parser.add_argument("--brokers", default="localhost:9092,localhost:9093,localhost:9094")
    parser.add_argument("--postgres-url", default="jdbc:postgresql://localhost:5432/eos_sink")
    parser.add_argument("--postgres-user", default="eos")
    parser.add_argument("--postgres-password", default="eos")
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--prom-step-secs", type=int, default=5)
    parser.add_argument("--all-prometheus-metrics", action="store_true")
    parser.add_argument("--shutdown-grace-secs", type=int, default=20)
    parser.add_argument("--screenshot-wait-secs", type=int, default=12)
    parser.add_argument("--audit-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--audit-topic", default="audit-topic")
    parser.add_argument("--audit-timeout-ms", type=int, default=60000)
    parser.add_argument(
        "--keep-faults-as-is",
        action="store_true",
        help="Do not restore faults.properties before audit runs; use the current local file as-is.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.repetitions <= 0:
        print("repetitions must be > 0", file=sys.stderr)
        return 2

    try:
        experiments = list(parse_experiment_list(args.experiments))
        maven_cmd = resolve_maven_command()
    except (ValueError, RuntimeError) as err:
        print(str(err), file=sys.stderr)
        return 2

    benchmark_original = BENCHMARK_PROPERTIES.read_text(encoding="utf-8")
    faults_original = FAULTS_PROPERTIES.read_text(encoding="utf-8")
    campaign_dir = ARTIFACT_ROOT / args.campaign_name
    campaign_dir.mkdir(parents=True, exist_ok=True)

    ensure_build(campaign_dir / "campaign.log", maven_cmd)

    try:
        for experiment in experiments:
            for repetition in range(1, args.repetitions + 1):
                print(f"[campaign] {experiment.name} run {repetition}/{args.repetitions}")
                run_single_experiment(
                    experiment,
                    repetition,
                    args,
                    benchmark_original,
                    faults_original,
                    maven_cmd,
                    campaign_dir,
                )
                if repetition != args.repetitions or experiment != experiments[-1]:
                    sleep_with_updates(args.break_secs, "between runs", campaign_dir / "campaign.log")
    finally:
        restore_properties(BENCHMARK_PROPERTIES, benchmark_original)
        restore_properties(FAULTS_PROPERTIES, faults_original)

    print(f"[campaign] artifacts saved under {campaign_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
