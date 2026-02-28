#!/usr/bin/env python3
"""
Master Docker Compose Manager for Kafka Performance Testing
Manages: Kafka Cluster, Monitoring Dashboard, and Sink services
"""

import os
import sys
import subprocess
import time
import argparse
from pathlib import Path
from typing import Optional, List
from enum import Enum
from distutils.spawn import find_executable


class Service(Enum):
    """Available services to manage"""
    KAFKA = "kafka"
    MONITORING = "monitoring"
    SINK = "sink"
    ALL = "all"


class ServiceConfig:
    """Configuration for each service"""
    
    def __init__(self, name: str, path: str, compose_file: str, depends_on: Optional[List[str]] = None):
        self.name = name
        self.path = Path(path)
        self.compose_file = compose_file
        self.full_path = self.path / compose_file
        self.depends_on = depends_on or []
    
    def exists(self) -> bool:
        """Check if compose file exists"""
        return self.full_path.exists()


class DockerComposeManager:
    """Manages docker-compose operations across multiple services"""
    
    # Service configurations
    SERVICES = {
        Service.KAFKA: ServiceConfig(
            name="Kafka Cluster",
            path="/Users/ajay/Workspace/kafka-perf/kafka-cluster",
            compose_file="docker-compose-kafka-cluster.yml"
        ),
        Service.MONITORING: ServiceConfig(
            name="Monitoring Dashboard",
            path="/Users/ajay/Workspace/kafka-perf/monitoring-dash",
            compose_file="docker-compose-monitoring.yml",
            depends_on=[Service.KAFKA.value]
        ),
        Service.SINK: ServiceConfig(
            name="Sink (PostgreSQL)",
            path="/Users/ajay/Workspace/kafka-perf/sink",
            compose_file="docker-sink-compose.yml",
            depends_on=[Service.KAFKA.value]
        ),
    }
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._validate_services()
    
    def _validate_services(self):
        """Validate that all compose files exist"""
        missing = []
        for service_enum, config in self.SERVICES.items():
            if not config.exists():
                missing.append(f"{config.name}: {config.full_path}")
        
        if missing:
            print("❌ Error: Missing docker-compose files:")
            for item in missing:
                print(f"  - {item}")
            sys.exit(1)
    
    def _get_services_to_run(self, service: Service) -> List[Service]:
        """Get list of services to run, respecting dependencies"""
        if service == Service.ALL:
            return [Service.KAFKA, Service.MONITORING, Service.SINK]
        return [service]
    
    def _get_ordered_services(self, services: List[Service]) -> List[Service]:
        """Order services respecting dependencies"""
        ordered = []
        processed = set()
        
        def add_with_deps(svc: Service):
            if svc in processed:
                return
            deps = self.SERVICES[svc].depends_on
            for dep_name in deps:
                for svc_enum in Service:
                    if svc_enum.value == dep_name and svc_enum != Service.ALL:
                        add_with_deps(svc_enum)
            ordered.append(svc)
            processed.add(svc)
        
        for svc in services:
            add_with_deps(svc)
        
        return ordered
    
    def _run_command(self, config: ServiceConfig, command: str) -> bool:
        """Run a docker-compose command"""
        cmd = f"docker-compose -f {config.compose_file} {command}"
        
        if self.verbose:
            print(f"📋 Running in {config.path}:")
            print(f"   {cmd}")
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                cwd=str(config.path),
                capture_output=not self.verbose,
                text=True
            )
            return result.returncode == 0
        except Exception as e:
            print(f"❌ Error running command: {e}")
            return False
    
    def up(self, service: Service = Service.ALL, detach: bool = True, create_topics: bool = False, create_tables: bool = False):
        """Start services"""
        services = self._get_services_to_run(service)
        ordered = self._get_ordered_services(services)
        
        print(f"🚀 Starting {len(ordered)} service(s)...\n")
        
        for svc in ordered:
            config = self.SERVICES[svc]
            print(f"▶️  Starting {config.name}...")
            
            cmd = "up -d" if detach else "up"
            if not self._run_command(config, cmd):
                print(f"❌ Failed to start {config.name}")
                return False
            
            print(f"✅ {config.name} started\n")
            time.sleep(1)
        
        print("🎉 All services started successfully!")
        
        # Create topics if requested and Kafka was started
        if create_topics and (Service.KAFKA in ordered or service == Service.ALL):
            print()
            self.create_topics(wait_for_kafka=True)
        
        # Create database tables if requested and Sink was started
        if create_tables and (Service.SINK in ordered or service == Service.ALL):
            print()
            self.create_database_tables(wait_for_db=True)
        
        return True
    
    def down(self, service: Service = Service.ALL):
        """Stop services"""
        services = self._get_services_to_run(service)
        # Stop in reverse order of dependencies
        ordered = list(reversed(self._get_ordered_services(services)))
        
        print(f"🛑 Stopping {len(ordered)} service(s)...\n")
        
        for svc in ordered:
            config = self.SERVICES[svc]
            print(f"⏸️  Stopping {config.name}...")
            
            if not self._run_command(config, "down"):
                print(f"⚠️  Warning: Failed to stop {config.name}")
            else:
                print(f"✅ {config.name} stopped\n")
        
        print("✅ All services stopped")
        return True
    
    def status(self):
        """Show status of all services"""
        print("📊 Service Status:\n")
        
        for svc, config in self.SERVICES.items():
            cmd = "ps --format table"
            result = subprocess.run(
                f"docker-compose -f {config.compose_file} {cmd}",
                shell=True,
                cwd=str(config.path),
                capture_output=True,
                text=True
            )
            
            print(f"🔹 {config.name} ({config.path.name}):")
            if result.returncode == 0 and result.stdout.strip():
                # Print each line of the table with indentation
                for line in result.stdout.strip().split('\n'):
                    print(f"   {line}")
            else:
                print(f"   ℹ️  No running containers")
            print()
    
    def logs(self, service: Service, follow: bool = False, tail: int = 100):
        """Show logs from a service"""
        if service == Service.ALL:
            print("❌ Error: Use specific service for logs (kafka, monitoring, or sink)")
            return
        
        config = self.SERVICES[service]
        cmd = f"logs {'--follow' if follow else ''} --tail {tail}"
        
        print(f"📝 Logs for {config.name}:\n")
        self._run_command(config, cmd)
    
    def restart(self, service: Service = Service.ALL):
        """Restart services"""
        print(f"🔄 Restarting services...\n")
        self.down(service)
        time.sleep(2)
        self.up(service)
    
    def clean(self, service: Service = Service.ALL):
        """Remove services and volumes"""
        services = self._get_services_to_run(service)
        ordered = list(reversed(self._get_ordered_services(services)))
        
        print(f"🗑️  Cleaning {len(ordered)} service(s)...\n")
        
        for svc in ordered:
            config = self.SERVICES[svc]
            print(f"🧹 Cleaning {config.name}...")
            
            if not self._run_command(config, "down -v"):
                print(f"⚠️  Warning: Failed to clean {config.name}")
            else:
                print(f"✅ {config.name} cleaned\n")
        
        print("✅ Cleanup complete")
        return True
    
    def create_topics(self, wait_for_kafka: bool = True):
        """Create Kafka topics using the create-topic.py script"""
        kafka_config = self.SERVICES[Service.KAFKA]
        script_path = kafka_config.path / "create-topic.py"
        
        if not script_path.exists():
            print(f"❌ Error: Topic creation script not found at {script_path}")
            return False
        
        if wait_for_kafka:
            print("⏳ Waiting for Kafka to be ready...")
            time.sleep(5)  # Give Kafka time to fully start
        
        print("📋 Creating Kafka topics...\n")
        
        try:
            result = subprocess.run(
                ["python3", str(script_path)],
                cwd=str(kafka_config.path),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Print output
            if result.stdout:
                print(result.stdout)
            
            if result.returncode == 0:
                print("✅ Topics created successfully!")
                return True
            else:
                if result.stderr:
                    print(f"❌ Error: {result.stderr}")
                return False
        
        except subprocess.TimeoutExpired:
            print("❌ Error: Topic creation timed out")
            return False
        except Exception as e:
            print(f"❌ Error running topic creation script: {e}")
            return False
    
    def create_database_tables(self, wait_for_db: bool = True):
        """Create database tables using the create-tables.py script"""
        sink_config = self.SERVICES[Service.SINK]
        script_path = sink_config.path / "create-tables.py"
        
        if not script_path.exists():
            print(f"❌ Error: Database table creation script not found at {script_path}")
            return False
        
        if wait_for_db:
            print("⏳ Waiting for PostgreSQL to be ready...")
            time.sleep(5)  # Give PostgreSQL time to fully start
        
        print("📊 Creating database tables...\n")
        
        try:
            result = subprocess.run(
                ["python3", str(script_path)],
                cwd=str(sink_config.path),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Print output
            if result.stdout:
                print(result.stdout)
            
            if result.returncode == 0:
                print("✅ Database tables created successfully!")
                return True
            else:
                if result.stderr:
                    print(f"❌ Error: {result.stderr}")
                return False
        
        except subprocess.TimeoutExpired:
            print("❌ Error: Table creation timed out")
            return False
        except Exception as e:
            print(f"❌ Error running table creation script: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Master Docker Compose Manager for Kafka Performance Testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s up                           # Start all services
  %(prog)s up --create-topics           # Start services and create Kafka topics
  %(prog)s up --create-tables           # Start services and create database tables
  %(prog)s up --create-topics --create-tables  # Start and initialize everything
  %(prog)s up --service kafka           # Start only Kafka
  %(prog)s down                         # Stop all services
  %(prog)s status                       # Show status of all services
  %(prog)s logs --service kafka         # Show Kafka logs
  %(prog)s logs --service kafka --follow  # Follow Kafka logs
  %(prog)s topics                       # Create Kafka topics
  %(prog)s tables                       # Create database tables
  %(prog)s restart                      # Restart all services
  %(prog)s clean                        # Remove all services and volumes
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Up command
    up_parser = subparsers.add_parser("up", help="Start services")
    up_parser.add_argument(
        "--service",
        choices=["kafka", "monitoring", "sink", "all"],
        default="all",
        help="Service to start (default: all)"
    )
    up_parser.add_argument(
        "--create-topics",
        action="store_true",
        help="Create Kafka topics after starting (requires create-topic.py script)"
    )
    up_parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create database tables after starting (requires create-tables.py script)"
    )
    up_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Down command
    down_parser = subparsers.add_parser("down", help="Stop services")
    down_parser.add_argument(
        "--service",
        choices=["kafka", "monitoring", "sink", "all"],
        default="all",
        help="Service to stop (default: all)"
    )
    down_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show service status")
    status_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Show service logs")
    logs_parser.add_argument(
        "--service",
        choices=["kafka", "monitoring", "sink"],
        required=True,
        help="Service to show logs for"
    )
    logs_parser.add_argument(
        "--follow",
        action="store_true",
        help="Follow log output"
    )
    logs_parser.add_argument(
        "--tail",
        type=int,
        default=100,
        help="Number of lines to show (default: 100)"
    )
    logs_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Restart command
    restart_parser = subparsers.add_parser("restart", help="Restart services")
    restart_parser.add_argument(
        "--service",
        choices=["kafka", "monitoring", "sink", "all"],
        default="all",
        help="Service to restart (default: all)"
    )
    restart_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Remove services and volumes")
    clean_parser.add_argument(
        "--service",
        choices=["kafka", "monitoring", "sink", "all"],
        default="all",
        help="Service to clean (default: all)"
    )
    clean_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Topics command
    topics_parser = subparsers.add_parser("topics", help="Create Kafka topics")
    topics_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    # Tables command
    tables_parser = subparsers.add_parser("tables", help="Create database tables")
    tables_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
    
    # Map service names to enums
    service_map = {
        "kafka": Service.KAFKA,
        "monitoring": Service.MONITORING,
        "sink": Service.SINK,
        "all": Service.ALL,
    }
    
    verbose = getattr(args, "verbose", False)
    manager = DockerComposeManager(verbose=verbose)
    
    try:
        if args.command == "up":
            service = service_map[args.service]
            manager.up(service, create_topics=args.create_topics, create_tables=args.create_tables)
        
        elif args.command == "down":
            service = service_map[args.service]
            manager.down(service)
        
        elif args.command == "status":
            manager.status()
        
        elif args.command == "logs":
            service = service_map[args.service]
            manager.logs(service, follow=args.follow, tail=args.tail)
        
        elif args.command == "topics":
            manager.create_topics(wait_for_kafka=True)
        
        elif args.command == "tables":
            manager.create_database_tables(wait_for_db=True)
        
        elif args.command == "restart":
            service = service_map[args.service]
            manager.restart(service)
        
        elif args.command == "clean":
            service = service_map[args.service]
            manager.clean(service)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
