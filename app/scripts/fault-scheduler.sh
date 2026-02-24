#!/bin/bash

#################################################################################
# FAULT SCHEDULING ORCHESTRATOR SCRIPT
# 
# Master script to schedule and manage fault injection across Kafka-to-PostgreSQL
# pipeline. Enables time-based fault injection with predictable windows.
#
# Usage:
#   ./fault-scheduler.sh [COMMAND] [OPTIONS]
#
# Commands:
#   setup-faults        Configure faults in faults.properties
#   enable-fault        Enable specific fault with schedule
#   disable-fault       Disable specific fault
#   list-faults         List all faults and their schedules
#   test-schedule       Test fault scheduling configuration
#   run-consumer        Run consumer with scheduled faults
#   monitor-faults      Monitor fault injection in real-time
#
#################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FAULTS_PROPS="$PROJECT_ROOT/app/src/main/resources/faults.properties"
BENCHMARK_PROPS="$PROJECT_ROOT/app/src/main/resources/benchmark.properties"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

#################################################################################
# Helper Functions
#################################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

validate_fault_type() {
    local fault=$1
    case $fault in
        F1|F2|F3|F4|F5|F6)
            return 0
            ;;
        *)
            log_error "Invalid fault type: $fault. Must be F1-F6"
            return 1
            ;;
    esac
}

get_fault_name() {
    case $1 in
        F1) echo "Crash Before DB Commit" ;;
        F2) echo "Crash After DB Commit Before Ack" ;;
        F3) echo "Partial Batch Writes" ;;
        F4) echo "DB Container Restart" ;;
        F5) echo "Slow Sink Backpressure" ;;
        F6) echo "Network Boundary Fault" ;;
        *) echo "Unknown" ;;
    esac
}

#################################################################################
# Core Functions
#################################################################################

# Enable a specific fault with scheduling parameters
enable_fault() {
    local fault=$1
    local start_msgs=${2:-50000}
    local duration_msgs=${3:-10000}
    local iterations=${4:-1}
    
    validate_fault_type "$fault" || return 1
    
    local fault_lower=$(echo "$fault" | tr '[:upper:]' '[:lower:]')
    local fault_name=$(get_fault_name "$fault")
    
    log_info "Configuring $fault ($fault_name)..."
    
    # Update faults.properties
    if ! grep -q "fault.${fault_lower}.schedule.enabled" "$FAULTS_PROPS"; then
        log_warn "Schedule config not found for $fault, adding defaults..."
    fi
    
    # Use sed to update properties (platform-agnostic)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s/fault.${fault_lower}.schedule.enabled=.*/fault.${fault_lower}.schedule.enabled=true/" "$FAULTS_PROPS"
        sed -i '' "s/fault.${fault_lower}.schedule.start.messages=.*/fault.${fault_lower}.schedule.start.messages=$start_msgs/" "$FAULTS_PROPS"
        sed -i '' "s/fault.${fault_lower}.schedule.duration.messages=.*/fault.${fault_lower}.schedule.duration.messages=$duration_msgs/" "$FAULTS_PROPS"
        sed -i '' "s/fault.${fault_lower}.schedule.iterations=.*/fault.${fault_lower}.schedule.iterations=$iterations/" "$FAULTS_PROPS"
    else
        # Linux
        sed -i "s/fault.${fault_lower}.schedule.enabled=.*/fault.${fault_lower}.schedule.enabled=true/" "$FAULTS_PROPS"
        sed -i "s/fault.${fault_lower}.schedule.start.messages=.*/fault.${fault_lower}.schedule.start.messages=$start_msgs/" "$FAULTS_PROPS"
        sed -i "s/fault.${fault_lower}.schedule.duration.messages=.*/fault.${fault_lower}.schedule.duration.messages=$duration_msgs/" "$FAULTS_PROPS"
        sed -i "s/fault.${fault_lower}.schedule.iterations=.*/fault.${fault_lower}.schedule.iterations=$iterations/" "$FAULTS_PROPS"
    fi
    
    log_success "Enabled $fault: start=$start_msgs msgs, duration=$duration_msgs msgs, iterations=$iterations"
}

# Disable a specific fault
disable_fault() {
    local fault=$1
    
    validate_fault_type "$fault" || return 1
    
    local fault_lower=$(echo "$fault" | tr '[:upper:]' '[:lower:]')
    local fault_name=$(get_fault_name "$fault")
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/fault.${fault_lower}.schedule.enabled=.*/fault.${fault_lower}.schedule.enabled=false/" "$FAULTS_PROPS"
    else
        sed -i "s/fault.${fault_lower}.schedule.enabled=.*/fault.${fault_lower}.schedule.enabled=false/" "$FAULTS_PROPS"
    fi
    
    log_success "Disabled $fault ($fault_name)"
}

# List all faults and their current configuration
list_faults() {
    log_info "Current Fault Schedule Configuration:"
    echo ""
    
    local faults=(F1 F2 F3 F4 F5 F6)
    for fault in "${faults[@]}"; do
        local fault_lower=$(echo "$fault" | tr '[:upper:]' '[:lower:]')
        local fault_name=$(get_fault_name "$fault")
        
        local enabled=$(grep "fault.${fault_lower}.schedule.enabled=" "$FAULTS_PROPS" | cut -d'=' -f2)
        local start=$(grep "fault.${fault_lower}.schedule.start.messages=" "$FAULTS_PROPS" | cut -d'=' -f2)
        local duration=$(grep "fault.${fault_lower}.schedule.duration.messages=" "$FAULTS_PROPS" | cut -d'=' -f2)
        local iterations=$(grep "fault.${fault_lower}.schedule.iterations=" "$FAULTS_PROPS" | cut -d'=' -f2)
        
        if [ "$enabled" = "true" ]; then
            status="${GREEN}ENABLED${NC}"
        else
            status="${YELLOW}DISABLED${NC}"
        fi
        
        printf "  %-3s %-40s %s\n" "$fault" "$fault_name" "$status"
        printf "      Start: %8s msgs | Duration: %8s msgs | Iterations: %d\n" "$start" "$duration" "$iterations"
        echo ""
    done
}

# Setup multiple faults with a preset scenario
setup_faults() {
    local scenario=$1
    
    case $scenario in
        light)
            log_info "Setting up LIGHT fault scenario..."
            enable_fault F5 10000 5000 1
            disable_fault F1
            disable_fault F2
            disable_fault F3
            disable_fault F4
            disable_fault F6
            ;;
        moderate)
            log_info "Setting up MODERATE fault scenario..."
            enable_fault F3 20000 8000 1
            enable_fault F5 40000 5000 1
            disable_fault F1
            disable_fault F2
            disable_fault F4
            disable_fault F6
            ;;
        heavy)
            log_info "Setting up HEAVY fault scenario..."
            enable_fault F2 15000 5000 2
            enable_fault F3 30000 8000 1
            enable_fault F4 50000 2000 1
            enable_fault F5 60000 10000 2
            disable_fault F1
            disable_fault F6
            ;;
        chaos)
            log_info "Setting up CHAOS fault scenario (all faults enabled)..."
            enable_fault F1 10000 3000 1
            enable_fault F2 20000 3000 1
            enable_fault F3 30000 5000 2
            enable_fault F4 40000 2000 1
            enable_fault F5 50000 4000 2
            enable_fault F6 60000 3000 1
            ;;
        custom)
            log_error "Custom scenario requires explicit fault configuration"
            return 1
            ;;
        *)
            log_error "Unknown scenario: $scenario"
            log_info "Available scenarios: light, moderate, heavy, chaos"
            return 1
            ;;
    esac
    
    log_success "Scenario '$scenario' configured"
    list_faults
}

# Test the schedule configuration by running simulation
test_schedule() {
    local total_msgs=${1:-200000}
    
    log_info "Testing fault schedule configuration with $total_msgs messages..."
    echo ""
    
    local faults=(F1 F2 F3 F4 F5 F6)
    
    for fault in "${faults[@]}"; do
        local fault_lower=$(echo "$fault" | tr '[:upper:]' '[:lower:]')
        local enabled=$(grep "fault.${fault_lower}.schedule.enabled=" "$FAULTS_PROPS" | cut -d'=' -f2)
        
        if [ "$enabled" = "true" ]; then
            local start=$(grep "fault.${fault_lower}.schedule.start.messages=" "$FAULTS_PROPS" | cut -d'=' -f2)
            local duration=$(grep "fault.${fault_lower}.schedule.duration.messages=" "$FAULTS_PROPS" | cut -d'=' -f2)
            local iterations=$(grep "fault.${fault_lower}.schedule.iterations=" "$FAULTS_PROPS" | cut -d'=' -f2)
            
            local fault_name=$(get_fault_name "$fault")
            
            echo -e "${BLUE}$fault - $fault_name${NC}"
            
            for ((iter=1; iter<=iterations; iter++)); do
                local cycle=$((start + duration))
                local window_start=$(((iter-1) * cycle + start))
                local window_end=$(((iter-1) * cycle + start + duration))
                
                if [ $window_end -le $total_msgs ]; then
                    echo "  Iteration $iter: [${window_start}..${window_end}) = $((window_end - window_start)) messages"
                elif [ $window_start -lt $total_msgs ]; then
                    echo "  Iteration $iter: [${window_start}..${total_msgs}) = $((total_msgs - window_start)) messages (partial)"
                else
                    echo "  Iteration $iter: Not reached in test run"
                fi
            done
            echo ""
        fi
    done
    
    log_success "Schedule test completed"
}

# Run consumer with scheduled faults
run_consumer() {
    local num_threads=${1:-1}
    
    log_info "Starting FaultInjectorConsumer with scheduled faults..."
    log_info "Threads: $num_threads"
    log_info "Faults Properties: $FAULTS_PROPS"
    
    # Build and run
    cd "$PROJECT_ROOT/app"
    
    # Compile
    log_info "Building project..."
    mvn clean compile -q 2>/dev/null || {
        log_error "Build failed"
        return 1
    }
    
    # Run consumer
    export MAIN_CLASS="com.kafka.perf.baseline.FaultInjectorConsumer"
    
    log_success "Running consumer..."
    java -cp target/classes:target/dependency/* \
        -Xms512m -Xmx1g \
        $MAIN_CLASS
}

# Monitor fault injection in real-time
monitor_faults() {
    local log_file=${1:-consumer.log}
    
    log_info "Monitoring fault injection..."
    log_info "Watching: $log_file"
    echo ""
    
    # Watch for fault injection messages
    tail -f "$log_file" | grep -E "FaultScheduler|Injecting fault|Starting|Completed"
}

# Display help
show_help() {
    cat << EOF
${BLUE}Fault Scheduling Orchestrator${NC}

Usage: $0 [COMMAND] [OPTIONS]

Commands:
  ${GREEN}setup-faults${NC} <scenario>          Configure faults for a scenario
                                  Scenarios: light, moderate, heavy, chaos
                                  
  ${GREEN}enable-fault${NC} <F#> [start] [duration] [iterations]
                                  Enable specific fault with schedule
                                  Example: enable-fault F5 10000 5000 2
                                  
  ${GREEN}disable-fault${NC} <F#>               Disable specific fault
                                  
  ${GREEN}list-faults${NC}                      List all faults and schedules
                                  
  ${GREEN}test-schedule${NC} [total_msgs]      Test schedule with message count
                                  Default: 200000 messages
                                  
  ${GREEN}run-consumer${NC} [threads]           Run consumer with scheduled faults
                                  Default: 1 thread
                                  
  ${GREEN}monitor-faults${NC} [log_file]       Monitor fault injection in real-time
                                  
  ${GREEN}--help${NC}                           Show this help message

Examples:
  # Setup heavy fault scenario
  $0 setup-faults heavy
  
  # Enable F5 (slow backpressure) after 10k msgs for 5k msgs, 2 iterations
  $0 enable-fault F5 10000 5000 2
  
  # Test schedule with 500k messages
  $0 test-schedule 500000
  
  # Run consumer with scheduled faults
  $0 run-consumer
  
  # Monitor faults in real-time
  $0 monitor-faults app.log

Fault Types:
  F1 - Crash before database commit
  F2 - Crash after database commit before offset ack
  F3 - Partial batch writes
  F4 - Database container restart
  F5 - Slow sink backpressure
  F6 - Network boundary fault

Schedule Format:
  start.messages   - Enable fault after this many messages
  duration.messages - Apply fault for this many messages
  iterations      - Repeat this many times (0 = once)

EOF
}

#################################################################################
# Main
#################################################################################

main() {
    case "${1:-}" in
        setup-faults)
            setup_faults "${2:-light}"
            ;;
        enable-fault)
            enable_fault "${2:-F1}" "${3:-50000}" "${4:-10000}" "${5:-1}"
            ;;
        disable-fault)
            disable_fault "${2:-F1}"
            ;;
        list-faults)
            list_faults
            ;;
        test-schedule)
            test_schedule "${2:-200000}"
            ;;
        run-consumer)
            run_consumer "${2:-1}"
            ;;
        monitor-faults)
            monitor_faults "${2:-consumer.log}"
            ;;
        --help|-h|help)
            show_help
            ;;
        *)
            log_error "Unknown command: ${1:-}"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
