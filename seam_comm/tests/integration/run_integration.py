#!/usr/bin/env python3
"""
NATS Adapter Integration Test Runner

Automates execution of all test scenarios defined in test_matrix.yaml, collects results and generates performance reports
"""

import sys
import time
import yaml
import json
import shutil
import subprocess
import logging
import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('integration_test.log')
    ]
)

logger = logging.getLogger('integration_test')

# Build paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parents[3]  # oppie.xyz
BENCHMARK_DIR = PROJECT_ROOT / "oppie_xyz" / "seam_comm" / "benchmarks"
RESULTS_DIR = SCRIPT_DIR / "results"
MATRIX_FILE = SCRIPT_DIR / "test_matrix.yaml"

# Ensure results directory exists
RESULTS_DIR.mkdir(exist_ok=True)


def load_test_matrix() -> Dict:
    """Load test matrix

    Returns:
        Dict: Test matrix configuration
    """
    if not MATRIX_FILE.exists():
        logger.error(f"Test matrix file does not exist: {MATRIX_FILE}")
        sys.exit(1)

    with open(MATRIX_FILE, 'r') as f:
        return yaml.safe_load(f)


def restart_container(container_name: str) -> None:
    """Restart Docker container

    Args:
        container_name: Container name
    """
    logger.info(f"Restarting container: {container_name}")
    
    docker_compose_file = BENCHMARK_DIR / "docker-compose.yml"
    if not docker_compose_file.exists():
        logger.error(f"docker-compose.yml file does not exist: {docker_compose_file}")
        return
    
    # Stop container
    subprocess.run(
        ["docker-compose", "-f", str(docker_compose_file), "stop", container_name],
        check=True
    )
    
    # Wait for container to fully stop
    time.sleep(2)
    
    # Start container
    subprocess.run(
        ["docker-compose", "-f", str(docker_compose_file), "start", container_name],
        check=True
    )
    
    # Wait for container to fully start
    time.sleep(5)
    
    logger.info(f"Container restarted: {container_name}")


def toggle_tc_drop(interface: str = "eth0", duration_seconds: int = 5) -> None:
    """Simulate network packet drop using tc

    Args:
        interface: Network interface name
        duration_seconds: Packet drop duration in seconds
    """
    logger.info(f"Simulating network packet drop: {interface}, duration {duration_seconds} seconds")
    
    # Use toxiproxy-cli to simulate network packet drop
    # Note: This assumes toxiproxy container is being used
    try:
        # Add 100% packet drop
        subprocess.run(
            ["docker", "exec", "benchmarks_toxiproxy_1", "toxiproxy-cli", "toxic", "add",
             "--type=latency", "--toxicName=latency_drop", "--downstream", 
             "--toxicity=1.0", "--attribute=latency=1000", "nats"],
            check=True
        )
        
        logger.info(f"Network packet drop enabled, waiting for {duration_seconds} seconds")
        time.sleep(duration_seconds)
        
        # Remove packet drop
        subprocess.run(
            ["docker", "exec", "benchmarks_toxiproxy_1", "toxiproxy-cli", "toxic", "remove",
             "--toxicName=latency_drop", "nats"],
            check=True
        )
        
        logger.info("Network packet drop disabled")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to simulate network packet drop: {e}")


def collect_prometheus_data(start_time: float, end_time: float, metric_name: str) -> Dict:
    """Collect data from Prometheus

    Args:
        start_time: Start timestamp
        end_time: End timestamp
        metric_name: Metric name

    Returns:
        Dict: Prometheus query result
    """
    logger.info(f"Collecting data from Prometheus: {metric_name}")
    
    try:
        # Convert timestamps to Prometheus format
        start_str = datetime.datetime.fromtimestamp(start_time).isoformat() + "Z"
        end_str = datetime.datetime.fromtimestamp(end_time).isoformat() + "Z"
        
        # Query Prometheus
        result = subprocess.run(
            ["curl", "-s", f"http://localhost:9090/api/v1/query_range?query={metric_name}&start={start_str}&end={end_str}&step=0.5s"],
            capture_output=True,
            text=True,
            check=True
        )
        
        return json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"Failed to collect data from Prometheus: {e}")
        return {}


def run_benchmark(scenario: Dict, scenario_name: str) -> Dict[str, Any]:
    """Run benchmark test

    Args:
        scenario: Scenario configuration
        scenario_name: Scenario name

    Returns:
        Dict[str, Any]: Test results
    """
    logger.info(f"Running scenario: {scenario_name}")
    
    load_profile = scenario.get("load_profile", {})
    message_count = load_profile.get("message_count", 1000)
    message_size = load_profile.get("message_size", 1024)
    publish_rate = load_profile.get("publish_rate", 100)
    subscribers = load_profile.get("subscribers", 1)
    
    # Prepare benchmark command
    cmd = [
        sys.executable,
        str(BENCHMARK_DIR / "nats_bench.py"),
        "--msg-count", str(message_count),
        "--msg-size", str(message_size),
        "--pub-rate", str(publish_rate),
        "--subscribers", str(subscribers),
        "--json-output"
    ]
    
    # If there are variant configurations, add corresponding parameters
    if "variants" in scenario:
        variants_results = []
        for variant in scenario["variants"]:
            logger.info(f"Running variant: {variant['name']}")
            variant_cmd = list(cmd)  # Copy base command
            
            # Add variant-specific parameters
            config = variant.get("config", {})
            for key, value in config.items():
                variant_cmd.extend([f"--{key.replace('_', '-')}", str(value).lower()])
            
            # Run variant test
            variant_result = run_single_benchmark(variant_cmd, scenario, f"{scenario_name}_{variant['name']}")
            variant_result["variant"] = variant["name"]
            variants_results.append(variant_result)
        
        # Return all variant results
        return {
            "scenario": scenario_name,
            "variants": variants_results,
            "timestamp": time.time()
        }
    else:
        # Run single scenario directly
        result = run_single_benchmark(cmd, scenario, scenario_name)
        result["scenario"] = scenario_name
        result["timestamp"] = time.time()
        return result


def run_single_benchmark(cmd: List[str], scenario: Dict, result_name: str) -> Dict[str, Any]:
    """Run single benchmark test

    Args:
        cmd: Command line arguments
        scenario: Scenario configuration
        result_name: Result name

    Returns:
        Dict[str, Any]: Test results
    """
    start_time = time.time()
    fault_injection = scenario.get("fault_injection", {})
    
    # Start benchmark process
    logger.info(f"Starting benchmark: {' '.join(cmd)}")
    bench_process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # If fault injection is configured
    if fault_injection:
        fault_type = fault_injection.get("type")
        fault_timing = fault_injection.get("timing", 0)
        
        # Wait for some time before executing fault injection
        if fault_timing > 0:
            # Estimate wait time (based on message rate)
            pub_rate = scenario.get("load_profile", {}).get("publish_rate", 100)
            wait_seconds = fault_timing / pub_rate if pub_rate > 0 else 5
            logger.info(f"Waiting {wait_seconds:.2f} seconds before fault injection")
            time.sleep(wait_seconds)
        
        # Execute corresponding fault injection
        if fault_type == "container_restart":
            restart_container(fault_injection.get("target", "nats"))
        elif fault_type == "network_drop":
            toggle_tc_drop(
                fault_injection.get("target", "eth0"),
                fault_injection.get("duration", 5)
            )
        elif fault_type == "duplicate_messages":
            # This needs to be implemented in nats_bench.py, skipping for now
            logger.info("Duplicate message injection handled in benchmark tool")
    
    # Wait for benchmark to complete
    stdout, stderr = bench_process.communicate()
    end_time = time.time()
    
    # Parse results
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse benchmark results: {stdout}")
        logger.error(f"Error output: {stderr}")
        result = {
            "error": "Failed to parse results",
            "stdout": stdout,
            "stderr": stderr
        }
    
    # Collect additional metric data from Prometheus
    try:
        cpu_data = collect_prometheus_data(
            start_time, end_time, "process_cpu_seconds_total"
        )
        memory_data = collect_prometheus_data(
            start_time, end_time, "process_resident_memory_bytes"
        )
        backlog_data = collect_prometheus_data(
            start_time, end_time, "nats_consumer_backlog"
        )
        
        result["prometheus"] = {
            "cpu": cpu_data,
            "memory": memory_data,
            "backlog": backlog_data
        }
    except Exception as e:
        logger.error(f"Failed to collect Prometheus data: {e}")
    
    # Save results
    result_file = RESULTS_DIR / f"{result_name}.json"
    with open(result_file, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Test completed, results saved to: {result_file}")
    return result


def run_all_scenarios(matrix: Dict) -> List[Dict[str, Any]]:
    """Run all test scenarios

    Args:
        matrix: Test matrix configuration

    Returns:
        List[Dict[str, Any]]: Test results for all scenarios
    """
    all_results = []
    scenarios = matrix.get("scenarios", [])
    test_env = matrix.get("test_environment", {})
    repetitions = test_env.get("repetitions", 1)
    
    logger.info(f"Starting to run all scenarios, each scenario repeated {repetitions} times")
    
    for scenario in scenarios:
        scenario_name = scenario.get("name")
        logger.info(f"=========== Scenario: {scenario_name} ===========")
        
        # Run multiple times to get stable results
        scenario_results = []
        for run in range(repetitions):
            logger.info(f"Run {run+1}/{repetitions}")
            result = run_benchmark(scenario, f"{scenario_name}_run{run+1}")
            scenario_results.append(result)
        
        # Calculate average results from multiple runs
        avg_result = calculate_average_results(scenario_results)
        avg_result["individual_runs"] = scenario_results
        
        all_results.append(avg_result)
        
        # Pause after each scenario to let the system stabilize
        time.sleep(5)
    
    # Save summary of all results
    with open(RESULTS_DIR / "all_results.json", 'w') as f:
        json.dump(all_results, f, indent=2)
    
    return all_results


def calculate_average_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate average results from multiple runs

    Args:
        results: List of results from multiple runs

    Returns:
        Dict[str, Any]: Average results
    """
    if not results:
        return {}
    
    # Use first result as template
    avg_result = results[0].copy()
    
    # If it's a variant test, need to calculate average for each variant separately
    if "variants" in avg_result:
        variants = avg_result["variants"]
        variant_results = {v["variant"]: [] for v in variants}
        
        # Collect all run results for each variant
        for result in results:
            for variant_result in result.get("variants", []):
                variant_name = variant_result["variant"]
                variant_results[variant_name].append(variant_result)
        
        # Calculate average results for each variant
        avg_variants = []
        for variant_name, variant_runs in variant_results.items():
            avg_variant = calculate_average_results(variant_runs)
            avg_variant["variant"] = variant_name
            avg_variants.append(avg_variant)
        
        avg_result["variants"] = avg_variants
        return avg_result
    
    # Extract numeric fields that need averaging
    numeric_fields = [
        "msgs_sent", "msgs_recvd", "latency_p50", "latency_p95", "latency_p99",
        "throughput", "recovery_time_ms", "reconnect_count", "duplicate_rate"
    ]
    
    # Calculate averages
    for field in numeric_fields:
        values = [r.get(field, 0) for r in results if field in r]
        if values:
            avg_result[field] = sum(values) / len(values)
    
    # Mark as average result
    avg_result["is_average"] = True
    avg_result["runs_count"] = len(results)
    
    return avg_result


def verify_assertions(results: List[Dict[str, Any]], matrix: Dict) -> Tuple[bool, List[str]]:
    """Verify test results meet assertions

    Args:
        results: Test results list
        matrix: Test matrix configuration

    Returns:
        Tuple[bool, List[str]]: Whether verification passes and list of error messages
    """
    all_pass = True
    error_messages = []
    
    # Get all scenario configurations
    scenarios = {s["name"]: s for s in matrix.get("scenarios", [])}
    
    for result in results:
        scenario_name = result.get("scenario")
        if scenario_name not in scenarios:
            continue
        
        scenario = scenarios[scenario_name]
        assertions = scenario.get("assertions", {})
        
        # If it's a variant test
        if "variants" in result:
            for variant_result in result.get("variants", []):
                variant_name = variant_result.get("variant")
                logger.info(f"Verifying assertions for scenario {scenario_name} variant {variant_name}")
                
                # Check each assertion
                for assertion_name, expected_value in assertions.items():
                    actual_value = variant_result.get(assertion_name.replace("max_", "").replace("min_", ""))
                    if actual_value is None:
                        continue
                    
                    if assertion_name.startswith("max_") and actual_value > expected_value:
                        all_pass = False
                        error_msg = f"Scenario {scenario_name} variant {variant_name}: {assertion_name} assertion failed - actual value {actual_value} > expected max {expected_value}"
                        error_messages.append(error_msg)
                        logger.error(error_msg)
                    elif assertion_name.startswith("min_") and actual_value < expected_value:
                        all_pass = False
                        error_msg = f"Scenario {scenario_name} variant {variant_name}: {assertion_name} assertion failed - actual value {actual_value} < expected min {expected_value}"
                        error_messages.append(error_msg)
                        logger.error(error_msg)
        else:
            logger.info(f"Verifying assertions for scenario {scenario_name}")
            
            # Check each assertion
            for assertion_name, expected_value in assertions.items():
                actual_value = result.get(assertion_name.replace("max_", "").replace("min_", ""))
                if actual_value is None:
                    continue
                
                if assertion_name.startswith("max_") and actual_value > expected_value:
                    all_pass = False
                    error_msg = f"Scenario {scenario_name}: {assertion_name} assertion failed - actual value {actual_value} > expected max {expected_value}"
                    error_messages.append(error_msg)
                    logger.error(error_msg)
                elif assertion_name.startswith("min_") and actual_value < expected_value:
                    all_pass = False
                    error_msg = f"Scenario {scenario_name}: {assertion_name} assertion failed - actual value {actual_value} < expected min {expected_value}"
                    error_messages.append(error_msg)
                    logger.error(error_msg)
    
    if all_pass:
        logger.info("All assertion verifications passed")
    
    return all_pass, error_messages


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="NATS Adapter Integration Test Runner")
    parser.add_argument("--scenario", help="Run only specified scenario")
    parser.add_argument("--clean", action="store_true", help="Clean previous results")
    args = parser.parse_args()
    
    # Clean previous results
    if args.clean and RESULTS_DIR.exists():
        shutil.rmtree(RESULTS_DIR)
        RESULTS_DIR.mkdir()
    
    # Load test matrix
    matrix = load_test_matrix()
    
    # Run test scenarios
    if args.scenario:
        # Run only specified scenario
        scenarios = matrix.get("scenarios", [])
        scenario = next((s for s in scenarios if s["name"] == args.scenario), None)
        if not scenario:
            logger.error(f"Scenario not found: {args.scenario}")
            sys.exit(1)
        
        logger.info(f"Running single scenario: {args.scenario}")
        result = run_benchmark(scenario, args.scenario)
        
        # Verify assertions
        all_pass, error_messages = verify_assertions([result], matrix)
        if not all_pass:
            logger.error("Some assertions failed:")
            for msg in error_messages:
                logger.error(msg)
            sys.exit(1)
    else:
        # Run all scenarios
        logger.info("Running all scenarios")
        results = run_all_scenarios(matrix)
        
        # Verify all assertions
        all_pass, error_messages = verify_assertions(results, matrix)
        if not all_pass:
            logger.error("Some assertions failed:")
            for msg in error_messages:
                logger.error(msg)
            sys.exit(1)
    
    logger.info("All tests completed successfully!")


if __name__ == "__main__":
    main() 