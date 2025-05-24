#!/usr/bin/env python3
"""
NATS Adapter Integration Tests

Uses pytest to execute integration tests for NATS adapter, verifying persistence, failure recovery and performance monitoring functionality
"""

import json
import os
import pytest
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

# Get paths
SCRIPT_DIR = Path(__file__).parent.resolve()
RESULTS_DIR = SCRIPT_DIR / "results"

# Skip the entire module unless integration tests are explicitly enabled.  The
# integration suite relies on Docker and a running NATS environment which are
# not available in the default test environment.
if os.environ.get("RUN_INTEGRATION_TESTS") != "1":
    pytest.skip(
        "Integration tests require Docker environment", allow_module_level=True
    )


def load_results(scenario_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load test results

    Args:
        scenario_name: Scenario name, if provided only load results for this scenario

    Returns:
        List[Dict[str, Any]]: List of test results
    """
    if not RESULTS_DIR.exists():
        pytest.skip("No test results found. Run integration tests first.")
    
    # Try to load comprehensive results file
    all_results_file = RESULTS_DIR / "all_results.json"
    if all_results_file.exists():
        with open(all_results_file, 'r') as f:
            results = json.load(f)
            if scenario_name:
                return [r for r in results if r.get("scenario") == scenario_name]
            return results
    
    # If comprehensive results file doesn't exist, try to load individual result files
    if scenario_name:
        result_file = RESULTS_DIR / f"{scenario_name}.json"
        if result_file.exists():
            with open(result_file, 'r') as f:
                return [json.load(f)]
    
    # Load all individual result files
    results = []
    for result_file in RESULTS_DIR.glob("*.json"):
        if result_file.name != "all_results.json":
            with open(result_file, 'r') as f:
                results.append(json.load(f))
    
    return results


def run_integration_tests():
    """Run integration tests

    Use run_integration.py script to run all test scenarios
    """
    integration_script = SCRIPT_DIR / "run_integration.py"
    if not integration_script.exists():
        pytest.skip("Integration test script not found.")
    
    print("Running NATS adapter integration tests...")
    result = subprocess.run(
        ["python", str(integration_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print(f"Errors: {result.stderr}")
    
    assert result.returncode == 0, f"Integration tests failed with code {result.returncode}"


@pytest.fixture(scope="module")
def test_results():
    """Get test results"""
    return load_results()


@pytest.mark.integration
def test_run_all_integration_tests():
    """Run all integration tests"""
    run_integration_tests()


@pytest.mark.integration
def test_baseline_pubsub(test_results):
    """Test baseline publish/subscribe scenario"""
    results = [r for r in test_results if r.get("scenario") == "baseline_pubsub"]
    if not results:
        pytest.skip("No results for baseline_pubsub scenario")
    
    result = results[0]
    
    # Verify message delivery guarantee
    assert result.get("msgs_recvd") / result.get("msgs_sent", 1) >= 0.99, "Message delivery guarantee not met"
    
    # Verify latency
    assert result.get("latency_p50", 1000) <= 5.0, "Median latency exceeds threshold"
    assert result.get("latency_p95", 1000) <= 20.0, "95th percentile latency exceeds threshold"
    assert result.get("latency_p99", 1000) <= 50.0, "99th percentile latency exceeds threshold"


@pytest.mark.integration
def test_high_load_saturation(test_results):
    """Test high load saturation scenario"""
    results = [r for r in test_results if r.get("scenario") == "high_load_saturation"]
    if not results:
        pytest.skip("No results for high_load_saturation scenario")
    
    result = results[0]
    
    # Verify message delivery guarantee
    assert result.get("msgs_recvd") / result.get("msgs_sent", 1) >= 0.99, "Message delivery guarantee not met"
    
    # Verify latency
    assert result.get("latency_p50", 1000) <= 50.0, "Median latency exceeds threshold"
    assert result.get("latency_p95", 1000) <= 200.0, "95th percentile latency exceeds threshold"
    assert result.get("latency_p99", 1000) <= 500.0, "99th percentile latency exceeds threshold"
    
    # Verify backlog
    prometheus_data = result.get("prometheus", {}).get("backlog", {})
    backlog_values = []
    for result in prometheus_data.get("data", {}).get("result", []):
        for value in result.get("values", []):
            if len(value) >= 2:
                backlog_values.append(float(value[1]))
    
    if backlog_values:
        max_backlog = max(backlog_values)
        assert max_backlog <= 10000, f"Max backlog ({max_backlog}) exceeds threshold"


@pytest.mark.integration
def test_broker_restart(test_results):
    """Test broker restart scenario"""
    results = [r for r in test_results if r.get("scenario") == "broker_restart"]
    if not results:
        pytest.skip("No results for broker_restart scenario")
    
    result = results[0]
    
    # Verify message delivery guarantee
    assert result.get("msgs_recvd") / result.get("msgs_sent", 1) >= 0.99, "Message delivery guarantee not met during broker restart"
    
    # Verify recovery time
    assert result.get("recovery_time_ms", 10000) <= 2000, "Recovery time exceeds threshold"
    
    # Verify successful reconnection
    assert result.get("reconnect_count", 0) >= 1, "No reconnections detected"
    assert result.get("reconnect_successful", False), "Reconnection was not successful"


@pytest.mark.integration
def test_client_network_partition(test_results):
    """Test client network partition scenario"""
    results = [r for r in test_results if r.get("scenario") == "client_network_partition"]
    if not results:
        pytest.skip("No results for client_network_partition scenario")
    
    result = results[0]
    
    # Verify message delivery guarantee
    assert result.get("msgs_recvd") / result.get("msgs_sent", 1) >= 0.99, "Message delivery guarantee not met during network partition"
    
    # Verify recovery time
    assert result.get("recovery_time_ms", 10000) <= 3000, "Recovery time exceeds threshold"
    
    # Verify successful reconnection
    assert result.get("reconnect_count", 0) >= 1, "No reconnections detected"
    assert result.get("reconnect_successful", False), "Reconnection was not successful"


@pytest.mark.integration
def test_duplicate_message_id(test_results):
    """Test duplicate message ID scenario"""
    results = [r for r in test_results if r.get("scenario") == "duplicate_message_id"]
    if not results:
        pytest.skip("No results for duplicate_message_id scenario")
    
    result = results[0]
    
    # Verify no duplicate message processing
    assert result.get("duplicate_rate", 1.0) == 0.0, "Duplicate messages were processed"


@pytest.mark.integration
def test_compression_batch_ack_comparison(test_results):
    """Test compression and batch acknowledgment comparison scenario"""
    results = [r for r in test_results if r.get("scenario") == "compression_batch_ack_comparison"]
    if not results:
        pytest.skip("No results for compression_batch_ack_comparison scenario")
    
    result = results[0]
    variants = result.get("variants", [])
    
    # Verify at least two variants
    assert len(variants) >= 2, "Not enough variants to compare"
    
    # Get variants with compression enabled
    compression_variants = [v for v in variants if "compression" in v.get("variant") and "compression_" in v.get("variant")]
    
    # Get variants with batch acknowledgment enabled
    batch_variants = [v for v in variants if "batch" in v.get("variant") and "_batch" in v.get("variant")]
    
    if compression_variants:
        # Verify compression ratio
        for variant in compression_variants:
            compression_ratio = variant.get("compression_ratio", 0.0)
            assert compression_ratio >= 0.3, f"Compression ratio ({compression_ratio}) below threshold for {variant.get('variant')}"
    
    if batch_variants:
        # Verify batch efficiency
        for variant in batch_variants:
            batch_efficiency = variant.get("batch_efficiency", 0.0)
            assert batch_efficiency >= 0.5, f"Batch efficiency ({batch_efficiency}) below threshold for {variant.get('variant')}"


@pytest.mark.integration
def test_persistence_comparison(test_results):
    """Test persistence comparison scenario"""
    results = [r for r in test_results if r.get("scenario") == "persistence_comparison"]
    if not results:
        pytest.skip("No results for persistence_comparison scenario")
    
    result = results[0]
    variants = result.get("variants", [])
    
    # Verify exactly two variants
    assert len(variants) == 2, "Expected exactly 2 variants for persistence comparison"
    
    # Get variants with persistence enabled and disabled
    persistence_enabled = next((v for v in variants if "persistence_enabled" in v.get("variant")), None)
    persistence_disabled = next((v for v in variants if "persistence_disabled" in v.get("variant")), None)
    
    if persistence_enabled and persistence_disabled:
        # Calculate latency difference
        latency_diff_p50 = persistence_enabled.get("latency_p50", 0) - persistence_disabled.get("latency_p50", 0)
        assert latency_diff_p50 <= 5.0, f"Median latency difference ({latency_diff_p50}ms) exceeds threshold"
        
        # Calculate throughput decrease percentage
        throughput_enabled = persistence_enabled.get("throughput", 0)
        throughput_disabled = persistence_disabled.get("throughput", 0)
        if throughput_disabled > 0:
            throughput_decrease = (throughput_disabled - throughput_enabled) / throughput_disabled
            assert throughput_decrease <= 0.2, f"Throughput decrease ({throughput_decrease:.2%}) exceeds threshold"


if __name__ == "__main__":
    # Run all tests when executed directly
    run_integration_tests() 