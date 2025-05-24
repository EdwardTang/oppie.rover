#!/usr/bin/env python
"""
ZeroMQ benchmark test runner

Run ZeroMQ adapter benchmark tests and verify if <10μs message round-trip latency performance target is met.
"""

import os
import sys
import time
import statistics
from typing import List, Tuple

# Add project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from oppie_xyz.seam_comm.adapters.zeromq.client import ZeroMQClient
from oppie_xyz.seam_comm.adapters.zeromq.server import ZeroMQServer

# Test server addresses (use sufficiently high port numbers to avoid conflicts)
TEST_SERVER_ADDRESS = "tcp://127.0.0.1:35555"
TEST_PUBLISHER_ADDRESS = "tcp://127.0.0.1:35556"

def run_latency_benchmark(iterations: int = 1000, payload_size: int = 100) -> Tuple[float, List[float]]:
    """
    Run latency benchmark test
    
    Args:
        iterations: Number of iterations
        payload_size: Payload size in bytes
        
    Returns:
        Average latency in microseconds and list of all latency values
    """
    print(f"\nRunning latency benchmark - {iterations} iterations, payload size: {payload_size} bytes")
    
    # Start server
    server = None
    client = None
    try:
        server = ZeroMQServer(
            bind_address=TEST_SERVER_ADDRESS,
            publisher_address=TEST_PUBLISHER_ADDRESS
        )
        
        # Register echo method
        server.register_method("echo", lambda params: params)
        
        # Start server
        server.start(threaded=True)
        time.sleep(0.1)  # Wait for server to start
        
        # Create client
        client = ZeroMQClient(server_address=TEST_SERVER_ADDRESS)
        
        # Prepare test payload
        payload = {"data": "X" * payload_size}
        
        # Collect latency data
        latencies = []
        
        # Warmup
        for _ in range(10):
            client.call("echo", payload)
        
        # Run benchmark test
        for i in range(iterations):
            start_time = time.time()
            _ = client.call("echo", payload)
            end_time = time.time()
            
            # Calculate latency in microseconds
            latency_us = (end_time - start_time) * 1_000_000
            latencies.append(latency_us)
            
            if i % 100 == 0 and i > 0:
                print(f"Completed {i} iterations, current average latency: {statistics.mean(latencies[-100:]):.2f} microseconds")
        
        # Calculate statistics
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        
        print("\n=== Latency Statistics (microseconds) ===")
        print(f"Average latency:   {avg_latency:.2f}")
        print(f"Median latency:    {median_latency:.2f}")
        print(f"Minimum latency:   {min_latency:.2f}")
        print(f"Maximum latency:   {max_latency:.2f}")
        print(f"P95 latency:       {p95_latency:.2f}")
        print(f"P99 latency:       {p99_latency:.2f}")
        
        # Check if performance target is met
        if avg_latency < 10:
            print("\n✅ Performance target achieved! Average latency < 10 microseconds")
        else:
            print(f"\n❌ Performance target not met. Average latency: {avg_latency:.2f} microseconds > 10 microseconds")
        
        return avg_latency, latencies
        
    finally:
        # Stop server
        if server:
            server.stop()
        if client:
            client.close()

def run_throughput_benchmark(duration_seconds: int = 5, payload_size: int = 100) -> float:
    """
    Run throughput benchmark test
    
    Args:
        duration_seconds: Test duration in seconds
        payload_size: Payload size in bytes
        
    Returns:
        Messages per second (msgs/sec)
    """
    print(f"\nRunning throughput benchmark - {duration_seconds} seconds, payload size: {payload_size} bytes")
    
    # Start server
    server = ZeroMQServer(
        bind_address=TEST_SERVER_ADDRESS,
        publisher_address=TEST_PUBLISHER_ADDRESS
    )
    
    # Register echo method
    server.register_method("echo", lambda params: params)
    
    # Start server
    server.start(threaded=True)
    time.sleep(0.1)  # Wait for server to start
    
    try:
        # Create client
        client = ZeroMQClient(server_address=TEST_SERVER_ADDRESS)
        
        # Prepare test payload
        payload = {"data": "X" * payload_size}
        
        # Warmup
        for _ in range(10):
            client.call("echo", payload)
        
        # Run throughput test
        message_count = 0
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        while time.time() < end_time:
            client.call("echo", payload)
            message_count += 1
        
        actual_duration = time.time() - start_time
        
        # Calculate throughput
        throughput = message_count / actual_duration
        
        print(f"\nSent {message_count} messages in {actual_duration:.2f} seconds")
        print(f"Throughput: {throughput:.2f} messages/second")
        
        return throughput
        
    finally:
        # Stop server
        if server:
            server.stop()

if __name__ == "__main__":
    # Run benchmark tests
    print("===== ZeroMQ Benchmark Tests =====")
    
    # Run small payload latency test
    avg_latency_small, _ = run_latency_benchmark(iterations=1000, payload_size=100)
    
    # Run medium payload latency test
    avg_latency_medium, _ = run_latency_benchmark(iterations=500, payload_size=1000)
    
    # Run large payload latency test
    avg_latency_large, _ = run_latency_benchmark(iterations=100, payload_size=10000)
    
    # Run throughput test
    throughput = run_throughput_benchmark(duration_seconds=5, payload_size=100)
    
    # Summary report
    print("\n===== Test Summary =====")
    print(f"Small payload latency (100B):  {avg_latency_small:.2f} microseconds")
    print(f"Medium payload latency (1KB):  {avg_latency_medium:.2f} microseconds")
    print(f"Large payload latency (10KB):  {avg_latency_large:.2f} microseconds")
    print(f"Throughput (100B payload):     {throughput:.2f} messages/second")
    
    # Verify performance target
    if min(avg_latency_small, avg_latency_medium, avg_latency_large) < 10:
        print("\n✅ Performance target achieved! At least one payload type has average latency < 10 microseconds")
        sys.exit(0)
    else:
        print("\n❌ All payload types failed to meet performance target (< 10 microseconds)")
        sys.exit(1) 