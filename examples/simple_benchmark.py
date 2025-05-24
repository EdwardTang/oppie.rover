#!/usr/bin/env python
"""
Simple ZeroMQ Performance Test Script

Used to verify the performance of ZeroMQ adapter and measure round-trip latency for individual requests.
"""

import sys
import os
import time
import statistics
import random
import string
from typing import Dict, Any

# Add project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from oppie_xyz.seam_comm.adapters.zeromq.client import ZeroMQClient
from oppie_xyz.seam_comm.adapters.zeromq.server import ZeroMQServer

# Test server addresses (using random ports to avoid conflicts)
PORT_BASE = 45000 + random.randint(0, 1000)
TEST_SERVER_ADDRESS = f"tcp://127.0.0.1:{PORT_BASE}"
TEST_PUBLISHER_ADDRESS = f"tcp://127.0.0.1:{PORT_BASE+1}"

def generate_payload(size: int) -> Dict[str, Any]:
    """Generate test payload of specified size
    
    Args:
        size: Payload size in bytes
        
    Returns:
        Dictionary containing random string
    """
    # Generate random string
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    return {"data": random_str}

def measure_latency(client: ZeroMQClient, payload: Dict[str, Any]) -> float:
    """Measure latency of a single request
    
    Args:
        client: ZeroMQ client
        payload: Request payload
        
    Returns:
        Latency in microseconds
    """
    start_time = time.time()
    response = client.call("echo", payload)
    end_time = time.time()
    
    # Verify response
    assert "result" in response, "Response should contain result field"
    assert response["result"] == payload, "Response result should match request payload"
    
    # Return microsecond-level latency
    return (end_time - start_time) * 1_000_000

def main():
    """Run simple performance test"""
    print("===== Simple ZeroMQ Performance Test =====")
    print(f"Server address: {TEST_SERVER_ADDRESS}")
    print(f"Publisher address: {TEST_PUBLISHER_ADDRESS}")
    
    # Start server
    server = None
    client = None
    
    try:
        # Create and start server
        print("Starting ZeroMQ server...")
        server = ZeroMQServer(
            bind_address=TEST_SERVER_ADDRESS,
            publisher_address=TEST_PUBLISHER_ADDRESS
        )
        
        # Register echo method
        server.register_method("echo", lambda params: params)
        
        # Start server
        server.start(threaded=True)
        print("Server started, waiting for stabilization...")
        time.sleep(0.5)  # Wait for server to fully start
        
        # Create client
        print("Creating ZeroMQ client...")
        client = ZeroMQClient(server_address=TEST_SERVER_ADDRESS)
        
        # Warmup
        print("Warming up...")
        for _ in range(10):
            client.call("echo", {"warmup": True})
        
        # Prepare test parameters
        iterations = 100  # Run 100 times per payload type
        payload_sizes = [10, 100, 1000, 10000]  # 10B, 100B, 1KB, 10KB
        
        results = {}
        
        # Run tests
        for size in payload_sizes:
            print(f"\nRunning test - Payload size: {size} bytes, {iterations} iterations")
            latencies = []
            
            # Generate fixed-size payload
            payload = generate_payload(size)
            
            # Run multiple iterations to collect latency data
            for i in range(iterations):
                try:
                    latency = measure_latency(client, payload)
                    latencies.append(latency)
                    
                    if (i + 1) % 10 == 0:
                        print(f"Completed {i + 1}/{iterations} iterations")
                        
                except Exception as e:
                    print(f"Iteration {i+1} failed: {str(e)}")
            
            # Calculate statistics
            avg_latency = statistics.mean(latencies)
            median_latency = statistics.median(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
            
            # Display results
            print(f"\n=== {size} byte payload latency statistics (microseconds) ===")
            print(f"Average latency:   {avg_latency:.2f}")
            print(f"Median latency:    {median_latency:.2f}")
            print(f"Minimum latency:   {min_latency:.2f}")
            print(f"Maximum latency:   {max_latency:.2f}")
            print(f"P95 latency:       {p95_latency:.2f}")
            
            # Check if performance target is met
            if min_latency < 10:
                print(f"✅ Performance target achieved! Minimum latency {min_latency:.2f}μs < 10μs")
            else:
                print(f"❌ Performance target not met. Minimum latency: {min_latency:.2f}μs > 10μs")
            
            # Save results
            results[size] = {
                "avg": avg_latency,
                "median": median_latency,
                "min": min_latency,
                "max": max_latency,
                "p95": p95_latency
            }
        
        # Final summary
        print("\n===== Test Summary =====")
        for size, stats in results.items():
            print(f"{size} byte payload: avg={stats['avg']:.2f}μs, min={stats['min']:.2f}μs")
        
        # Verify performance targets
        min_latencies = [stats["min"] for stats in results.values()]
        if min(min_latencies) < 10:
            print("\n✅ Overall performance target achieved! At least one payload type has minimum latency < 10μs")
        else:
            print("\n❌ All payload types failed to meet performance target (< 10μs)")
            
    except Exception as e:
        print(f"Error occurred during test execution: {str(e)}")
        
    finally:
        # Cleanup resources
        print("\nCleaning up resources...")
        if client:
            client.close()
        if server:
            server.stop()
        
        print("Test completed")

if __name__ == "__main__":
    main() 