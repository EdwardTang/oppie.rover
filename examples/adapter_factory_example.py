#!/usr/bin/env python
"""
Adapter Factory Example

Demonstrates how to use adapter factory and different types of adapters while maintaining a unified API calling style.
"""

import sys
import os
import time
import logging
from typing import Dict, Any

# Add project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from oppie_xyz.seam_comm.adapters.adapter_factory import AdapterFactory, AdapterType

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def run_echo_test(adapter_type: str, config: Dict[str, Any] = None):
    """Run echo test using specified adapter
    
    Args:
        adapter_type: Adapter type, such as "zeromq", "grpc"
        config: Adapter configuration
    """
    print(f"\n=== Running test with {adapter_type} adapter ===")
    
    server = None
    client = None
    
    try:
        # Create server using adapter factory
        print(f"Creating {adapter_type} server...")
        server = AdapterFactory.create_server(adapter_type, config)
        
        # Register echo method
        server.register_method("echo", lambda params: params)
        
        # Start server
        server.start(threaded=True)
        print("Server started, waiting for stabilization...")
        time.sleep(0.5)  # Wait for server to fully start
        
        # Create client using adapter factory
        print(f"Creating {adapter_type} client...")
        client = AdapterFactory.create_client(adapter_type, config)
        
        # Execute RPC call
        test_data = {"message": "Hello, World!", "timestamp": time.time()}
        print(f"Sending request: {test_data}")
        
        start_time = time.time()
        response = client.call("echo", test_data)
        end_time = time.time()
        
        # Calculate latency
        latency_ms = (end_time - start_time) * 1000
        
        # Verify response
        if "result" in response and response["result"] == test_data:
            print(f"✅ Test successful! Response matches. Latency: {latency_ms:.2f}ms")
        else:
            print("❌ Test failed! Response mismatch:")
            print(f"  Request: {test_data}")
            print(f"  Response: {response}")
        
    except NotImplementedError as e:
        print(f"⚠️ {adapter_type} adapter not yet implemented: {str(e)}")
        
    except Exception as e:
        print(f"❌ Test error: {str(e)}")
        
    finally:
        # Cleanup resources
        if client:
            client.close()
        if server:
            server.stop()
            
        print(f"=== {adapter_type} test completed ===")

def main():
    """Run main test flow"""
    setup_logging()
    
    print("===== Adapter Factory Example =====")
    print("This example demonstrates how to use adapter factory and different types of adapters while maintaining a unified API calling style.")
    
    # Run ZeroMQ test
    zeromq_config = {
        "server_address": "tcp://localhost:45678",
        "bind_address": "tcp://*:45678",
        "publisher_address": "tcp://*:45679"
    }
    run_echo_test(AdapterType.ZEROMQ, zeromq_config)
    
    # Run gRPC test
    grpc_config = {
        "server_address": "localhost:50051",
        "bind_address": "[::]:50051"
    }
    run_echo_test(AdapterType.GRPC, grpc_config)
    
    print("\nAll tests completed.")

if __name__ == "__main__":
    main() 