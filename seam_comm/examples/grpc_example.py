#!/usr/bin/env python
"""
gRPC Adapter Usage Example

Demonstrates how to use gRPC adapter for client-server communication.
"""

import time
import logging
from typing import Dict, Any

from oppie_xyz.seam_comm.adapters.adapter_factory import AdapterFactory, AdapterType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Server address and port
SERVER_ADDRESS = "localhost:50551"

def echo_handler(params: Dict[str, Any]) -> Dict[str, Any]:
    """Echo handler function that returns received parameters as is"""
    logger.info(f"Server received echo request: {params}")
    return params

def process_task_handler(params: Dict[str, Any]) -> Dict[str, Any]:
    """Process task data and return result"""
    task_data = params.get("task", {})
    task_id = task_data.get("id", "unknown")
    logger.info(f"Processing task: {task_id}")
    
    # Simulate task processing
    time.sleep(0.1)
    
    # Return result
    return {
        "task_id": task_id,
        "status": "completed",
        "result": f"Task {task_id} processed"
    }

def handle_event(event: Dict[str, Any]) -> None:
    """Handle received events"""
    event_type = event.get("event")
    event_data = event.get("data", {})
    logger.info(f"Received event {event_type}: {event_data}")

def run_server():
    """Run gRPC server"""
    # Create server
    server = AdapterFactory.create_server(AdapterType.GRPC, {
        "bind_address": SERVER_ADDRESS,
        "max_workers": 10,
        "use_uds": False  # Use TCP on Windows, can use UDS on Linux
    })
    
    # Register methods
    server.register_method("echo", echo_handler)
    server.register_method("process_task", process_task_handler)
    
    # Start server
    logger.info(f"Starting gRPC server, listening on: {SERVER_ADDRESS}")
    server.start(threaded=True)
    
    try:
        # Periodically publish events
        count = 0
        while True:
            count += 1
            event_data = {
                "message": f"Periodic event #{count}",
                "timestamp": time.time()
            }
            logger.info("Publishing event: test_event")
            server.publish_event("test_event", event_data)
            
            # Publish every 5 seconds
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Received interrupt, stopping server...")
    finally:
        server.stop()
        logger.info("Server stopped")

def run_client():
    """Run gRPC client"""
    # Create client
    client = AdapterFactory.create_client(AdapterType.GRPC, {
        "server_address": SERVER_ADDRESS,
        "timeout_ms": 5000,
        "use_uds": False  # Use TCP on Windows, can use UDS on Linux
    })
    
    try:
        # Subscribe to events
        logger.info("Subscribing to events: test_event")
        client.subscribe(["test_event"], handle_event)
        
        # Loop sending requests
        count = 0
        while True:
            count += 1
            
            # Send echo request
            echo_data = {
                "message": f"Hello #{count}",
                "timestamp": time.time()
            }
            logger.info(f"Sending echo request: {echo_data}")
            response = client.call("echo", echo_data)
            logger.info(f"Received echo response: {response}")
            
            # Send task processing request
            task_data = {
                "task": {
                    "id": f"task-{count}",
                    "description": f"Test task #{count}"
                }
            }
            logger.info(f"Sending task processing request: {task_data}")
            response = client.call("process_task", task_data)
            logger.info(f"Received task processing response: {response}")
            
            # Wait 10 seconds
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Received interrupt, closing client...")
    finally:
        client.close()
        logger.info("Client closed")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Please specify run mode: server or client")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == "server":
        run_server()
    elif mode == "client":
        run_client()
    else:
        print(f"Unknown run mode: {mode}")
        print("Please use: server or client")
        sys.exit(1) 