#!/usr/bin/env python
"""
ZeroMQ Client Example

Demonstrates how to use ZeroMQClient adapter for JSON-RPC 2.0 requests and SSE subscriptions.
"""

import sys
import os
import time
import logging
import json
from typing import Dict, Any

# Add project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from oppie_xyz.seam_comm.adapters.zeromq.client import ZeroMQClient
from oppie_xyz.seam_comm.telemetry.tracer import setup_tracer, get_current_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import setup_metrics, increment_counter
from oppie_xyz.seam_comm.proto.task_card_pb2 import TaskCard
from oppie_xyz.seam_comm.utils.serialization import protobuf_to_dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def handle_event(event: Dict[str, Any]):
    """
    Handle received events
    
    Args:
        event: Event data
    """
    logger.info(f"Received event: {event['event']}")
    logger.info(f"Event data: {json.dumps(event['data'], indent=2)}")
    increment_counter("events.received", 1, {"event_type": event['event']})

def create_task_card(task_id: str, description: str, steps_count: int = 3) -> TaskCard:
    """
    Create example TaskCard
    
    Args:
        task_id: Task ID
        description: Task description
        steps_count: Number of steps
        
    Returns:
        TaskCard object
    """
    task_card = TaskCard()
    task_card.task_id = task_id
    task_card.goal_description = description
    
    # Add steps
    for i in range(steps_count):
        step = task_card.steps.add()
        step.instruction = f"Execute step {i+1}"
        step.expected_outcome = f"Step {i+1} completed"
    
    # Add context data
    task_card.context_data["env"] = "development"
    task_card.context_data["priority"] = "high"
    
    # Set other fields
    task_card.sem_confidence = 0.95
    task_card.required_capabilities.extend(["file_access"])
    task_card.timeout_seconds = 30
    
    # Set trace context
    trace_context = get_current_trace_context()
    if trace_context:
        task_card.trace_context.CopyFrom(trace_context)
    
    return task_card

def main():
    """Run ZeroMQ client example"""
    # Setup OpenTelemetry
    setup_tracer("zeromq-client-example")
    setup_metrics("zeromq-client-example")
    
    try:
        # Create ZeroMQ client
        client = ZeroMQClient(server_address="tcp://localhost:5555")
        logger.info("ZeroMQ client created and connected to server")
        
        # Subscribe to events
        logger.info("Subscribing to test events...")
        client.subscribe(["test_event"], handle_event)
        
        # Test simple echo request
        logger.info("Sending echo request...")
        response = client.call("echo", {"message": "Hello, ZeroMQ!", "timestamp": time.time()})
        logger.info(f"Received echo response: {response}")
        
        # Test TaskCard processing
        logger.info("Sending TaskCard processing request...")
        task_card = create_task_card("example-task-001", "Example task")
        task_dict = protobuf_to_dict(task_card)
        
        start_time = time.time()
        response = client.call("process_task", {"task_card": task_dict})
        processing_time = time.time() - start_time
        
        logger.info(f"Task processing completed, time taken: {processing_time:.3f} seconds")
        
        if "result" in response and "result_card" in response["result"]:
            result_card = response["result"]["result_card"]
            logger.info(f"Task result: {result_card['status']}")
            logger.info(f"Output summary: {result_card['output_summary']}")
        else:
            logger.error(f"Task processing failed: {response.get('error', 'Unknown error')}")
        
        # Wait for events (if any)
        logger.info("Waiting for events (press Ctrl+C to terminate)...")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received termination signal, exiting client...")
    except Exception as e:
        logger.error(f"Error occurred while running client: {str(e)}")
    
    logger.info("Client exited")

if __name__ == "__main__":
    main() 