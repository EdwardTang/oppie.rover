#!/usr/bin/env python
"""
ZeroMQ Server Example

Demonstrates how to use ZeroMQServer adapter to create a JSON-RPC 2.0 server.
"""

import sys
import os
import time
import signal
import logging
from typing import Dict, Any

# Add project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from oppie_xyz.seam_comm.adapters.zeromq.server import ZeroMQServer
from oppie_xyz.seam_comm.telemetry.tracer import setup_tracer, get_current_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import setup_metrics, increment_counter, record_latency
from oppie_xyz.seam_comm.proto.task_card_pb2 import TaskCard
from oppie_xyz.seam_comm.proto.result_card_pb2 import ResultCard
from oppie_xyz.seam_comm.utils.serialization import dict_to_protobuf, protobuf_to_dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def echo_method(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple echo method
    
    Args:
        params: Request parameters
        
    Returns:
        Response with the same parameters as request
    """
    logger.info(f"Received echo request: {params}")
    return params

def process_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Method to process TaskCard
    
    Args:
        params: Request parameters containing task_card
        
    Returns:
        Response containing result_card
    """
    # Extract TaskCard data from parameters
    task_dict = params.get("task_card", {})
    
    # Convert dictionary to TaskCard
    task_card = dict_to_protobuf(task_dict, TaskCard)
    
    logger.info(f"Processing task: {task_card.task_id}, goal: {task_card.goal_description}")
    logger.info(f"Number of steps: {len(task_card.steps)}")
    
    # Record metrics
    increment_counter("tasks.received", 1)
    start_time = time.time()
    
    try:
        # Simulate processing time
        processing_time = min(0.5, 0.01 * len(task_card.steps))
        time.sleep(processing_time)
        
        # Create ResultCard
        result_card = ResultCard()
        result_card.task_id = task_card.task_id
        result_card.status = ResultCard.SUCCESS
        result_card.output_summary = f"Task {task_card.task_id} completed successfully"
        
        # Maintain trace context coherence
        trace_context = get_current_trace_context()
        if trace_context:
            result_card.trace_context.CopyFrom(trace_context)
        
        # Record success metrics
        increment_counter("tasks.success", 1)
        
        # Return ResultCard dictionary
        return {"result_card": protobuf_to_dict(result_card)}
    
    except Exception as e:
        logger.error(f"Error occurred while processing task {task_card.task_id}: {str(e)}")
        
        # Record failure metrics
        increment_counter("tasks.failure", 1)
        
        # Create error ResultCard
        result_card = ResultCard()
        result_card.task_id = task_card.task_id
        result_card.status = ResultCard.FAILURE
        result_card.output_summary = f"Processing failed: {str(e)}"
        
        return {"result_card": protobuf_to_dict(result_card), "error": str(e)}
    
    finally:
        # Record processing time
        processing_time_ms = (time.time() - start_time) * 1000
        record_latency("task.processing_time", processing_time_ms)

def main():
    """Start ZeroMQ server example"""
    # Setup OpenTelemetry
    setup_tracer("zeromq-server-example")
    setup_metrics("zeromq-server-example")
    
    # Create ZeroMQ server
    server = ZeroMQServer(
        bind_address="tcp://*:5555",
        publisher_address="tcp://*:5556"
    )
    
    # Register methods
    server.register_method("echo", echo_method)
    server.register_method("process_task", process_task)
    
    # Add SIGINT handler for graceful exit
    def handle_sigint(sig, frame):
        logger.info("Received exit signal, stopping server...")
        server.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_sigint)
    
    # Start server
    logger.info("Starting ZeroMQ server...")
    
    try:
        # Run server in main thread
        server.start(threaded=False)
    except KeyboardInterrupt:
        logger.info("Received exit signal, stopping server...")
    finally:
        server.stop()
    
    logger.info("Server stopped")

if __name__ == "__main__":
    main() 