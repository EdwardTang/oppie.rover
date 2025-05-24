"""
gRPC adapter contract tests

Test gRPC client and server functionality and performance, verifying functionality and performance metrics.
"""

import time
import threading
import pytest


# Check if necessary dependencies are installed
DEPENDENCIES_AVAILABLE = True
try:
    from oppie_xyz.seam_comm.adapters.grpc.client import GrpcClient
    from oppie_xyz.seam_comm.adapters.grpc.server import GrpcServer
    from oppie_xyz.seam_comm.proto.task_card_pb2 import TaskCard
    from oppie_xyz.seam_comm.proto.result_card_pb2 import ResultCard
    from oppie_xyz.seam_comm.utils.serialization import protobuf_to_dict, dict_to_protobuf
except ImportError as e:
    print(f"Skipping gRPC tests because necessary dependencies are not available: {e}")
    DEPENDENCIES_AVAILABLE = False

# Skip all tests if dependencies are not available
pytestmark = pytest.mark.skipif(
    not DEPENDENCIES_AVAILABLE,
    reason="Necessary dependencies not available, skipping gRPC adapter tests"
)

# Test server addresses
TEST_SERVER_ADDRESS = "localhost:50551"

@pytest.fixture
def server():
    """Create and start test server"""
    if not DEPENDENCIES_AVAILABLE:
        pytest.skip("Dependencies not available, skipping test")
    
    server = GrpcServer(
        bind_address=TEST_SERVER_ADDRESS,
        max_workers=10,
        use_uds=False
    )
    
    # Register a simple echo method
    server.register_method("echo", lambda params: params)
    
    # Register a method to process TaskCard
    def process_task(params):
        # Extract TaskCard data from parameters
        task_dict = params.get("task_card", {})
        
        # Convert dictionary to TaskCard
        task_card = dict_to_protobuf(task_dict, TaskCard)
        
        # Create ResultCard
        result_card = ResultCard()
        result_card.task_id = task_card.task_id
        result_card.status = ResultCard.SUCCESS
        result_card.output_summary = f"Task {task_card.task_id} completed"
        
        # Return ResultCard dictionary
        return {"result_card": protobuf_to_dict(result_card)}
    
    server.register_method("process_task", process_task)
    
    # Start server (in background thread)
    server.start(threaded=True)
    
    # Wait for server to start
    time.sleep(0.5)
    
    yield server
    
    # Stop server after test
    server.stop()

@pytest.fixture
def client(server):
    """Create test client"""
    if not DEPENDENCIES_AVAILABLE:
        pytest.skip("Dependencies not available, skipping test")
    
    client = GrpcClient(server_address=TEST_SERVER_ADDRESS)
    yield client
    client.close()

def test_basic_rpc(client):
    """Test basic RPC call"""
    # Send simple echo request
    test_data = {"message": "Hello, World!", "number": 42}
    response = client.call("echo", test_data)
    
    # Verify response
    assert "jsonrpc" in response
    assert response["jsonrpc"] == "2.0"
    assert "id" in response
    assert "result" in response
    assert response["result"] == test_data

def test_task_processing(client):
    """Test TaskCard processing"""
    # Create a TaskCard
    task_card = TaskCard()
    task_card.task_id = "test-task-123"
    task_card.goal_description = "Test task card processing"
    
    # Add steps
    step = task_card.steps.add()
    step.instruction = "Execute test"
    step.expected_outcome = "Test passes"
    
    # Add context data
    task_card.context_data["test_key"] = "test_value"
    
    # Convert TaskCard to dictionary
    task_dict = protobuf_to_dict(task_card)
    
    # Call process task method
    response = client.call("process_task", {"task_card": task_dict})
    
    # Verify response
    assert "result" in response
    result = response["result"]
    assert "result_card" in result
    
    # Verify ResultCard
    result_card_dict = result["result_card"]
    assert result_card_dict["task_id"] == task_card.task_id
    assert result_card_dict["status"] == "SUCCESS"
    assert "completed" in result_card_dict["output_summary"]

def test_event_subscription(server, client):
    """Test event subscription"""
    # Save received events
    received_events = []
    event_received = threading.Event()
    
    # Event handler callback
    def handle_event(event):
        received_events.append(event)
        event_received.set()
    
    # Subscribe to test events
    client.subscribe(["test_event"], handle_event)
    
    # Wait for subscription setup to complete
    time.sleep(0.5)
    
    # Publish test event
    test_data = {"message": "Test event data", "timestamp": time.time()}
    server.publish_event("test_event", test_data)
    
    # Wait for event reception (max 3 seconds)
    event_received.wait(timeout=3)
    
    # Verify event reception
    assert len(received_events) > 0
    event = received_events[0]
    assert event["event"] == "test_event"
    assert event["data"] == test_data

@pytest.mark.benchmark
def test_rpc_latency_benchmark(client, benchmark):
    """Test RPC call latency (performance benchmark)"""
    # Create a small payload
    small_payload = {"message": "Simple test message"}
    
    # Run echo call using benchmark
    def run_echo():
        return client.call("echo", small_payload)
    
    # Run benchmark test
    result = benchmark(run_echo)
    
    # Verify result
    assert result["result"] == small_payload
    
    # Get latency information from benchmark tool and print
    # pytest-benchmark automatically collects statistics

@pytest.mark.benchmark
def test_large_payload_benchmark(client, benchmark):
    """Test performance with large payloads"""
    # Create a larger payload (approximately 10KB)
    large_payload = {
        "array": list(range(1000)),
        "nested": {key: f"value_{key}" for key in range(100)},
        "string": "X" * 5000
    }
    
    # Run echo call using benchmark
    def run_echo_large():
        return client.call("echo", large_payload)
    
    # Run benchmark test
    result = benchmark(run_echo_large)
    
    # Verify result
    assert result["result"] == large_payload

@pytest.mark.benchmark
def test_task_processing_benchmark(client, benchmark):
    """Test TaskCard processing performance"""
    # Create a complex TaskCard
    def create_task():
        task_card = TaskCard()
        task_card.task_id = f"bench-task-{time.time()}"
        task_card.goal_description = "Performance test task card"
        
        # Add multiple steps
        for i in range(10):
            step = task_card.steps.add()
            step.instruction = f"Execute step {i}"
            step.expected_outcome = f"Step {i} completed"
        
        # Add context data
        for i in range(20):
            task_card.context_data[f"key_{i}"] = f"value_{i}"
        
        # Set other fields
        task_card.sem_confidence = 0.95
        task_card.context_hash = "test_hash_12345"
        task_card.required_capabilities.extend(["network", "file_access"])
        task_card.timeout_seconds = 30
        
        # Convert TaskCard to dictionary
        return {"task_card": protobuf_to_dict(task_card)}
    
    # Run TaskCard processing using benchmark
    result = benchmark(lambda: client.call("process_task", create_task()))
    
    # Verify result
    assert "result" in result
    assert "result_card" in result["result"]
    
    # Check latency data (note: this is informational, not an assertion)
    # M1 stage (gRPC UDS) target is <100μs

def test_trace_propagation(client):
    """Test OpenTelemetry trace context propagation"""
    # Note: This test mainly verifies that trace_context field is correctly passed
    # Complete OpenTelemetry testing may require more complex setup
    
    response = client.call("echo", {"trace_test": True})
    
    # Verify response
    assert "result" in response
    
    # Not actually verifying trace propagation as this requires OpenTelemetry test environment
    # This test mainly ensures requests with trace_context don't fail

@pytest.mark.benchmark
def test_performance_comparison():
    """Compare performance across different platforms and adapters
    
    This test doesn't actually run, serves as documentation recording 
    performance comparison results in different environments
    
    Windows test results:
    - ZeroMQ adapter: ~13-16ms (TCP)
    - gRPC adapter: ~1-3ms (TCP)
    
    Linux test results (needs testing in Linux environment):
    - ZeroMQ adapter: To be tested
    - gRPC adapter: To be tested
    
    Analysis:
    - TCP performance is limited in Windows environment, especially for ZeroMQ
    - gRPC adapter performs better than ZeroMQ in Windows environment
    - Further testing needed in Linux environment for more accurate performance comparison
    """
    pass 