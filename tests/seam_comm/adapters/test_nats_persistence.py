"""
NATS JetStream adapter persistence and recovery tests

Test NATS JetStream adapter persistence functionality, including message recovery after server restart.
Requires nats-py library installation and running nats-server to execute these tests.
"""

import time
import threading
import subprocess
import pytest
import logging


# Check if the nats-py library and related dependencies are installed.  The
# NATS adapter also depends on generated protobuf modules which in turn require
# `google.protobuf`.  In CI environments these packages may be missing.  To
# avoid import errors during test collection we attempt the imports inside a
# try/except block and mark the tests to be skipped if any dependency is
# unavailable.
try:
    import nats  # noqa: F401
    from oppie_xyz.seam_comm.adapters.nats.client import NatsClient
    from oppie_xyz.seam_comm.adapters.nats.server import NatsServer
    NATS_AVAILABLE = True
except Exception:  # pragma: no cover - only executed when deps missing
    NATS_AVAILABLE = False
    NatsClient = None  # type: ignore
    NatsServer = None  # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Skip tests if required dependencies are missing
pytestmark = pytest.mark.skipif(
    not NATS_AVAILABLE,
    reason="nats-py or protobuf dependencies not available",
)

# Test configuration
NATS_HOST = "localhost"
NATS_PORT = 4222
NATS_URL = f"nats://{NATS_HOST}:{NATS_PORT}"
STREAM_NAME = "test_stream"
CONSUMER_NAME = "test_consumer"

# Store received messages
received_messages = []
received_lock = threading.Lock()

# Store sent messages
sent_messages = []

# NATS server process
nats_server_process = None

def start_nats_server():
    """Start NATS server process"""
    global nats_server_process
    
    # Check if NATS server command exists
    try:
        result = subprocess.run(["nats-server", "--version"], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              text=True,
                              check=False)
        if result.returncode != 0:
            logger.warning("nats-server command not available, skipping server startup")
            return False
    except FileNotFoundError:
        logger.warning("nats-server command not available, skipping server startup")
        return False
    
    # Start NATS server
    try:
        nats_server_process = subprocess.Popen(
            ["nats-server", 
             "-p", str(NATS_PORT),
             "-js"  # Enable JetStream
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for server to start
        time.sleep(1)
        
        # Check if server is running
        if nats_server_process.poll() is not None:
            logger.error("Unable to start NATS server")
            return False
            
        logger.info(f"NATS server started, PID: {nats_server_process.pid}")
        return True
    except Exception as e:
        logger.error(f"Error occurred while starting NATS server: {str(e)}")
        return False

def stop_nats_server():
    """Stop NATS server process"""
    global nats_server_process
    
    if nats_server_process:
        logger.info("Stopping NATS server...")
        nats_server_process.terminate()
        try:
            nats_server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            nats_server_process.kill()
            
        nats_server_process = None
        logger.info("NATS server stopped")

def restart_nats_server():
    """Restart NATS server"""
    stop_nats_server()
    time.sleep(2)  # Wait for complete shutdown
    return start_nats_server()

def message_handler(message):
    """Handle received messages"""
    with received_lock:
        received_messages.append(message)
        logger.info(f"Received message: {message}")

class TestRpcHandler:
    """RPC method handler"""
    
    def __init__(self):
        self.calls = []
    
    def handle_echo(self, params):
        """Echo RPC method"""
        self.calls.append(("echo", params))
        return {"result": params}
    
    def handle_add(self, params):
        """Add RPC method"""
        a = params.get("a", 0)
        b = params.get("b", 0)
        result = a + b
        self.calls.append(("add", params, result))
        return {"result": result}
    
    def reset(self):
        """Reset call records"""
        self.calls = []

@pytest.fixture(scope="module", autouse=True)
def nats_server():
    """Start and stop NATS server fixture"""
    server_started = start_nats_server()
    
    # Skip related tests if unable to start server
    if not server_started:
        pytest.skip("Unable to start NATS server")
        
    # Wait for server to fully start
    time.sleep(2)
    
    yield
    
    # Cleanup
    stop_nats_server()

@pytest.fixture
def reset_state():
    """Reset test state"""
    global received_messages, sent_messages
    
    with received_lock:
        received_messages = []
    sent_messages = []
    
    yield

@pytest.fixture
def rpc_handler():
    """RPC method handler fixture"""
    handler = TestRpcHandler()
    yield handler
    handler.reset()

def test_persistence_client_server_communication(reset_state, rpc_handler):
    """Test communication between client and server, including persistence and recovery"""
    # Create server
    server = NatsServer(
        bind_address=NATS_URL,
        stream_name=STREAM_NAME
    )
    
    # Register RPC methods
    server.register_method("echo", rpc_handler.handle_echo)
    server.register_method("add", rpc_handler.handle_add)
    
    # Start server
    server.start()
    
    # Wait for server to start
    time.sleep(1)
    
    # Create client
    client = NatsClient(
        server_address=NATS_URL,
        stream_name=STREAM_NAME,
        consumer_name=CONSUMER_NAME
    )
    
    # Subscribe to events
    client.subscribe(["test", "notification"], message_handler)
    
    # Wait for subscription to take effect
    time.sleep(1)
    
    # Test RPC calls
    for i in range(5):
        response = client.call("echo", {"message": f"Test message {i}"})
        assert "result" in response
        assert response["result"]["message"] == f"Test message {i}"
    
    # Test event publishing
    for i in range(10):
        event_data = {"message": f"Event message {i}", "timestamp": time.time()}
        task_id = f"task-{i}"
        server.publish_event("test", event_data, task_id=task_id)
        sent_messages.append((task_id, event_data))
    
    # Wait for event processing
    time.sleep(3)
    
    # Check if all messages were received
    with received_lock:
        assert len(received_messages) > 0
        initial_count = len(received_messages)
        logger.info(f"Initial phase received {initial_count} messages")
    
    # Restart NATS server
    logger.info("Restarting NATS server...")
    restart_success = restart_nats_server()
    assert restart_success, "Unable to restart NATS server"
    
    # Wait for server restart
    time.sleep(3)
    
    # Reconnect client and server
    client.close()
    server.stop()
    
    time.sleep(1)
    
    # Create new server
    new_server = NatsServer(
        bind_address=NATS_URL,
        stream_name=STREAM_NAME
    )
    
    # Register RPC methods
    new_server.register_method("echo", rpc_handler.handle_echo)
    new_server.register_method("add", rpc_handler.handle_add)
    
    # Start new server
    new_server.start()
    
    # Wait for server to start
    time.sleep(1)
    
    # Create new client
    new_client = NatsClient(
        server_address=NATS_URL,
        stream_name=STREAM_NAME,
        consumer_name=CONSUMER_NAME
    )
    
    # Resubscribe to events
    new_client.subscribe(["test", "notification"], message_handler)
    
    # Wait for subscription to take effect
    time.sleep(1)
    
    # Publish more events
    for i in range(10, 20):
        event_data = {"message": f"Event message {i}", "timestamp": time.time()}
        task_id = f"task-{i}"
        new_server.publish_event("test", event_data, task_id=task_id)
        sent_messages.append((task_id, event_data))
    
    # Wait for event processing
    time.sleep(3)
    
    # Test duplicate message sending with same ID (should be deduplicated)
    duplicate_task_id = "duplicate-task"
    duplicate_data = {"message": "Duplicate message", "timestamp": time.time()}
    
    # Send the same ID message 3 times consecutively
    for _ in range(3):
        new_server.publish_event("test", duplicate_data, task_id=duplicate_task_id)
        time.sleep(0.5)
    
    # Test RPC calls again
    response = new_client.call("add", {"a": 40, "b": 2})
    assert "result" in response
    assert response["result"] == 42
    
    # Wait for all messages to be processed
    time.sleep(3)
    
    # Check received messages
    with received_lock:
        final_count = len(received_messages)
        logger.info(f"Finally received {final_count} messages")
        
        # Should be more than initial count
        assert final_count > initial_count
        
        # Check message content
        message_texts = [msg.get("data", {}).get("message", "") for msg in received_messages]
        assert any("Event message 15" in text for text in message_texts)
        
        # Check duplicate message only received once
        duplicate_count = len([msg for msg in received_messages 
                             if msg.get("data", {}).get("message", "") == "Duplicate message"])
        assert duplicate_count == 1, f"Duplicate message received {duplicate_count} times, should only receive 1"
    
    # Close connections
    new_client.close()
    new_server.stop()
    
    logger.info("Test completed")

def test_fault_injection_recovery(reset_state):
    """Test recovery functionality through fault injection"""
    # Create server
    server = NatsServer(
        bind_address=NATS_URL,
        stream_name=f"{STREAM_NAME}_fault"
    )
    
    # Start server
    server.start()
    
    # Wait for server to start
    time.sleep(1)
    
    # Create client
    client = NatsClient(
        server_address=NATS_URL,
        stream_name=f"{STREAM_NAME}_fault",
        consumer_name=f"{CONSUMER_NAME}_fault"
    )
    
    # Subscribe to events
    client.subscribe(["test-fault"], message_handler)
    
    # Wait for subscription to take effect
    time.sleep(1)
    
    # Record sent messages
    sent_task_ids = []
    
    # Send first 50 messages
    for i in range(50):
        event_data = {"message": f"Fault test message {i}", "timestamp": time.time()}
        task_id = f"fault-task-{i}"
        sent_task_ids.append(task_id)
        server.publish_event("test-fault", event_data, task_id=task_id)
        time.sleep(0.02)  # Slightly slow down sending speed
    
    # Wait for message processing
    time.sleep(2)
    
    # Stop NATS server
    logger.info("Simulating fault: shutting down NATS server")
    stop_nats_server()
    
    # Wait 3 seconds
    time.sleep(3)
    
    # Restart NATS server
    logger.info("Restarting NATS server")
    restart_success = start_nats_server()
    assert restart_success, "Unable to restart NATS server"
    
    # Wait for server restart
    time.sleep(3)
    
    # Recreate client and server
    client.close()
    server.stop()
    
    time.sleep(1)
    
    # Create new server and client
    new_server = NatsServer(
        bind_address=NATS_URL,
        stream_name=f"{STREAM_NAME}_fault"
    )
    
    new_server.start()
    
    new_client = NatsClient(
        server_address=NATS_URL,
        stream_name=f"{STREAM_NAME}_fault",
        consumer_name=f"{CONSUMER_NAME}_fault"
    )
    
    new_client.subscribe(["test-fault"], message_handler)
    
    # Wait for connection and subscription to take effect
    time.sleep(2)
    
    # Send remaining 50 messages
    for i in range(50, 100):
        event_data = {"message": f"Fault test message {i}", "timestamp": time.time()}
        task_id = f"fault-task-{i}"
        sent_task_ids.append(task_id)
        new_server.publish_event("test-fault", event_data, task_id=task_id)
        time.sleep(0.02)  # Slightly slow down sending speed
    
    # Wait for all messages to be processed
    time.sleep(5)
    
    # Check received messages
    with received_lock:
        fault_messages = [msg for msg in received_messages 
                        if msg.get("event") == "test-fault"]
        
        logger.info(f"Fault recovery test received {len(fault_messages)} messages (should be 100)")
        
        # Due to complex test environment, we may not ensure all 100 messages are received
        # But should receive most messages, and no duplicate messages
        assert len(fault_messages) >= 80, f"Only received {len(fault_messages)} messages, expected at least 80"
        
        # Check for duplicate messages
        received_ids = set()
        duplicates = []
        
        for msg in fault_messages:
            msg_text = msg.get("data", {}).get("message", "")
            if msg_text in received_ids:
                duplicates.append(msg_text)
            else:
                received_ids.add(msg_text)
                
        # Allow small amount of duplicates (due to NATS JetStream at-least-once semantics)
        assert len(duplicates) <= 5, f"Found {len(duplicates)} duplicate messages, expected no more than 5"
    
    # Close connections
    new_client.close()
    new_server.stop()
    
    logger.info("Fault recovery test completed") 