# Seam Communication (Seam Comm)

Communication and Messaging Infrastructure (Seam) is a core component of the Oppie.xyz project, designed to handle inter-component communication and messaging.

## Features

- Supports unified message formats (TaskCard, ResultCard, ErrorCard, Event)
- Supports multiple communication protocols and adapters (ZeroMQ, gRPC, NATS JetStream)
- Adapters implement standard interfaces for easy switching and management
- Supports cross-protocol request-response and publish-subscribe patterns
- Integrates OpenTelemetry trace context propagation

## Adapter Evolution Path

The Seam component is designed with a three-stage evolution path:

1. **M0 Stage (ZeroMQ)**:
   - Implements intra-process and inter-process communication using ZeroMQ
   - Supports TCP and IPC communication methods
   - Performance target: <10μs message round-trip latency (Linux environment)

2. **M1 Stage (gRPC)**:
   - Implements inter-process communication using gRPC
   - Supports TCP and UDS/Named Pipe communication methods
   - Performance target: <100μs message round-trip latency
   - Enhanced features: type safety, automatic code generation

3. **M2 Stage (NATS JetStream)**:
   - Implements highly resilient distributed communication using NATS JetStream
   - Supports message persistence and complex delivery patterns
   - Supports multi-node deployment and high availability
   - Performance target: <1ms message round-trip latency

## How to Use

### Using ZeroMQ Adapter

```python
from oppie_xyz.seam_comm.adapters.adapter_factory import AdapterFactory, AdapterType

# Create client
client = AdapterFactory.create_client(AdapterType.ZEROMQ, {
    "server_address": "tcp://localhost:5555",
    "timeout_ms": 5000
})

# Send request
response = client.call("echo", {"message": "Hello"})

# Subscribe to events
client.subscribe(["task_completed"], lambda event: print(f"Received event: {event}"))

# Create server
server = AdapterFactory.create_server(AdapterType.ZEROMQ, {
    "bind_address": "tcp://*:5555",
    "publisher_address": "tcp://*:5556"
})

# Register method
server.register_method("echo", lambda params: params)

# Start server
server.start(threaded=True)

# Publish event
server.publish_event("task_completed", {"task_id": "123", "status": "success"})
```

### Using gRPC Adapter

```python
from oppie_xyz.seam_comm.adapters.adapter_factory import AdapterFactory, AdapterType

# Create client
client = AdapterFactory.create_client(AdapterType.GRPC, {
    "server_address": "localhost:50051",
    "timeout_ms": 5000,
    "use_uds": False  # Use TCP on Windows, can set to True for UDS on Linux
})

# Send request
response = client.call("echo", {"message": "Hello"})

# Subscribe to events
client.subscribe(["task_completed"], lambda event: print(f"Received event: {event}"))

# Create server
server = AdapterFactory.create_server(AdapterType.GRPC, {
    "bind_address": "[::]:50051",  # Or use UDS path like "/tmp/oppie.sock"
    "max_workers": 10,
    "use_uds": False
})

# Register method
server.register_method("echo", lambda params: params)

# Start server
server.start(threaded=True)

# Publish event
server.publish_event("task_completed", {"task_id": "123", "status": "success"})
```

### Using NATS JetStream Adapter

```python
from oppie_xyz.seam_comm.adapters.adapter_factory import AdapterFactory, AdapterType

# Create client
client = AdapterFactory.create_client(AdapterType.NATS, {
    "server_address": "nats://localhost:4222",
    "timeout_ms": 5000,
    "stream_name": "oppie_stream",
    "consumer_name": "oppie_consumer"
})

# Send request
response = client.call("echo", {"message": "Hello"})

# Subscribe to events
client.subscribe(["task_completed"], lambda event: print(f"Received event: {event}"))

# Create server
server = AdapterFactory.create_server(AdapterType.NATS, {
    "bind_address": "nats://localhost:4222",
    "stream_name": "oppie_stream",
    "subjects": ["rpc.*", "events.*"],
    "retention_policy": "limits",
    "max_age": 86400  # 1 day
})

# Register method
server.register_method("echo", lambda params: params)

# Start server
server.start(threaded=True)

# Publish event
server.publish_event("task_completed", {"task_id": "123", "status": "success"})
```

## Performance Comparison

**Windows Environment**:
- ZeroMQ Adapter: ~13-16ms (TCP)
- gRPC Adapter: ~1-3ms (TCP)
- NATS JetStream Adapter: To be tested

**Linux Environment**:
- ZeroMQ Adapter: To be tested
- gRPC Adapter: To be tested
- NATS JetStream Adapter: To be tested

*Note: TCP performance is limited in Windows environment, especially for ZeroMQ. For low-latency requirements, it is recommended to use Linux environment and consider UDS/IPC communication methods.*

## Message Types

Seam defines four main message types:

1. **TaskCard**: Describes the task to be executed
2. **ResultCard**: Contains task execution results
3. **ErrorCard**: Describes error conditions
4. **Event**: Events to notify other components

All message types are defined using Protocol Buffers (protobuf), ensuring cross-language compatibility and efficient serialization.

## Advanced Features

### NATS JetStream Features (M2 Stage)

The NATS JetStream adapter, introduced in the M2 stage, provides the following advanced features:

- **Message Persistence**: Supports persisting messages to disk to prevent data loss
- **Message Replay**: Supports replaying message streams from specific positions for system recovery
- **Durable Subscriptions**: Supports durable subscriptions to ensure consumers don't lose messages after restart
- **At-least-once Semantics**: Provides reliable message delivery guarantees
- **Multiple Delivery Strategies**: Supports various message delivery strategies (latest, all, sequence number, etc.)
- **Horizontal Scaling**: Supports cluster deployment and load balancing

**Note**: The current NATS JetStream adapter is in the framework stage. Full functionality requires installing the `nats-py` library and uncommenting the code to work properly. For more details, please refer to `adapters/nats/README.md`. 