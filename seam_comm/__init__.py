"""
Oppie.xyz Communication & Messaging Infrastructure (Seam)

This module provides unified messaging infrastructure for communication between different agents.
Supports multiple communication protocol adapters while using unified message formats and semantics:

1. Message Format: Protobuf 2.0 defined message types (TaskCard, ResultCard, ErrorCard, Event)
2. Communication Semantics: JSON-RPC 2.0 + SSE (Server-Sent Events)
3. Adapters:
   - M0 Stage: ZeroMQ in-process/IPC
   - M1 Stage: gRPC over UDS (Unix Domain Sockets)
   - M2 Stage: NATS JetStream

All adapters support OpenTelemetry trace context injection for cross-protocol request tracing.
"""

__version__ = "0.1.0" 