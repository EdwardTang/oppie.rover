"""
Communication Adapters Module

Adapter implementations providing unified interfaces for different communication protocols:
- zeromq: M0 stage ZeroMQ adapter (inproc/IPC)
- grpc: M1 stage gRPC adapter (UDS)
- nats: M2 stage NATS JetStream adapter

All adapters support JSON-RPC 2.0 + SSE semantics and integrate OpenTelemetry trace context injection.
"""

from .adapter_factory import AdapterFactory, AdapterType
from .adapter_interface import ClientAdapterInterface, ServerAdapterInterface

__all__ = [
    "AdapterFactory",
    "AdapterType", 
    "ClientAdapterInterface",
    "ServerAdapterInterface"
] 