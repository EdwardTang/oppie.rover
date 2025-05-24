"""
NATS JetStream Adapter

M2 stage communication adapter based on NATS JetStream messaging system.
Provides advanced message queue functionality with support for persistence and message replay.
"""

from oppie_xyz.seam_comm.adapters.nats.client import NatsClient
from oppie_xyz.seam_comm.adapters.nats.server import NatsServer
from oppie_xyz.seam_comm.adapters.nats.utils import (
    ensure_stream,
    ensure_durable_consumer,
    generate_deterministic_msg_id,
    BackoffTimer,
    DEFAULT_STREAM_CONFIG,
    DEFAULT_CONSUMER_CONFIG
)

__all__ = [
    "NatsClient", 
    "NatsServer",
    "ensure_stream",
    "ensure_durable_consumer",
    "generate_deterministic_msg_id",
    "BackoffTimer", 
    "DEFAULT_STREAM_CONFIG",
    "DEFAULT_CONSUMER_CONFIG"
] 