"""
NATS JetStream Utility Functions

Provides utility functions for setting up and managing NATS JetStream streams and consumers.
"""

import logging
from typing import Dict, Any, Optional
import asyncio

# Import nats-py library, using conditional import to avoid mandatory dependency
try:
    from nats.js import JetStreamContext
    from nats.errors import Error as NatsError
    NATS_AVAILABLE = True
except ImportError:
    NATS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Default stream configuration
DEFAULT_STREAM_CONFIG = {
    "name": "oppie.main",
    "subjects": ["oppie.>"],
    "retention": "limits",
    "max_age": 86400,  # 1 day
    "storage": "file",
    "discard": "old",
    "max_bytes": 1073741824,  # 1 GB
    "num_replicas": 1,
    "duplicate_window": 120_000_000_000,  # 2 minutes (nanoseconds)
}

# Default consumer configuration
DEFAULT_CONSUMER_CONFIG = {
    "durable_name": "oppie.consumer",
    "deliver_policy": "all",  # Receive all messages from beginning
    "ack_policy": "explicit",  # Must acknowledge explicitly
    "ack_wait": 60_000_000_000,  # 60 seconds (nanoseconds)
    "max_deliver": -1,  # Unlimited retries
    "filter_subject": "oppie.>",
}

async def ensure_stream(js: JetStreamContext, 
                       config: Dict[str, Any] = None) -> bool:
    """Ensure JetStream stream exists, create if it doesn't exist
    
    Args:
        js: JetStream context
        config: Stream configuration, uses default configuration if None
        
    Returns:
        bool: Whether stream creation/verification was successful
    """
    if not NATS_AVAILABLE:
        logger.error("Cannot create JetStream stream: nats-py library not installed")
        return False
    
    # Use default configuration or merge provided configuration
    stream_config = DEFAULT_STREAM_CONFIG.copy()
    if config:
        stream_config.update(config)
        
    stream_name = stream_config["name"]
    
    try:
        # Check if stream exists
        await js.stream_info(stream_name)
        logger.info(f"Using existing JetStream stream: {stream_name}")
        return True
    except NatsError:
        # Stream doesn't exist, create new stream
        try:
            await js.add_stream(**stream_config)
            logger.info(f"Created new JetStream stream: {stream_name}")
            return True
        except Exception as e:
            logger.error(f"Error occurred while creating JetStream stream: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Error occurred while checking JetStream stream: {str(e)}")
        return False

async def ensure_durable_consumer(js: JetStreamContext,
                                 stream_name: str,
                                 consumer_config: Dict[str, Any] = None) -> Optional[str]:
    """Ensure durable consumer exists, create if it doesn't exist
    
    Args:
        js: JetStream context
        stream_name: Stream name
        consumer_config: Consumer configuration, uses default configuration if None
        
    Returns:
        str: Consumer name, returns None if creation failed
    """
    if not NATS_AVAILABLE:
        logger.error("Cannot create JetStream consumer: nats-py library not installed")
        return None
        
    # Use default configuration or merge provided configuration
    config = DEFAULT_CONSUMER_CONFIG.copy()
    if consumer_config:
        config.update(consumer_config)
    
    durable_name = config["durable_name"]
    
    try:
        # Check if consumer exists
        await js.consumer_info(stream_name, durable_name)
        logger.info(f"Using existing JetStream consumer: {durable_name}")
        return durable_name
    except NatsError:
        # Consumer doesn't exist, create new consumer
        try:
            await js.add_consumer(stream_name, config)
            logger.info(f"Created new JetStream consumer: {durable_name}")
            return durable_name
        except Exception as e:
            logger.error(f"Error occurred while creating JetStream consumer: {str(e)}")
            return None
    except Exception as e:
        logger.error(f"Error occurred while checking JetStream consumer: {str(e)}")
        return None

async def generate_deterministic_msg_id(task_id: str, attempt: int = 0) -> str:
    """Generate deterministic message ID
    
    Uses task ID and attempt count to generate message ID for message deduplication
    
    Args:
        task_id: Task ID
        attempt: Attempt count
        
    Returns:
        str: Message ID
    """
    import hashlib
    
    # Combine string and calculate hash
    combined = f"{task_id}:{attempt}"
    msg_id = hashlib.sha256(combined.encode()).hexdigest()[:32]
    return msg_id

class BackoffTimer:
    """Exponential backoff timer
    
    Provides exponential backoff algorithm with jitter for retry operations
    """
    
    def __init__(self, 
                 initial_ms: float = 100, 
                 max_ms: float = 30000,
                 factor: float = 2.0,
                 jitter: float = 0.2):
        """Initialize backoff timer
        
        Args:
            initial_ms: Initial delay (milliseconds)
            max_ms: Maximum delay (milliseconds)
            factor: Growth factor
            jitter: Jitter ratio (0-1)
        """
        self.initial_ms = initial_ms
        self.max_ms = max_ms
        self.factor = factor
        self.jitter = jitter
        self.attempts = 0
        
    def reset(self):
        """Reset attempt count"""
        self.attempts = 0
        
    def next_delay(self) -> float:
        """Get next delay time (seconds)
        
        Returns:
            float: Delay time (seconds)
        """
        import random
        
        # Calculate exponential backoff delay (milliseconds)
        delay_ms = min(
            self.initial_ms * (self.factor ** self.attempts),
            self.max_ms
        )
        
        # Add jitter
        if self.jitter > 0:
            jitter_amount = delay_ms * self.jitter
            delay_ms = delay_ms - jitter_amount + (random.random() * jitter_amount * 2)
        
        # Increment attempt count
        self.attempts += 1
        
        # Convert to seconds and return
        return delay_ms / 1000.0
        
    async def sleep(self):
        """Sleep for the next delay time"""
        await asyncio.sleep(self.next_delay()) 