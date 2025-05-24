#!/usr/bin/env python3
"""
NATS JetStream Adapter Recovery Mechanism Benchmark Test

Measures message recovery performance in NATS server restart and client reconnection scenarios:
- Message persistence during server restart
- Recovery speed after client reconnection
- Latency of message acknowledgment and redelivery
"""

import argparse
import asyncio
import json
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import nats
from nats.errors import ConnectionClosedError, TimeoutError
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig

# Import Docker control module (if needed to control Docker containers in tests)
try:
    import docker
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False


@dataclass
class RecoveryConfig:
    """Recovery test configuration"""
    server_url: str = "nats://localhost:4222"
    stream_name: str = "recovery_test"
    subject: str = "recovery.test"
    msg_size: int = 1024  # bytes
    msg_count: int = 1000
    batch_size: int = 100
    ack_wait: int = 30  # seconds
    max_delivery: int = 10
    publish_interval: float = 0.1  # seconds
    output_format: str = "text"  # text, json, csv
    output_file: Optional[str] = None
    # Fault injection configuration
    inject_failure: bool = True
    failure_type: str = "server_restart"  # server_restart, client_disconnect
    failure_delay: int = 5  # seconds before fault occurs
    nats_container: str = "benchmarks_nats_1"  # Docker container name


@dataclass
class RecoveryResult:
    """Recovery test results"""
    config: RecoveryConfig
    start_time: float
    end_time: float
    total_messages: int
    published_before_failure: int
    published_after_failure: int
    received_before_failure: int
    received_after_failure: int
    redelivered_count: int
    duplicate_count: int
    publish_latencies: List[float]  # milliseconds
    recovery_latency: float  # time from fault to recovery (milliseconds)
    reconnect_count: int = 0
    errors: int = 0

    @property
    def duration(self) -> float:
        """Test duration (seconds)"""
        return self.end_time - self.start_time

    @property
    def total_published(self) -> int:
        """Total messages published"""
        return self.published_before_failure + self.published_after_failure

    @property
    def total_received(self) -> int:
        """Total messages received"""
        return self.received_before_failure + self.received_after_failure

    @property
    def recovery_percentage(self) -> float:
        """Percentage of messages recovered"""
        if self.published_before_failure == 0:
            return 0.0
        recovered = min(self.received_after_failure, self.published_before_failure)
        return (recovered / self.published_before_failure) * 100

    @property
    def publish_latency_stats(self) -> Dict[str, float]:
        """Publish latency statistics (milliseconds)"""
        if not self.publish_latencies:
            return {"min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}
        
        sorted_latencies = sorted(self.publish_latencies)
        return {
            "min": min(sorted_latencies),
            "max": max(sorted_latencies),
            "avg": statistics.mean(sorted_latencies),
            "p50": sorted_latencies[int(len(sorted_latencies) * 0.5)],
            "p95": sorted_latencies[int(len(sorted_latencies) * 0.95)],
            "p99": sorted_latencies[int(len(sorted_latencies) * 0.99)]
        }

    def to_dict(self) -> Dict:
        """Convert results to dictionary"""
        return {
            "config": {
                "server_url": self.config.server_url,
                "stream_name": self.config.stream_name,
                "subject": self.config.subject,
                "msg_size": self.config.msg_size,
                "msg_count": self.config.msg_count,
                "failure_type": self.config.failure_type,
                "failure_delay": self.config.failure_delay
            },
            "result": {
                "start_time": self.start_time,
                "end_time": self.end_time,
                "duration_seconds": self.duration,
                "total_messages": self.total_messages,
                "published_before_failure": self.published_before_failure,
                "published_after_failure": self.published_after_failure,
                "received_before_failure": self.received_before_failure,
                "received_after_failure": self.received_after_failure,
                "total_published": self.total_published,
                "total_received": self.total_received,
                "redelivered_count": self.redelivered_count,
                "duplicate_count": self.duplicate_count,
                "recovery_percentage": self.recovery_percentage,
                "recovery_latency_ms": self.recovery_latency,
                "reconnect_count": self.reconnect_count,
                "errors": self.errors,
                "publish_latency_ms": self.publish_latency_stats
            }
        }

    def to_csv_header(self) -> str:
        """Generate CSV header"""
        return "timestamp,server_url,stream_name,subject,msg_size,msg_count," \
               "failure_type,failure_delay,duration_sec,total_messages," \
               "published_before_failure,published_after_failure," \
               "received_before_failure,received_after_failure," \
               "total_published,total_received,redelivered_count,duplicate_count," \
               "recovery_percentage,recovery_latency_ms,reconnect_count,errors," \
               "publish_latency_min_ms,publish_latency_avg_ms,publish_latency_p50_ms," \
               "publish_latency_p95_ms,publish_latency_p99_ms,publish_latency_max_ms"

    def to_csv_row(self) -> str:
        """Generate CSV row"""
        pl = self.publish_latency_stats
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return f"{timestamp},{self.config.server_url},{self.config.stream_name}," \
               f"{self.config.subject},{self.config.msg_size},{self.config.msg_count}," \
               f"{self.config.failure_type},{self.config.failure_delay}," \
               f"{self.duration:.3f},{self.total_messages}," \
               f"{self.published_before_failure},{self.published_after_failure}," \
               f"{self.received_before_failure},{self.received_after_failure}," \
               f"{self.total_published},{self.total_received}," \
               f"{self.redelivered_count},{self.duplicate_count}," \
               f"{self.recovery_percentage:.2f},{self.recovery_latency:.3f}," \
               f"{self.reconnect_count},{self.errors}," \
               f"{pl['min']:.3f},{pl['avg']:.3f},{pl['p50']:.3f}," \
               f"{pl['p95']:.3f},{pl['p99']:.3f},{pl['max']:.3f}"


class RecoveryBenchmark:
    """NATS recovery benchmark test"""
    
    def __init__(self, config: RecoveryConfig):
        self.config = config
        self.result = RecoveryResult(
            config=config,
            start_time=0,
            end_time=0,
            total_messages=config.msg_count,
            published_before_failure=0,
            published_after_failure=0,
            received_before_failure=0,
            received_after_failure=0,
            redelivered_count=0,
            duplicate_count=0,
            publish_latencies=[],
            recovery_latency=0
        )
        
        # State tracking
        self.nc = None
        self.js = None
        self.sub = None
        self.received_msgs = set()
        self.redelivered_msgs = set()
        self.reconnect_count = 0
        self.error_count = 0
        self.failure_injected = False
        self.failure_start_time = 0
        self.recovery_start_time = 0
        self.first_msg_after_failure = 0
        
        # Coordination events
        self.test_complete = asyncio.Event()
        self.publisher_complete = asyncio.Event()
        self.failure_complete = asyncio.Event()
        self.subscriber_ready = asyncio.Event()
    
    def on_reconnect(self):
        """Reconnection callback"""
        print("NATS client reconnected")
        self.reconnect_count += 1
        # If failure was injected but not yet recovered, mark as recovery start
        if self.failure_injected and self.recovery_start_time == 0:
            self.recovery_start_time = time.time()
    
    async def setup_stream(self) -> None:
        """Set up Stream"""
        try:
            # Check if Stream exists
            await self.js.stream_info(self.config.stream_name)
            print(f"Stream '{self.config.stream_name}' already exists")
        except Exception:
            # Create new Stream
            stream_config = StreamConfig(
                name=self.config.stream_name,
                subjects=[f"{self.config.subject}.>"],
                storage="file",  # Use file storage to ensure persistence
                retention="limits",
                max_msgs=1_000_000,
                max_bytes=1_073_741_824,  # 1GB
                discard="old",
                max_age=3600 * 24,  # 1 day
                max_msg_size=self.config.msg_size * 2,
                duplicate_window=120,  # 2 minutes
            )
            await self.js.add_stream(stream_config)
            print(f"Created stream '{self.config.stream_name}'")
    
    async def setup_consumer(self) -> None:
        """Set up Consumer"""
        consumer_name = "recovery-consumer"
        
        try:
            # Check if Consumer exists
            await self.js.consumer_info(self.config.stream_name, consumer_name)
            print(f"Consumer '{consumer_name}' already exists")
        except Exception:
            # Create new Consumer
            consumer_config = ConsumerConfig(
                durable_name=consumer_name,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy="explicit",
                max_deliver=self.config.max_delivery,
                ack_wait=self.config.ack_wait,
                max_ack_pending=self.config.batch_size * 2
            )
            await self.js.add_consumer(self.config.stream_name, consumer_config)
            print(f"Created consumer '{consumer_name}'")
            
        # Create subscription
        self.sub = await self.js.pull_subscribe(
            f"{self.config.subject}.>",
            consumer_name,
            stream=self.config.stream_name
        )
        
        self.subscriber_ready.set()
        print("Subscriber setup complete")
    
    async def message_handler(self, msg) -> None:
        """Handle received messages"""
        try:
            data = json.loads(msg.data.decode())
            msg_id = data.get("id")
            
            # Check if this is the first message after failure
            if self.failure_injected and self.first_msg_after_failure == 0:
                self.first_msg_after_failure = time.time()
                if self.recovery_start_time > 0:
                    # Calculate recovery latency
                    self.result.recovery_latency = (self.first_msg_after_failure - self.recovery_start_time) * 1000
            
            # Check if message was already received
            if msg_id in self.received_msgs:
                self.result.duplicate_count += 1
                print(f"Duplicate message received: {msg_id}")
            else:
                self.received_msgs.add(msg_id)
                
                # Count separately before and after failure
                if not self.failure_injected:
                    self.result.received_before_failure += 1
                else:
                    self.result.received_after_failure += 1
            
            # Check redelivery
            redelivered = msg.metadata and msg.metadata.num_delivered > 1
            if redelivered:
                self.result.redelivered_count += 1
                self.redelivered_msgs.add(msg_id)
                print(f"Redelivered message: {msg_id}, delivery count: {msg.metadata.num_delivered}")
            
            # Acknowledge message
            await msg.ack()
            
        except Exception as e:
            print(f"Error processing message: {e}")
            self.error_count += 1
    
    async def pull_messages_task(self) -> None:
        """Continuous message pulling task"""
        while not self.test_complete.is_set():
            try:
                if self.sub is None:
                    await asyncio.sleep(0.1)
                    continue
                    
                msgs = await self.sub.fetch(batch=self.config.batch_size, timeout=1)
                for msg in msgs:
                    await self.message_handler(msg)
            except TimeoutError:
                # Timeout is normal, continue trying
                pass
            except ConnectionClosedError:
                print("Connection closed while pulling messages, waiting for reconnect...")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"Error in pull messages task: {e}")
                self.error_count += 1
            
            await asyncio.sleep(0.1)
    
    async def publisher(self) -> None:
        """Message publisher"""
        subject = f"{self.config.subject}.1"
        payload = "X" * (self.config.msg_size - 100)  # Reserve space for message ID and timestamp
        
        # Start publishing messages in loop
        for i in range(self.config.msg_count):
            # Check if failure should be injected
            if (not self.failure_injected and 
                self.config.inject_failure and 
                i >= self.config.msg_count // 3):  # Inject failure after sending about 1/3 of messages
                
                # Set failure flag
                self.failure_injected = True
                self.failure_start_time = time.time()
                
                # Record messages published before failure
                self.result.published_before_failure = i
                
                # Inject failure asynchronously
                asyncio.create_task(self.inject_failure())
                
                # Wait for failure completion
                await self.failure_complete.wait()
                print("Resuming publishing after failure...")
            
            # Create message
            msg_id = f"{i}-{uuid.uuid4()}"
            data = {
                "id": msg_id,
                "time": time.time(),
                "seq": i,
                "data": payload
            }
            
            try:
                # Publish message and measure latency
                start_time = time.time()
                
                if self.js is not None:
                    # Use JetStream publishing
                    await self.js.publish(subject, json.dumps(data).encode())
                else:
                    # Use standard NATS publishing (usually during reconnection)
                    await self.nc.publish(subject, json.dumps(data).encode())
                
                # Calculate latency and store
                latency = (time.time() - start_time) * 1000  # Convert to milliseconds
                self.result.publish_latencies.append(latency)
                
                # Count separately before and after failure
                if not self.failure_injected:
                    self.result.published_before_failure += 1
                else:
                    self.result.published_after_failure += 1
                
                # Wait for publish interval
                await asyncio.sleep(self.config.publish_interval)
                
            except ConnectionClosedError:
                print("Connection closed while publishing, waiting for reconnect...")
                await asyncio.sleep(1)
                # Decrease counter to retry current message
                i -= 1
            except Exception as e:
                print(f"Error publishing message: {e}")
                self.error_count += 1
        
        # Mark publisher complete
        self.publisher_complete.set()
        print("Publisher completed")
    
    async def inject_failure(self) -> None:
        """Inject failure"""
        try:
            print(f"Injecting failure: {self.config.failure_type}")
            
            # Wait for configured delay time
            await asyncio.sleep(self.config.failure_delay)
            
            if self.config.failure_type == "server_restart":
                # Restart NATS server
                if DOCKER_AVAILABLE:
                    try:
                        client = docker.from_env()
                        container = client.containers.get(self.config.nats_container)
                        print(f"Restarting NATS container: {self.config.nats_container}")
                        container.restart(timeout=5)
                        print("NATS container restarted")
                    except Exception as e:
                        print(f"Error restarting Docker container: {e}")
                        # If Docker not available, simulate server failure
                        if self.nc and self.nc.is_connected:
                            await self.nc.close()
                            print("Closed NATS connection to simulate server restart")
                else:
                    # If Docker not available, simulate server failure
                    if self.nc and self.nc.is_connected:
                        await self.nc.close()
                        print("Closed NATS connection to simulate server restart")
            
            elif self.config.failure_type == "client_disconnect":
                # Disconnect client connection
                if self.nc and self.nc.is_connected:
                    await self.nc.close()
                    print("Client connection closed for failure simulation")
                    
                    # Reconnect
                    print("Reconnecting after simulated failure...")
                    await self.connect_client()
            
            # Wait for some time to let system recover
            await asyncio.sleep(5)
            
            # Mark failure injection complete
            self.failure_complete.set()
            
        except Exception as e:
            print(f"Error injecting failure: {e}")
            self.error_count += 1
            self.failure_complete.set()
    
    async def connect_client(self) -> None:
        """Connect NATS client"""
        # Connection options
        options = {
            "servers": [self.config.server_url],
            "name": "recovery-benchmark-client",
            "reconnected_cb": self.on_reconnect,
            "max_reconnect_attempts": -1,  # Unlimited retries
            "reconnect_time_wait": 0.5    # Retry interval
        }
        
        try:
            self.nc = await nats.connect(**options)
            self.js = self.nc.jetstream()
            print("Connected to NATS server")
            return True
        except Exception as e:
            print(f"Error connecting to NATS: {e}")
            self.error_count += 1
            return False
    
    async def run(self) -> RecoveryResult:
        """Run recovery benchmark test"""
        print(f"Starting recovery benchmark with config: {self.config}")
        
        try:
            # Connect NATS
            success = await self.connect_client()
            if not success:
                raise Exception("Failed to connect to NATS server")
            
            # Set up Stream and Consumer
            await self.setup_stream()
            await self.setup_consumer()
            
            # Start message pulling task
            pull_task = asyncio.create_task(self.pull_messages_task())
            
            # Wait for subscriber ready
            await self.subscriber_ready.wait()
            
            # Record start time
            self.result.start_time = time.time()
            
            # Start publisher
            _ = asyncio.create_task(self.publisher())
            
            # Wait for publisher completion
            await self.publisher_complete.wait()
            
            # Wait a bit more for any remaining messages
            print("Waiting for remaining messages...")
            await asyncio.sleep(5)
            
            # Mark test complete
            self.test_complete.set()
            
            # Wait for pull task completion
            pull_task.cancel()
            try:
                await pull_task
            except asyncio.CancelledError:
                pass
            
            # Record end time
            self.result.end_time = time.time()
            
            # Update error count
            self.result.errors = self.error_count
            self.result.reconnect_count = self.reconnect_count
            
            print(f"Recovery benchmark completed in {self.result.duration:.2f} seconds")
            
            return self.result
            
        except Exception as e:
            print(f"Error during recovery benchmark: {e}")
            self.result.end_time = time.time()
            self.result.errors = self.error_count + 1
            return self.result
        finally:
            if self.nc:
                await self.nc.close()


class RecoveryTestReporter:
    """Recovery test result reporter"""
    
    def __init__(self, config: RecoveryConfig):
        self.config = config
    
    def print_results(self, result: RecoveryResult) -> None:
        """Print test results to console"""
        print("\n" + "=" * 60)
        print("NATS JetStream Recovery Benchmark Results")
        print("=" * 60)
        
        # Configuration information
        print(f"Server URL: {result.config.server_url}")
        print(f"Stream: {result.config.stream_name}")
        print(f"Subject: {result.config.subject}")
        print(f"Message size: {result.config.msg_size} bytes")
        print(f"Total messages: {result.config.msg_count}")
        print(f"Failure type: {result.config.failure_type}")
        print(f"Failure delay: {result.config.failure_delay} seconds")
        
        print(f"\nTest Duration: {result.duration:.2f} seconds")
        
        # Message statistics
        print("\nMessage Statistics:")
        print(f"  Published before failure: {result.published_before_failure}")
        print(f"  Published after failure: {result.published_after_failure}")
        print(f"  Total published: {result.total_published}")
        print(f"  Received before failure: {result.received_before_failure}")
        print(f"  Received after failure: {result.received_after_failure}")
        print(f"  Total received: {result.total_received}")
        print(f"  Redelivered: {result.redelivered_count}")
        print(f"  Duplicates: {result.duplicate_count}")
        
        # Delivery rate
        delivery_rate = (result.total_received / result.total_published) * 100 if result.total_published > 0 else 0
        print(f"  Delivery rate: {delivery_rate:.2f}%")
        
        # Recovery statistics
        print("\nRecovery Statistics:")
        print(f"  Recovery percentage: {result.recovery_percentage:.2f}%")
        print(f"  Recovery latency: {result.recovery_latency:.2f} ms")
        print(f"  Reconnection count: {result.reconnect_count}")
        print(f"  Errors: {result.errors}")
        
        # Latency statistics
        pl_stats = result.publish_latency_stats
        print("\nPublish Latency (ms):")
        print(f"  Min: {pl_stats['min']:.2f}")
        print(f"  Average: {pl_stats['avg']:.2f}")
        print(f"  Median (P50): {pl_stats['p50']:.2f}")
        print(f"  P95: {pl_stats['p95']:.2f}")
        print(f"  P99: {pl_stats['p99']:.2f}")
        print(f"  Max: {pl_stats['max']:.2f}")
    
    def save_results(self, result: RecoveryResult) -> None:
        """Save test results to file"""
        if not self.config.output_file:
            return
        
        output_path = self.config.output_file
        
        if self.config.output_format == "json":
            with open(output_path, 'w') as f:
                json.dump(result.to_dict(), f, indent=2)
            print(f"Results saved to JSON file: {output_path}")
        
        elif self.config.output_format == "csv":
            import os
            write_header = not os.path.exists(output_path)
            
            with open(output_path, 'a') as f:
                if write_header:
                    f.write(result.to_csv_header() + "\n")
                f.write(result.to_csv_row() + "\n")
            print(f"Results appended to CSV file: {output_path}")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="NATS JetStream Recovery Benchmark")
    
    parser.add_argument("--server-url", default="nats://localhost:4222",
                       help="NATS server URL")
    parser.add_argument("--stream-name", default="recovery_test",
                       help="JetStream stream name")
    parser.add_argument("--subject", default="recovery.test",
                       help="Message subject")
    parser.add_argument("--msg-size", type=int, default=1024,
                       help="Message size in bytes")
    parser.add_argument("--msg-count", type=int, default=1000,
                       help="Number of messages to publish")
    parser.add_argument("--batch-size", type=int, default=100,
                       help="Message batch size")
    parser.add_argument("--publish-interval", type=float, default=0.1,
                       help="Publish interval in seconds")
    parser.add_argument("--failure-type", choices=["server_restart", "client_disconnect"],
                       default="server_restart", help="Failure type")
    parser.add_argument("--failure-delay", type=int, default=5,
                       help="Delay before failure injection (seconds)")
    parser.add_argument("--no-failure", action="store_true",
                       help="Skip failure injection")
    parser.add_argument("--output-format", choices=["text", "json", "csv"],
                       default="text", help="Output format")
    parser.add_argument("--output-file", help="Output file path")
    parser.add_argument("--nats-container", default="benchmarks_nats_1",
                       help="NATS Docker container name")
    
    return parser.parse_args()


async def main() -> None:
    """Main function"""
    args = parse_args()
    
    # Create configuration
    config = RecoveryConfig(
        server_url=args.server_url,
        stream_name=args.stream_name,
        subject=args.subject,
        msg_size=args.msg_size,
        msg_count=args.msg_count,
        batch_size=args.batch_size,
        publish_interval=args.publish_interval,
        inject_failure=not args.no_failure,
        failure_type=args.failure_type,
        failure_delay=args.failure_delay,
        output_format=args.output_format,
        output_file=args.output_file,
        nats_container=args.nats_container
    )
    
    # Create and run benchmark
    benchmark = RecoveryBenchmark(config)
    result = await benchmark.run()
    
    # Report results
    reporter = RecoveryTestReporter(config)
    reporter.print_results(result)
    reporter.save_results(result)


if __name__ == "__main__":
    asyncio.run(main()) 