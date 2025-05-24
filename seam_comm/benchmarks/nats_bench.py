#!/usr/bin/env python3
"""
NATS JetStream适配器性能基准测试

测量发布/订阅场景下的延迟和吞吐量。支持参数化配置：
- 客户端数量
- 消息大小
- 消息数量
- 持久化/非持久化模式
"""

import argparse
import asyncio
import json
import os
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import nats
from nats.errors import TimeoutError
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig


@dataclass
class BenchmarkConfig:
    """基准测试配置"""
    server_url: str = "nats://localhost:4222"
    stream_name: str = "benchmark"
    subject: str = "benchmark.test"
    client_count: int = 1
    publisher_count: int = 1
    subscriber_count: int = 1
    msg_size: int = 1024  # 字节
    msg_count: int = 10000
    durable: bool = True
    jetstream: bool = True
    output_format: str = "text"  # text, json, csv
    output_file: Optional[str] = None
    consumer_name: str = "benchmark-consumer"
    async_publish: bool = False
    ack_wait: int = 30  # 秒
    max_delivery: int = 10
    batch_size: int = 100


@dataclass
class BenchmarkResult:
    """基准测试结果"""
    config: BenchmarkConfig
    start_time: float
    end_time: float
    total_messages: int
    published_messages: int
    received_messages: int
    publish_latencies: List[float]  # 毫秒
    e2e_latencies: List[float]  # 毫秒
    errors: int = 0
    reconnects: int = 0

    @property
    def duration(self) -> float:
        """测试持续时间（秒）"""
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        """吞吐量（消息/秒）"""
        return self.published_messages / self.duration if self.duration > 0 else 0

    @property
    def publish_latency_stats(self) -> Dict[str, float]:
        """发布延迟统计（毫秒）"""
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

    @property
    def e2e_latency_stats(self) -> Dict[str, float]:
        """端到端延迟统计（毫秒）"""
        if not self.e2e_latencies:
            return {"min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}
        
        sorted_latencies = sorted(self.e2e_latencies)
        return {
            "min": min(sorted_latencies),
            "max": max(sorted_latencies),
            "avg": statistics.mean(sorted_latencies),
            "p50": sorted_latencies[int(len(sorted_latencies) * 0.5)],
            "p95": sorted_latencies[int(len(sorted_latencies) * 0.95)],
            "p99": sorted_latencies[int(len(sorted_latencies) * 0.99)]
        }

    def to_dict(self) -> Dict:
        """将结果转换为字典"""
        return {
            "config": {
                "server_url": self.config.server_url,
                "client_count": self.config.client_count,
                "publisher_count": self.config.publisher_count,
                "subscriber_count": self.config.subscriber_count,
                "msg_size": self.config.msg_size,
                "msg_count": self.config.msg_count,
                "durable": self.config.durable,
                "jetstream": self.config.jetstream,
                "async_publish": self.config.async_publish,
                "batch_size": self.config.batch_size
            },
            "result": {
                "start_time": self.start_time,
                "end_time": self.end_time,
                "duration_seconds": self.duration,
                "total_messages": self.total_messages,
                "published_messages": self.published_messages,
                "received_messages": self.received_messages,
                "throughput_msgs_per_sec": self.throughput,
                "publish_latency_ms": self.publish_latency_stats,
                "e2e_latency_ms": self.e2e_latency_stats,
                "errors": self.errors,
                "reconnects": self.reconnects
            }
        }

    def to_csv_header(self) -> str:
        """生成CSV头"""
        return "timestamp,server_url,client_count,publisher_count,subscriber_count," \
               "msg_size,msg_count,durable,jetstream,async_publish,batch_size," \
               "duration_sec,total_messages,published_messages,received_messages," \
               "throughput_msgs_per_sec," \
               "publish_latency_min_ms,publish_latency_avg_ms,publish_latency_p50_ms," \
               "publish_latency_p95_ms,publish_latency_p99_ms,publish_latency_max_ms," \
               "e2e_latency_min_ms,e2e_latency_avg_ms,e2e_latency_p50_ms," \
               "e2e_latency_p95_ms,e2e_latency_p99_ms,e2e_latency_max_ms," \
               "errors,reconnects"

    def to_csv_row(self) -> str:
        """生成CSV行"""
        pl = self.publish_latency_stats
        el = self.e2e_latency_stats
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return f"{timestamp},{self.config.server_url},{self.config.client_count}," \
               f"{self.config.publisher_count},{self.config.subscriber_count}," \
               f"{self.config.msg_size},{self.config.msg_count},{self.config.durable}," \
               f"{self.config.jetstream},{self.config.async_publish},{self.config.batch_size}," \
               f"{self.duration:.3f},{self.total_messages},{self.published_messages}," \
               f"{self.received_messages},{self.throughput:.3f}," \
               f"{pl['min']:.3f},{pl['avg']:.3f},{pl['p50']:.3f},{pl['p95']:.3f}," \
               f"{pl['p99']:.3f},{pl['max']:.3f}," \
               f"{el['min']:.3f},{el['avg']:.3f},{el['p50']:.3f},{el['p95']:.3f}," \
               f"{el['p99']:.3f},{el['max']:.3f}," \
               f"{self.errors},{self.reconnects}"


class NatsBenchmark:
    """NATS基准测试"""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.result = BenchmarkResult(
            config=config,
            start_time=0,
            end_time=0,
            total_messages=config.msg_count * config.publisher_count,
            published_messages=0,
            received_messages=0,
            publish_latencies=[],
            e2e_latencies=[]
        )
        self.clients = []
        self.publish_times = {}  # 用于测量端到端延迟
        self.received_ids = set()
        self.reconnect_count = 0
        self.received_count = 0
        self.published_count = 0
        self.error_count = 0
        
        # 用于协调测试
        self.all_received = asyncio.Event()
        self.all_published = asyncio.Event()
        self.test_complete = asyncio.Event()
    
    async def setup_stream(self, js: JetStreamContext) -> None:
        """设置Stream"""
        try:
            # 检查Stream是否存在
            await js.stream_info(self.config.stream_name)
            print(f"Stream '{self.config.stream_name}' already exists")
        except nats.js.api.ApiError:
            # 创建新的Stream
            stream_config = StreamConfig(
                name=self.config.stream_name,
                subjects=[f"{self.config.subject}.>"],
                storage="file" if self.config.durable else "memory",
                retention="limits",
                max_msgs=1_000_000,
                max_bytes=1_073_741_824,  # 1GB
                discard="old",
                max_age=3600 * 24,  # 1天
                max_msg_size=self.config.msg_size * 2,
                duplicate_window=120,  # 2分钟
            )
            await js.add_stream(stream_config)
            print(f"Created stream '{self.config.stream_name}'")
    
    async def setup_consumer(self, js: JetStreamContext) -> None:
        """设置Consumer"""
        if not self.config.durable:
            return
        
        try:
            # 检查Consumer是否存在
            await js.consumer_info(self.config.stream_name, self.config.consumer_name)
            print(f"Consumer '{self.config.consumer_name}' already exists")
        except nats.js.api.ApiError:
            # 创建新的Consumer
            consumer_config = ConsumerConfig(
                durable_name=self.config.consumer_name,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy="explicit",
                max_deliver=self.config.max_delivery,
                ack_wait=self.config.ack_wait,
                max_ack_pending=self.config.batch_size * 2
            )
            await js.add_consumer(self.config.stream_name, consumer_config)
            print(f"Created consumer '{self.config.consumer_name}'")
    
    async def message_handler(self, msg) -> None:
        """处理接收到的消息"""
        try:
            data = json.loads(msg.data.decode())
            msg_id = data.get("id")
            sent_time = data.get("time")
            
            # 防止重复计数
            if msg_id in self.received_ids:
                return
            
            self.received_ids.add(msg_id)
            self.received_count += 1
            
            # 计算端到端延迟
            if sent_time:
                e2e_latency = (time.time() - sent_time) * 1000  # 转换为毫秒
                self.result.e2e_latencies.append(e2e_latency)
            
            # 如果使用的是JetStream，确认消息
            if hasattr(msg, "ack") and callable(msg.ack):
                await msg.ack()
            
            # 检查是否收到所有消息
            if self.received_count >= self.result.total_messages:
                if not self.all_received.is_set():
                    self.all_received.set()
        
        except Exception as e:
            print(f"Error processing message: {e}")
            self.error_count += 1
    
    async def publisher(self, nc: nats.NATS, js: Optional[JetStreamContext], 
                        publisher_id: int) -> None:
        """消息发布者"""
        subject_base = self.config.subject
        payload = "X" * (self.config.msg_size - 100)  # 预留一些空间给消息ID和时间戳
        
        for i in range(self.config.msg_count):
            msg_id = f"{publisher_id}-{i}-{uuid.uuid4()}"
            data = {
                "id": msg_id,
                "time": time.time(),
                "publisher": publisher_id,
                "seq": i,
                "data": payload
            }
            
            subject = f"{subject_base}.{publisher_id}"
            start_time = time.time()
            
            try:
                if self.config.jetstream:
                    if self.config.async_publish:
                        # 异步发布
                        await js.publish_async(subject, json.dumps(data).encode())
                    else:
                        # 同步发布，等待确认
                        await js.publish(subject, json.dumps(data).encode())
                else:
                    # 标准NATS发布
                    await nc.publish(subject, json.dumps(data).encode())
                
                # 计算发布延迟
                publish_latency = (time.time() - start_time) * 1000  # 转换为毫秒
                self.result.publish_latencies.append(publish_latency)
                
                self.published_count += 1
                self.publish_times[msg_id] = start_time
                
                # 定期睡眠，避免过载
                if i > 0 and i % 1000 == 0:
                    await asyncio.sleep(0.001)
            
            except Exception as e:
                print(f"Error publishing message: {e}")
                self.error_count += 1
        
        # 标记该发布者已完成
        print(f"Publisher {publisher_id} completed publishing {self.config.msg_count} messages")
    
    async def subscriber(self, nc: nats.NATS, js: Optional[JetStreamContext], 
                         subscriber_id: int) -> None:
        """消息订阅者"""
        subject_filter = f"{self.config.subject}.>"
        
        if self.config.jetstream and self.config.durable:
            # 使用JetStream持久化订阅
            sub = await js.pull_subscribe(
                subject_filter, 
                self.config.consumer_name,
                stream=self.config.stream_name
            )
            
            # 使用任务持续拉取消息
            async def pull_messages():
                while not self.test_complete.is_set():
                    try:
                        msgs = await sub.fetch(batch=self.config.batch_size, timeout=1)
                        for msg in msgs:
                            await self.message_handler(msg)
                    except TimeoutError:
                        # 超时是正常的，继续尝试
                        pass
                    except Exception as e:
                        print(f"Error in pull subscriber: {e}")
                        self.error_count += 1
                    
                    await asyncio.sleep(0.01)
            
            asyncio.create_task(pull_messages())
        else:
            # 标准NATS订阅
            await nc.subscribe(subject_filter, cb=self.message_handler)
        
        print(f"Subscriber {subscriber_id} started")
    
    async def connect_client(self, client_id: int) -> Tuple[nats.NATS, Optional[JetStreamContext]]:
        """连接NATS客户端"""
        # 连接选项
        options = {
            "servers": [self.config.server_url],
            "name": f"benchmark-client-{client_id}",
            "reconnected_cb": self.on_reconnect
        }
        
        nc = await nats.connect(**options)
        js = None
        
        if self.config.jetstream:
            js = nc.jetstream()
        
        self.clients.append(nc)
        return nc, js
    
    def on_reconnect(self):
        """重连回调"""
        self.reconnect_count += 1
    
    async def run(self) -> BenchmarkResult:
        """运行基准测试"""
        print(f"Starting benchmark with config: {self.config}")
        
        try:
            # 连接首个客户端并设置Stream和Consumer
            nc, js = await self.connect_client(0)
            
            if self.config.jetstream:
                await self.setup_stream(js)
                if self.config.durable:
                    await self.setup_consumer(js)
            
            # 启动订阅者
            subscribers = []
            for i in range(self.config.subscriber_count):
                client_id = i + 1  # 从1开始，0已被用于设置
                nc_sub, js_sub = await self.connect_client(client_id)
                sub_task = asyncio.create_task(
                    self.subscriber(nc_sub, js_sub, i)
                )
                subscribers.append(sub_task)
            
            # 给订阅者一些时间来设置
            await asyncio.sleep(1)
            
            # 记录开始时间
            self.result.start_time = time.time()
            
            # 启动发布者
            publishers = []
            for i in range(self.config.publisher_count):
                client_id = i + self.config.subscriber_count + 1
                nc_pub, js_pub = await self.connect_client(client_id)
                pub_task = asyncio.create_task(
                    self.publisher(nc_pub, js_pub, i)
                )
                publishers.append(pub_task)
            
            # 等待所有发布者完成
            await asyncio.gather(*publishers)
            
            # 标记发布完成
            self.all_published.set()
            
            # 等待所有消息被接收或超时
            timeout = 30  # 秒
            try:
                await asyncio.wait_for(self.all_received.wait(), timeout)
            except asyncio.TimeoutError:
                print(f"Timeout waiting for all messages to be received. "
                      f"Received {self.received_count}/{self.result.total_messages}")
            
            # 记录结束时间
            self.result.end_time = time.time()
            
            # 标记测试完成
            self.test_complete.set()
            
            # 更新结果
            self.result.published_messages = self.published_count
            self.result.received_messages = self.received_count
            self.result.errors = self.error_count
            self.result.reconnects = self.reconnect_count
            
            # 等待清理订阅者
            await asyncio.sleep(1)
            
            return self.result
        
        finally:
            # 清理连接
            for nc in self.clients:
                await nc.close()
    
    def print_results(self, result: BenchmarkResult) -> None:
        """打印测试结果"""
        pl = result.publish_latency_stats
        el = result.e2e_latency_stats
        
        print("\n" + "=" * 50)
        print("NATS JetStream Benchmark Results")
        print("=" * 50)
        print("Configuration:")
        print(f"  Server: {self.config.server_url}")
        print(f"  JetStream: {self.config.jetstream}")
        print(f"  Durable: {self.config.durable}")
        print(f"  Publishers: {self.config.publisher_count}")
        print(f"  Subscribers: {self.config.subscriber_count}")
        print(f"  Message Size: {self.config.msg_size} bytes")
        print(f"  Message Count: {self.config.msg_count} per publisher")
        print(f"  Total Messages: {result.total_messages}")
        print(f"  Async Publish: {self.config.async_publish}")
        print("-" * 50)
        print("Results:")
        print(f"  Duration: {result.duration:.3f} seconds")
        print(f"  Published: {result.published_messages} messages")
        print(f"  Received: {result.received_messages} messages")
        print(f"  Coverage: {(result.received_messages / result.total_messages * 100):.2f}%")
        print(f"  Throughput: {result.throughput:.3f} msgs/sec")
        print(f"  Errors: {result.errors}")
        print(f"  Reconnects: {result.reconnects}")
        print("-" * 50)
        print("Publish Latency (ms):")
        print(f"  Min: {pl['min']:.3f}")
        print(f"  Avg: {pl['avg']:.3f}")
        print(f"  p50: {pl['p50']:.3f}")
        print(f"  p95: {pl['p95']:.3f}")
        print(f"  p99: {pl['p99']:.3f}")
        print(f"  Max: {pl['max']:.3f}")
        print("-" * 50)
        print("End-to-End Latency (ms):")
        print(f"  Min: {el['min']:.3f}")
        print(f"  Avg: {el['avg']:.3f}")
        print(f"  p50: {el['p50']:.3f}")
        print(f"  p95: {el['p95']:.3f}")
        print(f"  p99: {el['p99']:.3f}")
        print(f"  Max: {el['max']:.3f}")
        print("=" * 50)
    
    def save_results(self, result: BenchmarkResult) -> None:
        """保存测试结果到文件"""
        if not self.config.output_file:
            return
        
        os.makedirs(os.path.dirname(os.path.abspath(self.config.output_file)), exist_ok=True)
        
        if self.config.output_format == "json":
            with open(self.config.output_file, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
        
        elif self.config.output_format == "csv":
            file_exists = os.path.exists(self.config.output_file)
            
            with open(self.config.output_file, "a") as f:
                if not file_exists:
                    f.write(result.to_csv_header() + "\n")
                f.write(result.to_csv_row() + "\n")
        
        print(f"Results saved to {self.config.output_file}")


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="NATS JetStream Benchmark")
    
    parser.add_argument("--server", default="nats://localhost:4222",
                        help="NATS server URL (default: nats://localhost:4222)")
    parser.add_argument("--stream", default="benchmark",
                        help="Stream name (default: benchmark)")
    parser.add_argument("--subject", default="benchmark.test",
                        help="Subject prefix (default: benchmark.test)")
    parser.add_argument("--clients", type=int, default=1,
                        help="Total number of clients (default: 1)")
    parser.add_argument("--publishers", type=int, default=1,
                        help="Number of publishers (default: 1)")
    parser.add_argument("--subscribers", type=int, default=1,
                        help="Number of subscribers (default: 1)")
    parser.add_argument("--size", type=int, default=1024,
                        help="Message size in bytes (default: 1024)")
    parser.add_argument("--count", type=int, default=10000,
                        help="Number of messages per publisher (default: 10000)")
    parser.add_argument("--no-durable", action="store_true",
                        help="Disable durable subscriptions")
    parser.add_argument("--no-js", action="store_true",
                        help="Disable JetStream (use core NATS)")
    parser.add_argument("--async-publish", action="store_true",
                        help="Use async publish (don't wait for acks)")
    parser.add_argument("--batch", type=int, default=100,
                        help="Batch size for pull subscriptions (default: 100)")
    parser.add_argument("--format", choices=["text", "json", "csv"], default="text",
                        help="Output format (default: text)")
    parser.add_argument("--output", help="Output file for JSON or CSV results")
    
    return parser.parse_args()


async def main() -> None:
    """主函数"""
    args = parse_args()
    
    config = BenchmarkConfig(
        server_url=args.server,
        stream_name=args.stream,
        subject=args.subject,
        client_count=args.clients,
        publisher_count=args.publishers,
        subscriber_count=args.subscribers,
        msg_size=args.size,
        msg_count=args.count,
        durable=not args.no_durable,
        jetstream=not args.no_js,
        output_format=args.format,
        output_file=args.output,
        async_publish=args.async_publish,
        batch_size=args.batch
    )
    
    benchmark = NatsBenchmark(config)
    result = await benchmark.run()
    
    benchmark.print_results(result)
    benchmark.save_results(result)


if __name__ == "__main__":
    asyncio.run(main()) 