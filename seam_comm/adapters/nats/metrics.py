#!/usr/bin/env python3
"""
NATS JetStream适配器的OpenTelemetry指标支持

实现了OpenTelemetry的指标收集器，用于监控NATS JetStream适配器的性能和健康状况。
支持的指标包括：
- 发布延迟
- 端到端延迟
- 消息吞吐量
- 消费者积压
- 重连次数
- 重新传递计数
- 重复消息计数
- 持久化写入延迟
"""

import os
import time
import numpy as np
from typing import Dict, Optional, List, Union

# 检查是否启用了OpenTelemetry
ENABLE_OTEL = os.environ.get("ENABLE_OTEL", "0").lower() in ["1", "true", "yes"]
# 检查是否启用HTTP指标服务器
ENABLE_METRICS_HTTP = os.environ.get("SEAM_METRICS_HTTP", "0").lower() in ["1", "true", "yes"]

# 如果启用了OpenTelemetry，导入相关库
if ENABLE_OTEL:
    try:
        from opentelemetry import metrics
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        
        # 设置资源，包含服务名称和版本信息
        resource = Resource.create({
            "service.name": "seam_comm.nats_adapter",
            "service.version": "0.1.0",
        })
        
        # 设置OTLP导出器
        otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
        exporter = OTLPMetricExporter(endpoint=otlp_endpoint)
        
        # 创建周期性导出的指标读取器
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
        
        # 设置指标提供者
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        
        # 创建仪表
        meter = metrics.get_meter("seam_comm.nats_adapter")
        
        # 创建指标
        publish_latency = meter.create_histogram(
            name="nats.publish.latency",
            description="发布消息的延迟（毫秒）",
            unit="ms",
        )
        
        e2e_latency = meter.create_histogram(
            name="nats.e2e.latency",
            description="端到端消息传递延迟（毫秒）",
            unit="ms",
        )
        
        msgs_per_sec = meter.create_counter(
            name="nats.messages.published",
            description="已发布消息数量",
            unit="1",
        )
        
        consumer_backlog = meter.create_up_down_counter(
            name="nats.consumer.backlog",
            description="消费者积压消息数量",
            unit="1",
        )
        
        reconnect_count = meter.create_counter(
            name="nats.client.reconnects",
            description="客户端重连次数",
            unit="1",
        )
        
        redelivery_count = meter.create_counter(
            name="nats.messages.redelivered",
            description="重新传递的消息数量",
            unit="1",
        )
        
        duplicate_count = meter.create_counter(
            name="nats.messages.duplicates",
            description="检测到的重复消息数量",
            unit="1",
        )
        
        persistence_write_latency = meter.create_histogram(
            name="nats.persistence.write.latency",
            description="消息持久化写入延迟（毫秒）",
            unit="ms",
        )
        
        OTEL_AVAILABLE = True
    except ImportError:
        # 如果未安装OpenTelemetry库，则禁用指标
        OTEL_AVAILABLE = False
else:
    OTEL_AVAILABLE = False


class HistogramSummary:
    """
    直方图摘要，用于计算延迟的各种百分位数和统计信息

    这个类在不依赖OpenTelemetry的情况下也能工作，适合离线分析和报告生成
    """

    def __init__(self, name: str, unit: str = "ms"):
        """
        初始化直方图摘要

        Args:
            name: 指标名称
            unit: 单位（默认为毫秒）
        """
        self.name = name
        self.unit = unit
        self.values: List[float] = []
        self.start_time = time.time()
        self.last_reset_time = self.start_time

    def record(self, value: float) -> None:
        """
        记录一个值

        Args:
            value: 要记录的值
        """
        self.values.append(value)

    def reset(self) -> None:
        """重置所有值，保留名称和单位"""
        self.values = []
        self.last_reset_time = time.time()

    def get_percentiles(self, percentiles: List[float] = None) -> Dict[float, float]:
        """
        计算百分位数

        Args:
            percentiles: 要计算的百分位数列表，例如[0.5, 0.95, 0.99]

        Returns:
            Dict[float, float]: 百分位数及其对应的值
        """
        if not percentiles:
            percentiles = [0.5, 0.9, 0.95, 0.99]

        if not self.values:
            return {p: 0.0 for p in percentiles}

        result = {}
        for p in percentiles:
            result[p] = float(np.percentile(self.values, p * 100))
        return result

    def get_stats(self) -> Dict[str, float]:
        """
        获取基本统计信息

        Returns:
            Dict[str, float]: 包含min、max、mean、count等统计信息
        """
        if not self.values:
            return {"min": 0.0, "max": 0.0, "mean": 0.0, "count": 0, "sum": 0.0}

        return {
            "min": float(min(self.values)),
            "max": float(max(self.values)),
            "mean": float(np.mean(self.values)),
            "count": len(self.values),
            "sum": float(sum(self.values))
        }

    def get_summary(self) -> Dict[str, Union[str, float, Dict]]:
        """
        获取完整摘要

        Returns:
            Dict: 包含名称、单位、百分位数和基本统计信息的完整摘要
        """
        return {
            "name": self.name,
            "unit": self.unit,
            "percentiles": self.get_percentiles(),
            "stats": self.get_stats(),
            "duration_seconds": time.time() - self.last_reset_time
        }


class NatsMetrics:
    """NATS指标收集器"""
    
    def __init__(self, client_id: str = "default", enabled: bool = True):
        """初始化指标收集器

        Args:
            client_id: 客户端ID，用于区分不同的客户端实例
            enabled: 是否启用指标收集
        """
        self.enabled = enabled and OTEL_AVAILABLE and ENABLE_OTEL
        self.client_id = client_id
        
        # 用于计算各种指标的内部状态
        self._publish_start_times: Dict[str, float] = {}  # 消息ID -> 发布开始时间
        self._last_reconnect_time = 0.0
        self._last_metrics_time = time.time()
        self._message_count_since_last = 0
        
        # 用于离线分析的直方图摘要
        self.publish_latency_summary = HistogramSummary("publish_latency", "ms")
        self.e2e_latency_summary = HistogramSummary("e2e_latency", "ms")
        self.persistence_write_latency_summary = HistogramSummary("persistence_write_latency", "ms")
        
        # 计数器
        self.message_count = 0
        self.reconnect_count = 0
        self.redelivery_count = 0
        self.duplicate_count = 0
        self.backlog_count = 0
        
    def record_publish_start(self, msg_id: str) -> None:
        """记录消息发布开始时间

        Args:
            msg_id: 消息ID
        """
        self._publish_start_times[msg_id] = time.time()
    
    def record_publish_end(self, msg_id: str, attributes: Optional[Dict[str, str]] = None) -> None:
        """记录消息发布结束，计算并记录发布延迟

        Args:
            msg_id: 消息ID
            attributes: 附加标签属性
        """
        if msg_id not in self._publish_start_times:
            return
        
        # 计算发布延迟（毫秒）
        start_time = self._publish_start_times.pop(msg_id)
        latency_ms = (time.time() - start_time) * 1000
        
        # 添加默认属性
        attrs = {"client_id": self.client_id}
        if attributes:
            attrs.update(attributes)
        
        # 更新本地直方图摘要
        self.publish_latency_summary.record(latency_ms)
        self.message_count += 1
        
        # 如果启用了OpenTelemetry，记录指标
        if self.enabled:
            # 记录发布延迟
            publish_latency.record(latency_ms, attrs)
            
            # 记录发布消息计数
            msgs_per_sec.add(1, attrs)
            self._message_count_since_last += 1
            
            # 每10秒计算一次消息速率
            now = time.time()
            if now - self._last_metrics_time >= 10:
                self._last_metrics_time = now
                self._message_count_since_last = 0
    
    def record_e2e_latency(self, latency_ms: float, attributes: Optional[Dict[str, str]] = None) -> None:
        """记录端到端延迟

        Args:
            latency_ms: 延迟时间（毫秒）
            attributes: 附加标签属性
        """
        # 更新本地直方图摘要
        self.e2e_latency_summary.record(latency_ms)
        
        if not self.enabled:
            return
        
        # 添加默认属性
        attrs = {"client_id": self.client_id}
        if attributes:
            attrs.update(attributes)
        
        # 记录端到端延迟
        e2e_latency.record(latency_ms, attrs)
    
    def record_consumer_backlog(self, count: int, stream: str, consumer: str) -> None:
        """记录消费者积压消息数量

        Args:
            count: 积压消息数量
            stream: 流名称
            consumer: 消费者名称
        """
        # 更新本地计数器
        self.backlog_count = count
        
        if not self.enabled:
            return
        
        attrs = {
            "client_id": self.client_id,
            "stream": stream,
            "consumer": consumer
        }
        
        # 记录消费者积压
        consumer_backlog.add(count, attrs)
    
    def record_reconnect(self) -> None:
        """记录客户端重连事件"""
        # 更新本地计数器
        self.reconnect_count += 1
        
        if not self.enabled:
            return
        
        # 防止短时间内多次记录同一次重连
        now = time.time()
        if now - self._last_reconnect_time < 1.0:
            return
        
        self._last_reconnect_time = now
        attrs = {"client_id": self.client_id}
        
        # 记录重连次数
        reconnect_count.add(1, attrs)
    
    def record_redelivery(self, count: int = 1, attributes: Optional[Dict[str, str]] = None) -> None:
        """记录消息重新传递

        Args:
            count: 重新传递的消息数量
            attributes: 附加标签属性
        """
        # 更新本地计数器
        self.redelivery_count += count
        
        if not self.enabled:
            return
        
        # 添加默认属性
        attrs = {"client_id": self.client_id}
        if attributes:
            attrs.update(attributes)
        
        # 记录重新传递计数
        redelivery_count.add(count, attrs)
    
    def record_duplicate(self, count: int = 1, attributes: Optional[Dict[str, str]] = None) -> None:
        """记录重复消息

        Args:
            count: 重复消息数量
            attributes: 附加标签属性
        """
        # 更新本地计数器
        self.duplicate_count += count
        
        if not self.enabled:
            return
        
        # 添加默认属性
        attrs = {"client_id": self.client_id}
        if attributes:
            attrs.update(attributes)
        
        # 记录重复消息计数
        duplicate_count.add(count, attrs)
    
    def record_persistence_write_latency(self, latency_ms: float, attributes: Optional[Dict[str, str]] = None) -> None:
        """记录持久化写入延迟

        Args:
            latency_ms: 延迟时间（毫秒）
            attributes: 附加标签属性
        """
        # 更新本地直方图摘要
        self.persistence_write_latency_summary.record(latency_ms)
        
        if not self.enabled:
            return
        
        # 添加默认属性
        attrs = {"client_id": self.client_id}
        if attributes:
            attrs.update(attributes)
        
        # 记录持久化写入延迟
        persistence_write_latency.record(latency_ms, attrs)
    
    def get_all_summaries(self) -> Dict[str, Dict]:
        """获取所有指标摘要

        Returns:
            Dict[str, Dict]: 所有指标的摘要
        """
        return {
            "publish_latency": self.publish_latency_summary.get_summary(),
            "e2e_latency": self.e2e_latency_summary.get_summary(),
            "persistence_write_latency": self.persistence_write_latency_summary.get_summary(),
            "counters": {
                "messages": self.message_count,
                "reconnects": self.reconnect_count,
                "redeliveries": self.redelivery_count,
                "duplicates": self.duplicate_count,
                "backlog": self.backlog_count
            },
            "client_id": self.client_id,
            "timestamp": time.time(),
            "uptime_seconds": time.time() - self.publish_latency_summary.start_time
        }
    
    def reset_summaries(self) -> None:
        """重置所有直方图摘要"""
        self.publish_latency_summary.reset()
        self.e2e_latency_summary.reset()
        self.persistence_write_latency_summary.reset()


# 创建默认的指标收集器实例
default_metrics = NatsMetrics(client_id="default", enabled=ENABLE_OTEL)


def get_metrics(client_id: str = "default") -> NatsMetrics:
    """获取指标收集器实例

    Args:
        client_id: 客户端ID，用于区分不同的客户端实例
    
    Returns:
        NatsMetrics: 指标收集器实例
    """
    return NatsMetrics(client_id=client_id, enabled=ENABLE_OTEL)


# HTTP服务器用于暴露指标
if ENABLE_METRICS_HTTP:
    try:
        from aiohttp import web
        
        # 存储所有注册的指标收集器
        _registered_metrics = {}
        
        def register_metrics(metrics: NatsMetrics) -> None:
            """注册指标收集器
            
            Args:
                metrics: 指标收集器实例
            """
            _registered_metrics[metrics.client_id] = metrics
        
        # 注册默认指标收集器
        register_metrics(default_metrics)
        
        async def metrics_handler(request):
            """HTTP处理程序，返回Prometheus格式的指标
            
            Args:
                request: HTTP请求
            
            Returns:
                web.Response: HTTP响应
            """
            # 这里简单返回所有收集器的JSON摘要
            # 实际生产中，应该实现完整的Prometheus格式转换
            all_metrics = {
                client_id: metrics.get_all_summaries()
                for client_id, metrics in _registered_metrics.items()
            }
            return web.json_response(all_metrics)
        
        async def metrics_reset_handler(request):
            """HTTP处理程序，重置所有指标
            
            Args:
                request: HTTP请求
            
            Returns:
                web.Response: HTTP响应
            """
            for metrics in _registered_metrics.values():
                metrics.reset_summaries()
            return web.Response(text="All metrics reset")
        
        # 创建aiohttp应用
        metrics_app = web.Application()
        metrics_app.add_routes([
            web.get('/metrics', metrics_handler),
            web.post('/metrics/reset', metrics_reset_handler)
        ])
        
    except ImportError:
        print("Warning: aiohttp not available, HTTP metrics server disabled")
        metrics_app = None 