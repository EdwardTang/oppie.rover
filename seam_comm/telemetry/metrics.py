"""
OpenTelemetry Metrics Collection

Provides functionality for collecting and exporting metrics to support monitoring system performance and health status.
"""

import logging
from typing import Dict, Any

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader
)
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

logger = logging.getLogger(__name__)

# Global metric counters and gauges dictionary
_counters = {}
_gauges = {}
_histograms = {}

def setup_metrics(service_name: str, otlp_endpoint: str = "localhost:4317", export_interval_ms: int = 5000):
    """Configure OpenTelemetry metrics collection
    
    Args:
        service_name: Service name
        otlp_endpoint: OTLP receiver address
        export_interval_ms: Metrics export interval in milliseconds
    """
    # Create OTLP exporter
    otlp_exporter = OTLPMetricExporter(endpoint=otlp_endpoint)
    
    # Create console exporter (for development debugging)
    console_exporter = ConsoleMetricExporter()
    
    # Create periodic export reader
    otlp_reader = PeriodicExportingMetricReader(
        otlp_exporter,
        export_interval_millis=export_interval_ms
    )
    
    console_reader = PeriodicExportingMetricReader(
        console_exporter,
        export_interval_millis=export_interval_ms
    )
    
    # Create MeterProvider
    provider = MeterProvider(metric_readers=[otlp_reader, console_reader])
    
    # Set global MeterProvider
    metrics.set_meter_provider(provider)
    
    # Get Meter
    meter = metrics.get_meter(service_name)
    
    logger.info(f"OpenTelemetry metrics configured, service name: {service_name}, OTLP endpoint: {otlp_endpoint}")
    
    return meter

def get_counter(name: str, description: str, unit: str = "1"):
    """Get or create counter
    
    Args:
        name: Counter name
        description: Counter description
        unit: Counter unit (default "1", representing count)
        
    Returns:
        Counter: Counter object
    """
    global _counters
    
    if name not in _counters:
        meter = metrics.get_meter(__name__)
        _counters[name] = meter.create_counter(
            name=name,
            description=description,
            unit=unit
        )
    
    return _counters[name]

def get_gauge(name: str, description: str, unit: str = "1"):
    """Get or create gauge (observer)
    
    Args:
        name: Gauge name
        description: Gauge description
        unit: Gauge unit
        
    Returns:
        ObservableGauge: Gauge object
    """
    global _gauges
    
    if name not in _gauges:
        meter = metrics.get_meter(__name__)
        _gauges[name] = meter.create_observable_gauge(
            name=name,
            description=description,
            unit=unit,
            callbacks=[]
        )
    
    return _gauges[name]

def get_histogram(name: str, description: str, unit: str = "ms"):
    """Get or create histogram
    
    Args:
        name: Histogram name
        description: Histogram description
        unit: Histogram unit (default "ms", representing milliseconds)
        
    Returns:
        Histogram: Histogram object
    """
    global _histograms
    
    if name not in _histograms:
        meter = metrics.get_meter(__name__)
        _histograms[name] = meter.create_histogram(
            name=name,
            description=description,
            unit=unit
        )
    
    return _histograms[name]

def increment_counter(name: str, amount: int = 1, attributes: Dict[str, Any] = None):
    """Increment counter value
    
    Args:
        name: Counter name
        amount: Amount to increment
        attributes: Attribute labels
    """
    counter = get_counter(name, f"Counter for {name}")
    counter.add(amount, attributes or {})

def record_latency(name: str, value_ms: float, attributes: Dict[str, Any] = None):
    """Record latency histogram
    
    Args:
        name: Histogram name
        value_ms: Latency value in milliseconds
        attributes: Attribute labels
    """
    histogram = get_histogram(name, f"Latency histogram for {name}")
    histogram.record(value_ms, attributes or {})

def add_gauge_callback(name: str, callback, description: str = None, unit: str = "1"):
    """Add gauge callback function
    
    Args:
        name: Gauge name
        callback: Callback function that returns current value
        description: Gauge description
        unit: Gauge unit
    """
    meter = metrics.get_meter(__name__)
    if description is None:
        description = f"Gauge for {name}"
    
    # Create Observable, register callback
    meter.create_observable_gauge(
        name=name,
        description=description,
        unit=unit,
        callbacks=[callback]
    ) 