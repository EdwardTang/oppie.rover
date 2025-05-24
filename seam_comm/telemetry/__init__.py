"""
OpenTelemetry Integration Module

Provides distributed tracing and metrics collection capabilities:
- tracer: Trace context management (injection, extraction, propagation)
- metrics: Metrics collection

This module ensures all communication adapters can consistently support cross-protocol request tracing.
"""

from .tracer import (
    setup_tracer,
    inject_trace_context,
    extract_trace_context,
    get_current_trace_context,
    with_trace_context,
    create_span
)

# Alias for compatibility
get_tracer = setup_tracer

__all__ = [
    "setup_tracer",
    "get_tracer",
    "inject_trace_context", 
    "extract_trace_context",
    "get_current_trace_context",
    "with_trace_context",
    "create_span"
] 