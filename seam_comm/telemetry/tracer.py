"""
OpenTelemetry Trace Context Management

Provides trace context injection, extraction and propagation capabilities to ensure cross-protocol request tracing.
"""

import logging
import contextvars
from typing import Dict, Any, Optional, ContextManager
from contextlib import contextmanager, nullcontext

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.context import Context, get_current, attach, detach
except Exception:  # pragma: no cover - optional dependency
    trace = None  # type: ignore
    TracerProvider = None  # type: ignore
    BatchSpanProcessor = None  # type: ignore
    ALWAYS_ON = None  # type: ignore
    OTLPSpanExporter = None  # type: ignore

    class Context:  # type: ignore
        pass

    def get_current():  # type: ignore
        return None

    def attach(_context):  # type: ignore
        return None

    def detach(_token):  # type: ignore
        return None

    logging.warning("OpenTelemetry not available, telemetry disabled")

# Use relative imports when importing protobuf to avoid circular dependencies
# In actual code, this may need to be adjusted to appropriate import methods
from oppie_xyz.seam_comm.proto.common_pb2 import TraceContext

logger = logging.getLogger(__name__)

# Context variable for current trace context
current_trace_context = contextvars.ContextVar('current_trace_context', default=None)

def setup_tracer(service_name: str, otlp_endpoint: str = "localhost:4317"):
    """Configure OpenTelemetry tracer
    
    Args:
        service_name: Service name
        otlp_endpoint: OTLP receiver address
    """
    if trace is None or TracerProvider is None:
        logger.warning("OpenTelemetry not installed; tracer disabled")
        return None

    # Create TracerProvider
    provider = TracerProvider(sampler=ALWAYS_ON)
    
    # Create OTLP exporter
    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
    
    # Add batch span processor
    span_processor = BatchSpanProcessor(otlp_exporter)
    provider.add_span_processor(span_processor)
    
    # Set global TracerProvider
    trace.set_tracer_provider(provider)
    
    # Get tracer
    tracer = trace.get_tracer(service_name)
    
    logger.info(f"OpenTelemetry trace configured, service name: {service_name}, OTLP endpoint: {otlp_endpoint}")
    
    return tracer

def inject_trace_context() -> Optional[Dict[str, Any]]:
    """Inject current OpenTelemetry trace context into a transportable dictionary
    
    Converts the current active span context (if any) to dictionary form for passing in cross-service calls.
    
    Returns:
        Dict[str, Any]: Dictionary containing trace_id, span_id and other information, returns None if no active span
    """
    if trace is None:
        return None

    # Get current span context
    span_context = trace.get_current_span().get_span_context()
    
    # If no active span, check context variable
    if not span_context.is_valid:
        trace_ctx = current_trace_context.get()
        if trace_ctx is None:
            return None
            
        # If there's a saved TraceContext protobuf, convert to dictionary
        return {
            'trace_id': trace_ctx.trace_id,
            'span_id': trace_ctx.span_id,
            'sampled': trace_ctx.sampled,
            'baggage': {k: v for k, v in trace_ctx.baggage.items()} if hasattr(trace_ctx, 'baggage') else {}
        }
    
    # Convert to dictionary
    trace_context_dict = {
        'trace_id': span_context.trace_id.to_bytes(16, byteorder='big').hex(),
        'span_id': span_context.span_id.to_bytes(8, byteorder='big').hex(),
        'sampled': span_context.trace_flags.sampled,
        'baggage': {}
    }
    
    # Get baggage (OpenTelemetry context)
    ctx = get_current()
    if ctx:
        # Additional baggage items can be added here
        # For example: trace_context_dict['baggage']['user_id'] = ctx.get_value('user_id')
        pass
    
    # Also update TraceContext in context variable
    trace_ctx = TraceContext()
    trace_ctx.trace_id = trace_context_dict['trace_id']
    trace_ctx.span_id = trace_context_dict['span_id']
    trace_ctx.sampled = trace_context_dict['sampled']
    current_trace_context.set(trace_ctx)
    
    return trace_context_dict

def get_current_trace_context() -> Optional[TraceContext]:
    """Get current TraceContext
    
    Returns:
        TraceContext Protobuf object
    """
    if trace is None:
        return None

    span_context = trace.get_current_span().get_span_context()
    
    # If no active span, return None
    if not span_context.is_valid:
        return current_trace_context.get()
    
    # Create TraceContext protobuf
    trace_context = TraceContext()
    trace_context.trace_id = span_context.trace_id.to_bytes(16, byteorder='big').hex()
    trace_context.span_id = span_context.span_id.to_bytes(8, byteorder='big').hex()
    trace_context.sampled = span_context.trace_flags.sampled
    
    # Get baggage (OpenTelemetry context)
    ctx = get_current()
    if ctx:
        # Add baggage from current context to trace_context
        # (Specific implementation depends on OpenTelemetry API details)
        pass
    
    # Save to context variable
    current_trace_context.set(trace_context)
    
    return trace_context

def extract_trace_context(trace_context_dict: Dict[str, Any]) -> Optional[TraceContext]:
    """Extract TraceContext from dictionary
    
    Args:
        trace_context_dict: Dictionary containing trace_id, span_id etc.
        
    Returns:
        TraceContext Protobuf object
    """
    if not trace_context_dict:
        return None
    
    # Create TraceContext protobuf
    trace_context = TraceContext()
    
    trace_context.trace_id = trace_context_dict.get('trace_id', '')
    trace_context.span_id = trace_context_dict.get('span_id', '')
    trace_context.sampled = trace_context_dict.get('sampled', True)
    
    # Set baggage
    if 'baggage' in trace_context_dict and isinstance(trace_context_dict['baggage'], dict):
        for key, value in trace_context_dict['baggage'].items():
            trace_context.baggage[key] = str(value)
    
    # Save to context variable
    current_trace_context.set(trace_context)
    
    return trace_context

@contextmanager
def with_trace_context(trace_context: Optional[TraceContext]) -> ContextManager[None]:
    """Use specified TraceContext as current context
    
    Args:
        trace_context: TraceContext object
        
    Yields:
        None, used as context manager
    """
    if not trace_context:
        # If no trace_context provided, proceed directly
        yield
        return

    if trace is None:
        yield
        return
    
    # Save original trace_context
    old_trace_context = current_trace_context.get()
    
    try:
        # Set new trace_context
        current_trace_context.set(trace_context)
        
        # If valid, create OpenTelemetry context and attach
        if trace_context.trace_id and trace_context.span_id:
            # Convert strings from protobuf back to original format
            trace_id_bytes = bytes.fromhex(trace_context.trace_id)
            span_id_bytes = bytes.fromhex(trace_context.span_id)
            
            # Create SpanContext
            span_context = trace.SpanContext(
                trace_id=int.from_bytes(trace_id_bytes, byteorder='big'),
                span_id=int.from_bytes(span_id_bytes, byteorder='big'),
                is_remote=True,
                trace_flags=trace.TraceFlags(0x01 if trace_context.sampled else 0x00)
            )
            
            # Create OpenTelemetry Context
            otel_context = trace.set_span_in_context(trace.NonRecordingSpan(span_context))
            
            # Add baggage
            # (Specific implementation depends on OpenTelemetry API details)
            
            # Attach context
            token = attach(otel_context)
            try:
                yield
            finally:
                # Restore original context
                detach(token)
        else:
            yield
    finally:
        # Restore original trace_context
        current_trace_context.set(old_trace_context)

def create_span(name: str, attributes: Dict[str, Any] = None):
    """Create new span
    
    Args:
        name: Span name
        attributes: Span attributes
        
    Returns:
        Span object
    """
    if trace is None:
        return nullcontext()

    tracer = trace.get_tracer(__name__)
    return tracer.start_as_current_span(
        name,
        attributes=attributes or {},
        kind=trace.SpanKind.INTERNAL,
    )
