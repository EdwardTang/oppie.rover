"""
Security Monitor

Monitors executor behavior for security violations and anomalies.
"""

import logging
import time
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class SecurityMonitor:
    """
    Monitors executor behavior for security violations.
    
    Tracks:
    - Execution time anomalies
    - Resource usage spikes
    - Suspicious command patterns
    - File access violations
    """
    
    def __init__(self):
        self.is_monitoring = False
        self.start_time: Optional[float] = None
        self.executor_id: Optional[str] = None
        self.violations: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}
        
    def start_monitoring(self, executor_id: str):
        """Start monitoring an executor."""
        self.executor_id = executor_id
        self.start_time = time.time()
        self.is_monitoring = True
        self.violations.clear()
        self.metrics.clear()
        
        logger.debug(f"Started security monitoring for executor {executor_id}")
        
    def stop_monitoring(self):
        """Stop monitoring and cleanup."""
        if self.is_monitoring:
            duration = time.time() - (self.start_time or 0)
            self.metrics["total_monitoring_time"] = duration
            
            logger.debug(f"Stopped security monitoring for executor {self.executor_id}")
            
        self.is_monitoring = False
        self.executor_id = None
        self.start_time = None
        
    def check_violations(self) -> List[Dict[str, Any]]:
        """
        Check for security violations.
        
        Returns:
            List of violation descriptions
        """
        if not self.is_monitoring:
            return []
            
        current_violations = []
        
        # Check execution time
        if self.start_time:
            execution_time = time.time() - self.start_time
            if execution_time > 600:  # 10 minutes
                current_violations.append({
                    "type": "LONG_EXECUTION",
                    "description": f"Execution time exceeded 10 minutes: {execution_time:.2f}s",
                    "severity": "medium",
                    "timestamp": time.time()
                })
                
        # Check for suspicious patterns (placeholder implementation)
        # In a real implementation, you'd monitor system calls, network activity, etc.
        
        # Add any new violations to the list
        for violation in current_violations:
            if violation not in self.violations:
                self.violations.append(violation)
                logger.warning(f"Security violation detected: {violation}")
                
        return current_violations
        
    def log_event(self, event_type: str, details: Dict[str, Any]):
        """Log a security-relevant event."""
        if not self.is_monitoring:
            return
            
        event = {
            "executor_id": self.executor_id,
            "event_type": event_type,
            "details": details,
            "timestamp": time.time()
        }
        
        # In a real implementation, you'd send this to a security logging system
        logger.debug(f"Security event: {event}")
        
    def get_security_report(self) -> Dict[str, Any]:
        """Get a security report for the current monitoring session."""
        return {
            "executor_id": self.executor_id,
            "monitoring_active": self.is_monitoring,
            "violations": self.violations,
            "metrics": self.metrics,
            "start_time": self.start_time,
            "total_violations": len(self.violations)
        } 