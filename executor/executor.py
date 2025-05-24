"""
Executor Implementation

Individual executor that processes TaskCards in sandboxed environments.
"""

import asyncio
import logging
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Import protobuf messages with error handling
try:
    from ..seam_comm.proto.messages_pb2 import TaskCard, ResultCard, ErrorCard
except ImportError:
    # Create placeholder classes if protobuf not available
    class TaskCard:
        def __init__(self, task_id="", description="", steps=None):
            self.task_id = task_id
            self.description = description 
            self.steps = steps or []
    
    class ResultCard:
        def __init__(self, task_id="", executor_id="", result_data="", execution_time=0, timestamp=0, confidence_score=0.9):
            self.task_id = task_id
            self.executor_id = executor_id
            self.result_data = result_data
            self.execution_time = execution_time
            self.timestamp = timestamp
            self.confidence_score = confidence_score
    
    class ErrorCard:
        def __init__(self, task_id="", error_type="", error_message="", timestamp=0, executor_id=""):
            self.task_id = task_id
            self.error_type = error_type
            self.error_message = error_message
            self.timestamp = timestamp
            self.executor_id = executor_id

from .sandbox.base import SandboxBase


logger = logging.getLogger(__name__)


class ExecutorError(Exception):
    """Base exception for executor-related errors."""
    pass


class Executor:
    """
    Individual executor instance that processes TaskCards.
    
    Handles:
    - TaskCard parsing and validation
    - Sandbox creation and management
    - Capability loading based on requirements
    - Security monitoring during execution
    - Result generation and error handling
    """
    
    def __init__(
        self,
        executor_id: Optional[str] = None,
        sandbox_type: str = "e2b",  # Changed default to E2B for production use
        max_execution_time: int = 300,  # 5 minutes default
        resource_limits: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize executor instance.
        
        Args:
            executor_id: Unique identifier for this executor
            sandbox_type: Type of sandbox to use ('container', 'subprocess')
            max_execution_time: Maximum execution time in seconds
            resource_limits: Resource limits for sandbox
        """
        self.executor_id = executor_id or str(uuid.uuid4())
        self.sandbox_type = sandbox_type
        self.max_execution_time = max_execution_time
        self.resource_limits = resource_limits or {}
        
        # Components - lazy load to avoid circular imports
        self.sandbox: Optional[SandboxBase] = None
        self._capability_manager = None
        self._security_monitor = None
        
        # State
        self.current_task: Optional[TaskCard] = None
        self.is_busy = False
        self.stats = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_execution_time": 0,
            "security_violations": 0
        }
        
        logger.info(f"Initialized executor {self.executor_id} with sandbox type: {sandbox_type}")
    
    @property
    def capability_manager(self):
        """Lazy load capability manager."""
        if self._capability_manager is None:
            from .capabilities.manager import CapabilityManager
            self._capability_manager = CapabilityManager()
        return self._capability_manager
    
    @property
    def security_monitor(self):
        """Lazy load security monitor."""
        if self._security_monitor is None:
            from .security.monitor import SecurityMonitor
            self._security_monitor = SecurityMonitor()
        return self._security_monitor
    
    async def execute_task(self, task_card: TaskCard) -> Union[ResultCard, ErrorCard]:
        """
        Execute a TaskCard and return results.
        
        Args:
            task_card: The task to execute
            
        Returns:
            ResultCard on success, ErrorCard on failure
        """
        if self.is_busy:
            return ErrorCard(
                task_id=task_card.task_id,
                error_type="EXECUTOR_BUSY",
                error_message="Executor is currently processing another task",
                timestamp=int(time.time())
            )
        
        self.is_busy = True
        self.current_task = task_card
        start_time = time.time()
        
        try:
            logger.info(f"Starting execution of task {task_card.task_id}")
            
            # Parse task requirements
            capabilities = self._parse_capabilities(task_card)
            
            # Create sandbox with capabilities
            self.sandbox = self._create_sandbox(capabilities)
            
            # Execute task in sandbox
            result = await self._execute_in_sandbox(task_card)
            
            # Create result card
            execution_time = time.time() - start_time
            result_card = ResultCard(
                task_id=task_card.task_id,
                executor_id=self.executor_id,
                result_data=result,
                execution_time=execution_time,
                timestamp=int(time.time()),
                confidence_score=0.9  # Default confidence
            )
            
            self.stats["tasks_completed"] += 1
            self.stats["total_execution_time"] += execution_time
            
            logger.info(f"Completed task {task_card.task_id} in {execution_time:.2f}s")
            return result_card
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_card = ErrorCard(
                task_id=task_card.task_id,
                error_type="EXECUTION_ERROR",
                error_message=str(e),
                timestamp=int(time.time()),
                executor_id=self.executor_id
            )
            
            self.stats["tasks_failed"] += 1
            self.stats["total_execution_time"] += execution_time
            
            logger.error(f"Failed to execute task {task_card.task_id}: {e}")
            return error_card
            
        finally:
            # Cleanup
            if self.sandbox:
                await self.sandbox.cleanup()
                self.sandbox = None
            
            self.current_task = None
            self.is_busy = False
    
    def _parse_capabilities(self, task_card: TaskCard) -> List[str]:
        """
        Parse required capabilities from TaskCard.
        
        Args:
            task_card: The task to analyze
            
        Returns:
            List of required capability names
        """
        # Extract capabilities from task description or metadata
        # For now, use basic heuristics
        capabilities = []
        
        description = task_card.description.lower()
        
        if "network" in description or "http" in description or "download" in description:
            capabilities.append("network")
        
        if "file" in description or "write" in description or "read" in description:
            capabilities.append("filesystem")
        
        if "gpu" in description or "cuda" in description or "tensorflow" in description:
            capabilities.append("gpu")
        
        # Add required capabilities from task metadata
        if hasattr(task_card, 'required_capabilities'):
            capabilities.extend(task_card.required_capabilities)
        
        return list(set(capabilities))  # Remove duplicates
    
    def _create_sandbox(self, capabilities: List[str]) -> SandboxBase:
        """
        Create sandbox with required capabilities.
        
        Args:
            capabilities: List of required capabilities
            
        Returns:
            Configured sandbox instance
        """
        if self.sandbox_type == "e2b":
            from .sandbox.e2b_sandbox import E2BSandbox
            sandbox = E2BSandbox(
                capabilities=capabilities,
                resource_limits=self.resource_limits
            )
        elif self.sandbox_type == "subprocess":
            from .sandbox.subprocess_sandbox import SubprocessSandbox
            sandbox = SubprocessSandbox(
                capabilities=capabilities,
                resource_limits=self.resource_limits
            )
        else:
            # Default to E2B for production use
            from .sandbox.e2b_sandbox import E2BSandbox
            sandbox = E2BSandbox(
                capabilities=capabilities,
                resource_limits=self.resource_limits
            )
        
        return sandbox
    
    async def _execute_in_sandbox(self, task_card: TaskCard) -> str:
        """
        Execute task within the sandbox.
        
        Args:
            task_card: The task to execute
            
        Returns:
            Execution result as string
        """
        if not self.sandbox:
            raise ExecutorError("No sandbox available for execution")
        
        # Start security monitoring
        self.security_monitor.start_monitoring(self.executor_id)
        
        try:
            # Prepare execution environment
            await self.sandbox.prepare()
            
            # Execute task steps
            results = []
            for i, step in enumerate(task_card.steps):
                step_description = getattr(step, 'description', str(step))
                logger.debug(f"Executing step {i+1}/{len(task_card.steps)}: {step_description}")
                
                command = getattr(step, 'command', step_description)
                step_result = await self.sandbox.execute_command(
                    command=command,
                    timeout=self.max_execution_time
                )
                
                results.append(f"Step {i+1}: {step_result}")
                
                # Check for security violations
                violations = self.security_monitor.check_violations()
                if violations:
                    self.stats["security_violations"] += len(violations)
                    raise ExecutorError(f"Security violations detected: {violations}")
            
            return "\n".join(results) if results else "Task completed successfully"
            
        finally:
            self.security_monitor.stop_monitoring()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics."""
        return {
            "executor_id": self.executor_id,
            "is_busy": self.is_busy,
            "current_task_id": self.current_task.task_id if self.current_task else None,
            **self.stats
        }
    
    async def shutdown(self):
        """Shutdown executor and cleanup resources."""
        if self.sandbox:
            await self.sandbox.cleanup()
        
        if self._security_monitor:
            self.security_monitor.stop_monitoring()
        
        logger.info(f"Executor {self.executor_id} shutdown complete") 