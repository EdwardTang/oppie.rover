"""
Base Sandbox Interface

Abstract base class for all sandbox implementations.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class SandboxBase(ABC):
    """
    Abstract base class for sandbox implementations.
    
    All sandbox implementations must inherit from this class and implement
    the required methods for task execution in isolated environments.
    """
    
    def __init__(
        self, 
        capabilities: Optional[List[str]] = None,
        resource_limits: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize sandbox with capabilities and resource limits.
        
        Args:
            capabilities: List of required capabilities (network, filesystem, gpu, etc.)
            resource_limits: Resource limits (memory, cpu, disk, etc.)
        """
        self.capabilities = capabilities or []
        self.resource_limits = resource_limits or {}
        self.is_prepared = False
        
    @abstractmethod
    async def prepare(self) -> None:
        """
        Prepare the sandbox environment for execution.
        This may involve setting up containers, VMs, or other isolation mechanisms.
        """
        pass
    
    @abstractmethod
    async def execute_command(self, command: str, timeout: int = 300) -> str:
        """
        Execute a command within the sandbox.
        
        Args:
            command: The command to execute
            timeout: Maximum execution time in seconds
            
        Returns:
            Command output as string
            
        Raises:
            SandboxError: If execution fails or times out
        """
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """
        Clean up sandbox resources.
        This should remove containers, VMs, temporary files, etc.
        """
        pass
    
    def has_capability(self, capability: str) -> bool:
        """Check if sandbox has a specific capability."""
        return capability in self.capabilities
    
    def get_resource_limit(self, resource: str) -> Any:
        """Get resource limit for a specific resource."""
        return self.resource_limits.get(resource)


class SandboxError(Exception):
    """Base exception for sandbox-related errors."""
    pass 