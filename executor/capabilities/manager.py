"""
Capability Manager

Manages dynamic capability loading for executor instances.
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class CapabilityManager:
    """
    Manages capabilities for executor instances.
    
    Capabilities define what resources and permissions an executor can access:
    - network: Network access
    - filesystem: File system access
    - gpu: GPU/CUDA access
    - compute: High-CPU access
    """
    
    def __init__(self):
        self.available_capabilities = {
            "network": NetworkCapability,
            "filesystem": FilesystemCapability,
            "gpu": GPUCapability,
            "compute": ComputeCapability
        }
        self.loaded_capabilities: Dict[str, Any] = {}
        
    def load_capability(self, capability_name: str, config: Optional[Dict] = None) -> bool:
        """
        Load a capability with optional configuration.
        
        Args:
            capability_name: Name of the capability to load
            config: Optional configuration for the capability
            
        Returns:
            True if loaded successfully, False otherwise
        """
        if capability_name in self.loaded_capabilities:
            logger.debug(f"Capability {capability_name} already loaded")
            return True
            
        if capability_name not in self.available_capabilities:
            logger.error(f"Unknown capability: {capability_name}")
            return False
            
        try:
            capability_class = self.available_capabilities[capability_name]
            capability_instance = capability_class(config or {})
            
            if capability_instance.is_available():
                self.loaded_capabilities[capability_name] = capability_instance
                logger.info(f"Loaded capability: {capability_name}")
                return True
            else:
                logger.warning(f"Capability {capability_name} not available on this system")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load capability {capability_name}: {e}")
            return False
    
    def unload_capability(self, capability_name: str) -> bool:
        """
        Unload a capability.
        
        Args:
            capability_name: Name of the capability to unload
            
        Returns:
            True if unloaded successfully, False otherwise
        """
        if capability_name in self.loaded_capabilities:
            try:
                self.loaded_capabilities[capability_name].cleanup()
                del self.loaded_capabilities[capability_name]
                logger.info(f"Unloaded capability: {capability_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to unload capability {capability_name}: {e}")
                return False
        return False
    
    def has_capability(self, capability_name: str) -> bool:
        """Check if a capability is loaded."""
        return capability_name in self.loaded_capabilities
    
    def get_capability(self, capability_name: str) -> Optional[Any]:
        """Get a loaded capability instance."""
        return self.loaded_capabilities.get(capability_name)
    
    def list_loaded_capabilities(self) -> List[str]:
        """Get list of loaded capability names."""
        return list(self.loaded_capabilities.keys())
    
    def cleanup_all(self):
        """Cleanup all loaded capabilities."""
        for capability_name in list(self.loaded_capabilities.keys()):
            self.unload_capability(capability_name)


# Base capability class
class BaseCapability:
    """Base class for all capabilities."""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def is_available(self) -> bool:
        """Check if this capability is available on the current system."""
        return True
        
    def cleanup(self):
        """Cleanup capability resources."""
        pass


# Basic capability implementations
class NetworkCapability(BaseCapability):
    """Network access capability."""
    
    def is_available(self) -> bool:
        # Basic network availability check
        import socket
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=3)
            return True
        except OSError:
            return False


class FilesystemCapability(BaseCapability):
    """Filesystem access capability."""
    
    def is_available(self) -> bool:
        # Filesystem is always available
        return True


class GPUCapability(BaseCapability):
    """GPU/CUDA access capability."""
    
    def is_available(self) -> bool:
        # Check for CUDA availability
        try:
            import subprocess
            result = subprocess.run(["nvidia-smi"], capture_output=True, timeout=5)
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False


class ComputeCapability(BaseCapability):
    """High-CPU compute capability."""
    
    def is_available(self) -> bool:
        # CPU is always available
        return True 