"""
Capabilities Module

Manages dynamic capability loading for executor instances.
Capabilities include network access, filesystem permissions, GPU access, etc.
"""

from .manager import CapabilityManager
from .network import NetworkCapability
from .filesystem import FilesystemCapability

__all__ = ["CapabilityManager", "NetworkCapability", "FilesystemCapability"] 