"""
Executor Pool Module

This module provides sandboxed execution capabilities for the oppie.xyz system.
It receives TaskCards from the Planner and executes them in secure, isolated environments.

Key Components:
- Executor: Individual execution units that process TaskCards
- Sandbox: Isolation mechanisms (gVisor, Firecracker, containers)
- Capabilities: Dynamic capability loading based on task requirements
- Security: Monitoring and enforcement of security policies
"""

from .executor import Executor

__all__ = ["Executor"]
__version__ = "0.1.0" 