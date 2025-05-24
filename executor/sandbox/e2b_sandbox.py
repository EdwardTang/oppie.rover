"""
E2B Sandbox Implementation

Firecracker microVM-based sandbox for secure task execution using E2B.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from .base import SandboxBase, SandboxError

logger = logging.getLogger(__name__)


class E2BSandbox(SandboxBase):
    """
    E2B (End-to-End Browser) sandbox implementation.
    
    Uses Firecracker microVMs for secure isolation of AI-generated code execution.
    Provides fast startup (~150ms) and 24-hour session persistence.
    """
    
    def __init__(
        self,
        capabilities: Optional[List[str]] = None,
        resource_limits: Optional[Dict[str, Any]] = None,
        template_id: Optional[str] = None
    ):
        super().__init__(capabilities, resource_limits)
        self.sandbox = None
        self.template_id = template_id
        self.session_start_time = None
        
        # Import E2B here to handle missing dependency gracefully
        try:
            from e2b_code_interpreter import Sandbox as E2BSandbox
            self.E2BSandbox = E2BSandbox
        except ImportError:
            raise SandboxError(
                "E2B code interpreter not available. Install with: pip install e2b-code-interpreter"
            )
        
    async def prepare(self) -> None:
        """Prepare the E2B sandbox environment."""
        try:
            start_time = time.time()
            
            # Create E2B sandbox (this runs in an async context)
            loop = asyncio.get_event_loop()
            if self.template_id:
                self.sandbox = await loop.run_in_executor(
                    None, 
                    lambda: self.E2BSandbox.create(template=self.template_id)
                )
            else:
                self.sandbox = await loop.run_in_executor(
                    None, 
                    self.E2BSandbox.create
                )
            
            self.session_start_time = time.time()
            startup_time = self.session_start_time - start_time
            
            logger.info(f"E2B sandbox created in {startup_time:.3f}s")
            
            # Install any required capabilities
            await self._install_capabilities()
            
        except Exception as e:
            logger.error(f"Failed to create E2B sandbox: {e}")
            raise SandboxError(f"Failed to create E2B sandbox: {e}")
        
    async def execute_command(self, command: str, timeout: int = 300) -> str:
        """Execute command in E2B sandbox."""
        if not self.sandbox:
            raise SandboxError("Sandbox not initialized")
            
        try:
            start_time = time.time()
            
            # Execute command in E2B sandbox
            loop = asyncio.get_event_loop()
            execution = await loop.run_in_executor(
                None,
                lambda: self.sandbox.run_code(command, timeout=timeout)
            )
            
            execution_time = time.time() - start_time
            logger.debug(f"Command executed in {execution_time:.3f}s")
            
            # Handle execution results
            if execution.error:
                logger.error(f"E2B execution error: {execution.error}")
                raise SandboxError(f"Execution error: {execution.error}")
            
            # Combine stdout and any logs
            result = ""
            if execution.stdout:
                result += execution.stdout
            if execution.logs.stdout:
                result += execution.logs.stdout
            if execution.logs.stderr:
                # Include stderr but don't treat as error unless execution failed
                logger.warning(f"E2B stderr: {execution.logs.stderr}")
                if not result:  # Only include stderr if no stdout
                    result = execution.logs.stderr
                    
            return result or "Command executed successfully (no output)"
            
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise SandboxError(f"Command execution failed: {e}")
            
    async def cleanup(self) -> None:
        """Clean up E2B sandbox resources."""
        if self.sandbox:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.sandbox.close)
                
                if self.session_start_time:
                    session_duration = time.time() - self.session_start_time
                    logger.info(f"E2B sandbox session ended after {session_duration:.1f}s")
                    
            except Exception as e:
                logger.error(f"Error cleaning up E2B sandbox: {e}")
            finally:
                self.sandbox = None
                self.session_start_time = None
    
    async def _install_capabilities(self) -> None:
        """Install required capabilities in the sandbox."""
        if not self.capabilities:
            return
            
        capability_commands = []
        
        for capability in self.capabilities:
            if capability == "network":
                # E2B has network access by default
                logger.debug("Network capability enabled (default in E2B)")
                
            elif capability == "filesystem":
                # E2B has filesystem access by default
                logger.debug("Filesystem capability enabled (default in E2B)")
                
            elif capability == "gpu":
                # Check if GPU is available in the environment
                capability_commands.append("nvidia-smi || echo 'GPU not available'")
                
            elif capability == "python-packages":
                # Install common Python packages for AI tasks
                capability_commands.extend([
                    "pip install --upgrade pip",
                    "pip install numpy pandas matplotlib seaborn",
                    "pip install scikit-learn tensorflow torch",
                ])
                
            else:
                logger.warning(f"Unknown capability: {capability}")
        
        # Execute capability installation commands
        for cmd in capability_commands:
            try:
                await self.execute_command(cmd)
                logger.debug(f"Capability command executed: {cmd}")
            except Exception as e:
                logger.warning(f"Failed to install capability with command '{cmd}': {e}")
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get information about the current sandbox session."""
        info = {
            "sandbox_type": "e2b",
            "is_active": self.sandbox is not None,
            "template_id": self.template_id,
            "capabilities": self.capabilities or [],
            "resource_limits": self.resource_limits or {},
        }
        
        if self.session_start_time:
            info["session_duration"] = time.time() - self.session_start_time
            info["session_start_time"] = self.session_start_time
            
        return info
    
    async def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload a file to the sandbox."""
        if not self.sandbox:
            raise SandboxError("Sandbox not initialized")
            
        try:
            loop = asyncio.get_event_loop()
            
            # Read local file content
            with open(local_path, 'rb') as f:
                content = f.read()
            
            # Upload to E2B sandbox
            await loop.run_in_executor(
                None,
                lambda: self.sandbox.filesystem.write(remote_path, content)
            )
            
            logger.debug(f"File uploaded: {local_path} -> {remote_path}")
            
        except Exception as e:
            logger.error(f"Failed to upload file {local_path}: {e}")
            raise SandboxError(f"Failed to upload file {local_path}: {e}")
    
    async def download_file(self, remote_path: str, local_path: str) -> None:
        """Download a file from the sandbox."""
        if not self.sandbox:
            raise SandboxError("Sandbox not initialized")
            
        try:
            loop = asyncio.get_event_loop()
            
            # Download from E2B sandbox
            content = await loop.run_in_executor(
                None,
                lambda: self.sandbox.filesystem.read(remote_path)
            )
            
            # Write to local file
            with open(local_path, 'wb') as f:
                f.write(content)
                
            logger.debug(f"File downloaded: {remote_path} -> {local_path}")
            
        except Exception as e:
            logger.error(f"Failed to download file {remote_path}: {e}")
            raise SandboxError(f"Failed to download file {remote_path}: {e}") 