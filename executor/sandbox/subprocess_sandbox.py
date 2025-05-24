"""
Subprocess Sandbox Implementation

Simple sandbox using subprocess for basic task execution.
This is primarily for testing and development purposes.
"""

import asyncio
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import SandboxBase, SandboxError


class SubprocessSandbox(SandboxBase):
    """
    Simple subprocess-based sandbox.
    
    Provides basic isolation using subprocess execution with restricted environment.
    This is suitable for development and testing but not production security.
    """
    
    def __init__(
        self,
        capabilities: Optional[List[str]] = None,
        resource_limits: Optional[Dict[str, Any]] = None
    ):
        super().__init__(capabilities, resource_limits)
        self.work_dir: Optional[Path] = None
        self.env: Dict[str, str] = {}
        
    async def prepare(self) -> None:
        """Prepare the subprocess sandbox environment."""
        if self.is_prepared:
            return
            
        # Create temporary working directory
        self.work_dir = Path(tempfile.mkdtemp(prefix="executor_sandbox_"))
        
        # Set up restricted environment
        self.env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": "",
            "HOME": str(self.work_dir),
            "TMPDIR": str(self.work_dir / "tmp"),
            "TEMP": str(self.work_dir / "tmp"),
            "TMP": str(self.work_dir / "tmp"),
        }
        
        # Create temp directory
        (self.work_dir / "tmp").mkdir(exist_ok=True)
        
        # Configure capabilities
        if "network" not in self.capabilities:
            # Note: This doesn't actually block network access in subprocess
            # In a production environment, you'd use netns or similar
            self.env["no_proxy"] = "*"
            
        self.is_prepared = True
        
    async def execute_command(self, command: str, timeout: int = 300) -> str:
        """
        Execute command in subprocess with restrictions.
        
        Args:
            command: Command to execute
            timeout: Maximum execution time in seconds
            
        Returns:
            Command output
            
        Raises:
            SandboxError: If execution fails
        """
        if not self.is_prepared:
            await self.prepare()
            
        try:
            # Simple command parsing - in production, you'd want more sophisticated parsing
            if command.startswith("python"):
                # For Python commands, create a temporary script
                script_content = command.replace("python -c ", "").strip("\"'")
                script_path = self.work_dir / f"script_{os.getpid()}.py"
                script_path.write_text(script_content)
                cmd_args = ["python", str(script_path)]
            else:
                # For shell commands, use shell execution
                cmd_args = command
                
            # Execute with timeout and restricted environment
            result = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *cmd_args if isinstance(cmd_args, list) else cmd_args,
                    cwd=self.work_dir,
                    env=self.env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=not isinstance(cmd_args, list)
                ),
                timeout=timeout
            )
            
            stdout, stderr = await result.communicate()
            
            if result.returncode != 0:
                error_msg = f"Command failed with code {result.returncode}: {stderr.decode()}"
                raise SandboxError(error_msg)
                
            return stdout.decode().strip()
            
        except asyncio.TimeoutError:
            raise SandboxError(f"Command timed out after {timeout} seconds")
        except Exception as e:
            raise SandboxError(f"Execution failed: {str(e)}")
            
    async def cleanup(self) -> None:
        """Clean up sandbox resources."""
        if self.work_dir and self.work_dir.exists():
            # Remove temporary directory and all contents
            import shutil
            try:
                shutil.rmtree(self.work_dir)
            except OSError as e:
                # Log error but don't fail cleanup
                print(f"Warning: Failed to cleanup sandbox directory {self.work_dir}: {e}")
                
        self.work_dir = None
        self.is_prepared = False 