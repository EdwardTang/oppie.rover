"""
Fixed LLM client implementation for Codex CLI
"""
import logging
import subprocess
import os
import tempfile
import json
from typing import Tuple, Optional
from abc import ABC, abstractmethod

import openai
import google.generativeai as genai

from .config import LLMConfig, LLMProvider


logger = logging.getLogger(__name__)


class CodexCLIClient(LLMClient):
    """Fixed Codex CLI client that avoids stdin piping issues"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        
        # Check if codex CLI is available
        if not self._is_codex_available():
            raise RuntimeError("Codex CLI is not installed or not in PATH")
            
    def _is_codex_available(self) -> bool:
        """Check if codex CLI is available"""
        try:
            result = subprocess.run(
                ["codex", "--version"], 
                capture_output=True, 
                text=True, 
                timeout=5
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
            logger.debug(f"Codex CLI check failed: {e}")
            return False
            
    def generate(self, prompt: str, max_tokens: int = 1000) -> Tuple[str, int]:
        """Generate text using Codex CLI with file-based input to avoid TTY issues"""
        try:
            # Solution 1: Use temporary file for input
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp_file:
                tmp_file.write(prompt)
                tmp_file.flush()
                tmp_path = tmp_file.name
            
            try:
                # Pass prompt via file instead of stdin
                cmd = [
                    "codex",
                    "-m", self.config.model,
                    "--max-tokens", str(max_tokens),
                    "--prompt-file", tmp_path  # Use file input if supported
                ]
                
                # Alternative: Set environment variable if Codex supports it
                env = os.environ.copy()
                env['CODEX_PROMPT'] = prompt
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=env
                )
                
                if result.returncode != 0:
                    raise RuntimeError(f"Codex CLI failed: {result.stderr}")
                    
                # Parse response
                tokens_used = max_tokens // 2  # Estimate if not provided
                
                return result.stdout.strip(), tokens_used
                
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                    
        except Exception as e:
            logger.error(f"Codex CLI error: {e}")
            raise 