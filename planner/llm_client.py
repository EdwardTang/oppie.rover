"""
LLM client implementations for different providers
"""
import logging
import subprocess
import os
from typing import Tuple, Optional
from abc import ABC, abstractmethod

import openai
try:  # Optional dependency for Gemini support
    import google.generativeai as genai
except ImportError:  # pragma: no cover - avoid import error during tests
    genai = None  # type: ignore[assignment]

from .config import LLMConfig, LLMProvider
# Import the safe CLI utilities
from ..utils.cli_utils import run_cli_safe, CLIError, is_raw_mode_supported


logger = logging.getLogger(__name__)


class LLMClient(ABC):
    """Abstract base class for LLM clients"""
    
    @abstractmethod
    def generate(self, prompt: str, max_tokens: int = 1000) -> Tuple[str, int]:
        """Generate text from prompt, returns (response, tokens_used)"""
        pass


class OpenAIClient(LLMClient):
    """OpenAI API client"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self.client = openai.OpenAI(
            api_key=config.api_key,
            base_url=config.base_url
        )
        
    def generate(self, prompt: str, max_tokens: int = 1000) -> Tuple[str, int]:
        """Generate text using OpenAI API"""
        try:
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.7
            )
            
            content = response.choices[0].message.content
            tokens_used = response.usage.total_tokens if response.usage else 0
            
            return content, tokens_used
            
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            raise
            
    def estimate_tokens(self, text: str) -> int:
        """Estimate tokens using tiktoken or simple approximation"""
        try:
            import tiktoken
            encoding = tiktoken.encoding_for_model(self.config.model)
            return len(encoding.encode(text))
        except Exception as exc:
            logger.exception("Token estimation failed", exc_info=exc)
            # Fallback to simple approximation
            return len(text) // 4


class GeminiClient(LLMClient):
    """Google Gemini API client"""

    def __init__(self, config: LLMConfig):
        if genai is None:
            raise ImportError(
                "google-generativeai is required for Gemini support"
            )
        self.config = config
        genai.configure(api_key=config.api_key)
        self.model = genai.GenerativeModel(config.model)
        
    def generate(self, prompt: str, max_tokens: int = 1000) -> Tuple[str, int]:
        """Generate text using Gemini API"""
        try:
            # Configure generation settings
            generation_config = genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=0.7
            )
            
            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            content = response.text
            # Gemini doesn't provide token usage in free tier, estimate
            tokens_used = max_tokens // 2  # Rough estimate
            
            return content, tokens_used
            
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            raise


class CodexCLIClient(LLMClient):
    """Codex CLI client with TTY-safe execution"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        
        # Check if codex CLI is available
        if not self._is_codex_available():
            raise RuntimeError("Codex CLI is not installed or not in PATH")
        
        # Log TTY support status
        if not is_raw_mode_supported():
            logger.info("TTY/Raw mode not supported - using safe CLI execution mode")
            
    def _is_codex_available(self) -> bool:
        """Check if codex CLI is available"""
        try:
            # Use safe CLI runner for version check
            stdout, stderr, return_code = run_cli_safe(
                command=["codex", "--version"],
                timeout=5,
                check_errors=False,
                force_non_interactive=True
            )
            return return_code == 0
        except Exception as e:
            logger.debug(f"Codex CLI check failed: {e}")
            return False
            
    def generate(self, prompt: str, max_tokens: int = 1000) -> Tuple[str, int]:
        """Generate text using Codex CLI with TTY-safe execution"""
        try:
            # Build command
            cmd = [
                "codex",
                "-m", self.config.model,
                "--max-tokens", str(max_tokens)
            ]
            
            # Use safe CLI execution to avoid TTY issues
            stdout, stderr, return_code = run_cli_safe(
                command=cmd,
                input_text=prompt,
                timeout=60,  # 60 second timeout
                check_errors=True,
                force_non_interactive=True
            )
            
            # Parse response - estimate tokens if not provided
            tokens_used = max_tokens // 2  # Estimate if not provided
            
            return stdout.strip(), tokens_used
            
        except CLIError as e:
            logger.error(f"Codex CLI error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected Codex CLI error: {e}")
            raise


class LLMClientFactory:
    """Factory for creating LLM clients with fallback support"""
    
    @staticmethod
    def create_client(config: LLMConfig) -> LLMClient:
        """Create an LLM client based on configuration"""
        if config.provider == LLMProvider.OPENAI:
            if not config.api_key:
                raise ValueError("OpenAI API key is required")
            return OpenAIClient(config)
            
        elif config.provider == LLMProvider.GEMINI:
            if not config.api_key:
                raise ValueError("Gemini API key is required")
            return GeminiClient(config)
            
        elif config.provider == LLMProvider.CODEX_CLI:
            return CodexCLIClient(config)
            
        else:
            raise ValueError(f"Unsupported LLM provider: {config.provider}")
    
    @staticmethod
    def create_with_fallback(primary_config: LLMConfig, fallback_config: Optional[LLMConfig] = None) -> LLMClient:
        """Create LLM client with fallback support"""
        
        # Try primary provider first
        try:
            logger.info(f"Attempting to create {primary_config.provider.value} client")
            return LLMClientFactory.create_client(primary_config)
        except Exception as e:
            logger.warning(f"Failed to create primary LLM client: {e}")
            
            # Try fallback if provided
            if fallback_config:
                try:
                    logger.info(f"Attempting fallback to {fallback_config.provider.value} client")
                    return LLMClientFactory.create_client(fallback_config)
                except Exception as e2:
                    logger.warning(f"Failed to create fallback LLM client: {e2}")
            
            # If both fail, try to find any working provider
            logger.info("Attempting to find any working LLM provider")
            return LLMClientFactory._find_working_provider()
    
    @staticmethod
    def _find_working_provider() -> LLMClient:
        """Find any working LLM provider in order of preference"""
        providers = [
            (LLMProvider.OPENAI, "OPENAI_API_KEY"),
            (LLMProvider.GEMINI, "GEMINI_API_KEY"),
            (LLMProvider.CODEX_CLI, None)  # No API key needed
        ]
        
        for provider, env_var in providers:
            try:
                # Check if API key is available (if needed)
                if env_var and not os.getenv(env_var):
                    logger.debug(f"No API key found for {provider.value}")
                    continue
                
                # Try to create config and client
                config = LLMConfig.from_env(provider)
                client = LLMClientFactory.create_client(config)
                
                logger.info(f"Successfully created {provider.value} client as fallback")
                return client
                
            except Exception as e:
                logger.debug(f"Failed to create {provider.value} client: {e}")
                continue
        
        # If all providers fail, raise an error
        raise RuntimeError(
            "No working LLM provider found. Please ensure at least one of the following is available:\n"
            "1. OpenAI API key in OPENAI_API_KEY environment variable\n"
            "2. Gemini API key in GEMINI_API_KEY environment variable\n"
            "3. Codex CLI installed and available in PATH"
        )
