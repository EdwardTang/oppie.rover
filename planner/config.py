"""
Configuration settings for Planner/Reflector agent
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class LLMProvider(Enum):
    """Supported LLM providers"""
    OPENAI = "openai"
    GEMINI = "gemini"
    CODEX_CLI = "codex_cli"  # Using Codex CLI as abstraction


@dataclass
class ReflexionConfig:
    """Configuration for Reflexion mechanism"""
    max_rounds: int = 3
    max_tokens: int = 8000
    timeout_seconds: int = 90
    confidence_threshold: float = 0.8
    enable_circuit_breaker: bool = True


@dataclass
class LLMConfig:
    """Configuration for LLM providers"""
    provider: LLMProvider
    model: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: float = 0.7
    max_tokens: int = 4000
    timeout: int = 60
    
    @classmethod
    def from_env(cls, provider: LLMProvider) -> "LLMConfig":
        """Create config from environment variables"""
        if provider == LLMProvider.OPENAI:
            return cls(
                provider=provider,
                model=os.getenv("OPENAI_MODEL", "gpt-4-turbo-preview"),
                api_key=os.getenv("OPENAI_API_KEY"),
                base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            )
        elif provider == LLMProvider.GEMINI:
            return cls(
                provider=provider,
                model=os.getenv("GEMINI_MODEL", "gemini-2.5-pro-preview-05-06"),
                api_key=os.getenv("GEMINI_API_KEY"),
                base_url=os.getenv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta"),
            )
        elif provider == LLMProvider.CODEX_CLI:
            # Codex CLI handles auth internally via config file
            return cls(
                provider=provider,
                model=os.getenv("CODEX_MODEL", "gemini-2.5-pro-preview-05-06"),
            )
        else:
            raise ValueError(f"Unsupported provider: {provider}")


@dataclass 
class PlannerConfig:
    """Main configuration for Planner agent"""
    llm_config: LLMConfig
    reflexion_config: ReflexionConfig
    semantic_confidence_threshold: float = 0.6
    enable_knowledge_base: bool = True
    enable_semantic_negotiation: bool = True
    task_timeout_default: int = 300  # Default timeout for tasks in seconds
    
    # Messaging configuration
    messaging_adapter: str = "grpc"  # grpc, nats, zeromq
    messaging_endpoint: str = "unix:///tmp/oppie.sock"
    
    # Tracing configuration
    enable_tracing: bool = True
    service_name: str = "oppie.planner"
    
    @classmethod
    def default(cls) -> "PlannerConfig":
        """Create default configuration"""
        # Default to Codex CLI with Gemini
        provider = LLMProvider.CODEX_CLI
        if os.getenv("OPENAI_API_KEY"):
            provider = LLMProvider.OPENAI
        elif os.getenv("GEMINI_API_KEY") and not os.path.exists(os.path.expanduser("~/.codex/config.json")):
            provider = LLMProvider.GEMINI
            
        return cls(
            llm_config=LLMConfig.from_env(provider),
            reflexion_config=ReflexionConfig(),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "llm_provider": self.llm_config.provider.value,
            "llm_model": self.llm_config.model,
            "reflexion_max_rounds": self.reflexion_config.max_rounds,
            "reflexion_max_tokens": self.reflexion_config.max_tokens,
            "reflexion_timeout": self.reflexion_config.timeout_seconds,
            "semantic_confidence_threshold": self.semantic_confidence_threshold,
            "messaging_adapter": self.messaging_adapter,
            "enable_tracing": self.enable_tracing,
        } 