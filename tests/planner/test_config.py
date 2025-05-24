"""
Tests for planner configuration
"""
import os
import pytest
from unittest.mock import patch

from oppie_xyz.planner.config import (
    LLMProvider,
    LLMConfig,
    ReflexionConfig,
    PlannerConfig
)


class TestLLMConfig:
    """Test LLM configuration"""
    
    def test_openai_config_from_env(self):
        """Test OpenAI config creation from environment"""
        with patch.dict(os.environ, {
            "OPENAI_API_KEY": "test-key-123",
            "OPENAI_MODEL": "gpt-4-turbo",
            "OPENAI_BASE_URL": "https://custom.openai.com/v1"
        }):
            config = LLMConfig.from_env(LLMProvider.OPENAI)
            assert config.provider == LLMProvider.OPENAI
            assert config.api_key == "test-key-123"
            assert config.model == "gpt-4-turbo"
            assert config.base_url == "https://custom.openai.com/v1"
            
    def test_gemini_config_from_env(self):
        """Test Gemini config creation from environment"""
        with patch.dict(os.environ, {
            "GEMINI_API_KEY": "gemini-key-456",
            "GEMINI_MODEL": "gemini-pro",
            "GEMINI_BASE_URL": "https://custom.google.com/v1"
        }):
            config = LLMConfig.from_env(LLMProvider.GEMINI)
            assert config.provider == LLMProvider.GEMINI
            assert config.api_key == "gemini-key-456"
            assert config.model == "gemini-pro"
            assert config.base_url == "https://custom.google.com/v1"
            
    def test_codex_cli_config_from_env(self):
        """Test Codex CLI config creation from environment"""
        with patch.dict(os.environ, {
            "CODEX_MODEL": "o3-mini"
        }):
            config = LLMConfig.from_env(LLMProvider.CODEX_CLI)
            assert config.provider == LLMProvider.CODEX_CLI
            assert config.model == "o3-mini"
            assert config.api_key is None  # Codex CLI handles auth internally
            
    def test_unsupported_provider(self):
        """Test error on unsupported provider"""
        with pytest.raises(ValueError, match="Unsupported provider"):
            LLMConfig.from_env("unsupported")  # type: ignore


class TestReflexionConfig:
    """Test Reflexion configuration"""
    
    def test_default_values(self):
        """Test default Reflexion config values"""
        config = ReflexionConfig()
        assert config.max_rounds == 3
        assert config.max_tokens == 8000
        assert config.timeout_seconds == 90
        assert config.confidence_threshold == 0.8
        assert config.enable_circuit_breaker is True
        
    def test_custom_values(self):
        """Test custom Reflexion config values"""
        config = ReflexionConfig(
            max_rounds=5,
            max_tokens=10000,
            timeout_seconds=120,
            confidence_threshold=0.9,
            enable_circuit_breaker=False
        )
        assert config.max_rounds == 5
        assert config.max_tokens == 10000
        assert config.timeout_seconds == 120
        assert config.confidence_threshold == 0.9
        assert config.enable_circuit_breaker is False


class TestPlannerConfig:
    """Test main planner configuration"""
    
    def test_default_config_with_codex(self):
        """Test default config creation with Codex CLI"""
        with patch.dict(os.environ, clear=True):
            with patch("os.path.exists", return_value=True):  # Simulate codex config exists
                config = PlannerConfig.default()
                assert config.llm_config.provider == LLMProvider.CODEX_CLI
                
    def test_default_config_with_openai(self):
        """Test default config creation with OpenAI when API key exists"""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            config = PlannerConfig.default()
            assert config.llm_config.provider == LLMProvider.OPENAI
            
    def test_default_config_with_gemini(self):
        """Test default config creation with Gemini when API key exists"""
        with patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}, clear=True):
            with patch("os.path.exists", return_value=False):  # No codex config
                config = PlannerConfig.default()
                assert config.llm_config.provider == LLMProvider.GEMINI
                
    def test_config_to_dict(self):
        """Test config serialization to dictionary"""
        config = PlannerConfig.default()
        config_dict = config.to_dict()
        
        assert "llm_provider" in config_dict
        assert "llm_model" in config_dict
        assert "reflexion_max_rounds" in config_dict
        assert "reflexion_max_tokens" in config_dict
        assert "reflexion_timeout" in config_dict
        assert "semantic_confidence_threshold" in config_dict
        assert "messaging_adapter" in config_dict
        assert "enable_tracing" in config_dict
        
    def test_custom_config_values(self):
        """Test custom planner config values"""
        llm_config = LLMConfig(
            provider=LLMProvider.OPENAI,
            model="gpt-4",
            api_key="test-key"
        )
        reflexion_config = ReflexionConfig(max_rounds=5)
        
        config = PlannerConfig(
            llm_config=llm_config,
            reflexion_config=reflexion_config,
            semantic_confidence_threshold=0.7,
            enable_knowledge_base=False,
            enable_semantic_negotiation=False,
            task_timeout_default=600,
            messaging_adapter="nats",
            messaging_endpoint="nats://localhost:4222",
            enable_tracing=False,
            service_name="test-planner"
        )
        
        assert config.llm_config.provider == LLMProvider.OPENAI
        assert config.reflexion_config.max_rounds == 5
        assert config.semantic_confidence_threshold == 0.7
        assert config.enable_knowledge_base is False
        assert config.enable_semantic_negotiation is False
        assert config.task_timeout_default == 600
        assert config.messaging_adapter == "nats"
        assert config.messaging_endpoint == "nats://localhost:4222"
        assert config.enable_tracing is False
        assert config.service_name == "test-planner" 