"""
Unit tests for E2B Sandbox implementation.
"""

import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock

from oppie_xyz.executor.sandbox.e2b_sandbox import E2BSandbox
from oppie_xyz.executor.sandbox.base import SandboxError


class TestE2BSandbox:
    """Test suite for E2B sandbox implementation."""

    @pytest.fixture
    def sandbox(self):
        """Create a test E2B sandbox instance."""
        with patch('oppie_xyz.executor.sandbox.e2b_sandbox.Sandbox') as mock_e2b:
            sandbox = E2BSandbox(
                capabilities=["network", "filesystem"],
                resource_limits={"memory": "1GB", "cpu": "1"}
            )
            sandbox.E2BSandbox = mock_e2b
            return sandbox

    @pytest.fixture
    def mock_e2b_instance(self):
        """Create a mock E2B sandbox instance."""
        mock = Mock()
        mock.run_code.return_value = Mock(
            stdout="Hello World",
            stderr="",
            error=None,
            logs=Mock(stdout="", stderr="")
        )
        mock.filesystem = Mock()
        mock.close = Mock()
        return mock

    @pytest.mark.asyncio
    async def test_prepare_success(self, sandbox, mock_e2b_instance):
        """Test successful sandbox preparation."""
        sandbox.E2BSandbox.create.return_value = mock_e2b_instance
        
        await sandbox.prepare()
        
        assert sandbox.sandbox is not None
        assert sandbox.session_start_time is not None
        sandbox.E2BSandbox.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_prepare_with_template(self, mock_e2b_instance):
        """Test sandbox preparation with custom template."""
        with patch('oppie_xyz.executor.sandbox.e2b_sandbox.Sandbox') as mock_e2b:
            sandbox = E2BSandbox(template_id="custom-template")
            sandbox.E2BSandbox = mock_e2b
            mock_e2b.create.return_value = mock_e2b_instance
            
            await sandbox.prepare()
            
            mock_e2b.create.assert_called_once_with(template="custom-template")

    @pytest.mark.asyncio
    async def test_prepare_failure(self, sandbox):
        """Test sandbox preparation failure."""
        sandbox.E2BSandbox.create.side_effect = Exception("Connection failed")
        
        with pytest.raises(SandboxError, match="Failed to create E2B sandbox"):
            await sandbox.prepare()

    @pytest.mark.asyncio
    async def test_execute_command_success(self, sandbox, mock_e2b_instance):
        """Test successful command execution."""
        sandbox.sandbox = mock_e2b_instance
        
        result = await sandbox.execute_command("print('Hello World')")
        
        assert result == "Hello World"
        mock_e2b_instance.run_code.assert_called_once_with("print('Hello World')", timeout=300)

    @pytest.mark.asyncio
    async def test_execute_command_with_timeout(self, sandbox, mock_e2b_instance):
        """Test command execution with custom timeout."""
        sandbox.sandbox = mock_e2b_instance
        
        await sandbox.execute_command("long_running_task()", timeout=600)
        
        mock_e2b_instance.run_code.assert_called_once_with("long_running_task()", timeout=600)

    @pytest.mark.asyncio
    async def test_execute_command_with_error(self, sandbox, mock_e2b_instance):
        """Test command execution with error."""
        sandbox.sandbox = mock_e2b_instance
        mock_e2b_instance.run_code.return_value = Mock(
            stdout="",
            stderr="",
            error="Python syntax error",
            logs=Mock(stdout="", stderr="")
        )
        
        with pytest.raises(SandboxError, match="Execution error: Python syntax error"):
            await sandbox.execute_command("invalid syntax")

    @pytest.mark.asyncio
    async def test_execute_command_not_initialized(self, sandbox):
        """Test command execution on uninitialized sandbox."""
        with pytest.raises(SandboxError, match="Sandbox not initialized"):
            await sandbox.execute_command("print('test')")

    @pytest.mark.asyncio
    async def test_cleanup_success(self, sandbox, mock_e2b_instance):
        """Test successful sandbox cleanup."""
        sandbox.sandbox = mock_e2b_instance
        sandbox.session_start_time = 1234567890
        
        await sandbox.cleanup()
        
        mock_e2b_instance.close.assert_called_once()
        assert sandbox.sandbox is None
        assert sandbox.session_start_time is None

    @pytest.mark.asyncio
    async def test_cleanup_with_error(self, sandbox, mock_e2b_instance):
        """Test sandbox cleanup with error."""
        sandbox.sandbox = mock_e2b_instance
        mock_e2b_instance.close.side_effect = Exception("Cleanup failed")
        
        # Should not raise exception, but log error
        await sandbox.cleanup()
        
        assert sandbox.sandbox is None

    def test_get_session_info(self, sandbox, mock_e2b_instance):
        """Test session information retrieval."""
        sandbox.sandbox = mock_e2b_instance
        sandbox.session_start_time = 1234567890
        
        info = sandbox.get_session_info()
        
        assert info["sandbox_type"] == "e2b"
        assert info["is_active"] is True
        assert info["capabilities"] == ["network", "filesystem"]
        assert "session_duration" in info
        assert "session_start_time" in info

    def test_get_session_info_inactive(self, sandbox):
        """Test session information for inactive sandbox."""
        info = sandbox.get_session_info()
        
        assert info["sandbox_type"] == "e2b"
        assert info["is_active"] is False
        assert "session_duration" not in info

    @pytest.mark.asyncio
    async def test_upload_file_success(self, sandbox, mock_e2b_instance):
        """Test successful file upload."""
        sandbox.sandbox = mock_e2b_instance
        
        with patch('builtins.open', mock_open(read_data=b'file content')):
            await sandbox.upload_file("/local/file.txt", "/remote/file.txt")
            
        mock_e2b_instance.filesystem.write.assert_called_once_with("/remote/file.txt", b'file content')

    @pytest.mark.asyncio
    async def test_download_file_success(self, sandbox, mock_e2b_instance):
        """Test successful file download."""
        sandbox.sandbox = mock_e2b_instance
        mock_e2b_instance.filesystem.read.return_value = b'downloaded content'
        
        with patch('builtins.open', mock_open()) as mock_file:
            await sandbox.download_file("/remote/file.txt", "/local/file.txt")
            
        mock_file.assert_called_once_with("/local/file.txt", 'wb')
        mock_file().write.assert_called_once_with(b'downloaded content')

    @pytest.mark.asyncio
    async def test_install_capabilities_python_packages(self, sandbox, mock_e2b_instance):
        """Test installation of Python packages capability."""
        sandbox.sandbox = mock_e2b_instance
        sandbox.capabilities = ["python-packages"]
        
        await sandbox._install_capabilities()
        
        # Should have called run_code for pip commands
        assert mock_e2b_instance.run_code.call_count >= 2

    @pytest.mark.asyncio
    async def test_install_capabilities_gpu(self, sandbox, mock_e2b_instance):
        """Test installation of GPU capability."""
        sandbox.sandbox = mock_e2b_instance
        sandbox.capabilities = ["gpu"]
        
        await sandbox._install_capabilities()
        
        # Should have called run_code for nvidia-smi check
        mock_e2b_instance.run_code.assert_called_with("nvidia-smi || echo 'GPU not available'", timeout=300)

    @pytest.mark.asyncio
    async def test_install_capabilities_unknown(self, sandbox, mock_e2b_instance):
        """Test handling of unknown capabilities."""
        sandbox.sandbox = mock_e2b_instance
        sandbox.capabilities = ["unknown-capability"]
        
        # Should not raise exception, just log warning
        await sandbox._install_capabilities()
        
        # Should not have called run_code
        mock_e2b_instance.run_code.assert_not_called()

    def test_import_error_handling(self):
        """Test graceful handling of missing E2B dependency."""
        with patch('builtins.__import__', side_effect=ImportError):
            with pytest.raises(SandboxError, match="E2B code interpreter not available"):
                E2BSandbox()


def mock_open(read_data=''):
    """Helper function to create a mock open function."""
    from unittest.mock import mock_open as original_mock_open
    return original_mock_open(read_data=read_data) 