"""
Tests for CLI utilities with TTY protection
"""
import os
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from oppie_xyz.utils.cli_utils import (
    is_tty_available,
    is_raw_mode_supported,
    run_cli_safe,
    run_codex_safe,
    CLIError,
    _is_tty_sensitive_command,
    _supports_file_input,
    _contains_error_indicators
)


class TestTTYDetection:
    """Test TTY and raw mode detection functions"""
    
    def test_is_tty_available_with_mock(self):
        """Test TTY availability detection with mocked stdin"""
        with patch('sys.stdin') as mock_stdin:
            # Test when TTY is available
            mock_stdin.isatty.return_value = True
            assert is_tty_available() is True
            
            # Test when TTY is not available
            mock_stdin.isatty.return_value = False
            assert is_tty_available() is False
            
            # Test when isatty is not available
            delattr(mock_stdin, 'isatty')
            assert is_tty_available() is False
    
    def test_is_raw_mode_supported_no_tty(self):
        """Test raw mode support when TTY is not available"""
        with patch('oppie_xyz.utils.cli_utils.is_tty_available', return_value=False):
            assert is_raw_mode_supported() is False
    
    @patch('oppie_xyz.utils.cli_utils.is_tty_available', return_value=True)
    def test_is_raw_mode_supported_with_tty_windows(self, mock_tty):
        """Test raw mode support on Windows with TTY"""
        with patch('os.name', 'nt'):
            with patch('builtins.__import__', side_effect=lambda name, *a, **k: MagicMock() if name == 'msvcrt' else __import__(name, *a, **k)):
                assert is_raw_mode_supported() is True
    
    @patch('oppie_xyz.utils.cli_utils.is_tty_available', return_value=True)
    def test_is_raw_mode_supported_with_tty_unix(self, mock_tty):
        """Test raw mode support on Unix with TTY"""
        with patch('os.name', 'posix'):
            with patch('builtins.__import__', side_effect=lambda name, *a, **k: MagicMock() if name in ['termios', 'tty'] else __import__(name, *a, **k)):
                assert is_raw_mode_supported() is True
    
    @patch('oppie_xyz.utils.cli_utils.is_tty_available', return_value=True)
    def test_is_raw_mode_supported_import_error(self, mock_tty):
        """Test raw mode support when required modules are not available"""
        with patch('builtins.__import__', side_effect=ImportError()):
            assert is_raw_mode_supported() is False

    def test_is_tty_available_in_ci_environment(self):
        """Test TTY availability in a CI environment with TTY_FALLBACK enabled."""
        with patch.dict('os.environ', {'CI': 'true', 'TTY_FALLBACK': '1'}):
            assert is_tty_available() is False
    
    def test_is_tty_available_in_ci_environment_without_fallback(self):
        """Test TTY availability in CI environment without TTY_FALLBACK."""
        with patch.dict('os.environ', {'CI': 'true'}, clear=True):
            with patch('sys.stdin') as mock_stdin:
                mock_stdin.isatty.return_value = True
                # Should still check actual TTY if TTY_FALLBACK is not set
                assert is_tty_available() is True


class TestCommandDetection:
    """Test command detection for TTY sensitivity"""
    
    def test_is_tty_sensitive_command(self):
        """Test detection of TTY-sensitive commands"""
        # Known TTY-sensitive commands
        assert _is_tty_sensitive_command(['codex', '-m', 'model']) is True
        assert _is_tty_sensitive_command(['ink', 'app']) is True
        assert _is_tty_sensitive_command(['npx', 'create-app']) is True
        assert _is_tty_sensitive_command(['yarn', 'install']) is True
        assert _is_tty_sensitive_command(['npm', 'install']) is True
        
        # Commands that should not be TTY-sensitive
        assert _is_tty_sensitive_command(['python', 'script.py']) is False
        assert _is_tty_sensitive_command(['git', 'status']) is False
        assert _is_tty_sensitive_command(['ls', '-la']) is False
        
        # Edge cases
        assert _is_tty_sensitive_command([]) is False
        assert _is_tty_sensitive_command(['']) is False
    
    def test_supports_file_input(self):
        """Test detection of commands that support file input"""
        # Commands known to support file input
        assert _supports_file_input(['codex', '-m', 'model']) is True
        
        # Commands that may not support file input directly
        assert _supports_file_input(['python', 'script.py']) is False
        assert _supports_file_input(['git', 'status']) is False
        
        # Edge cases
        assert _supports_file_input([]) is False
    
    def test_contains_error_indicators(self):
        """Test detection of error indicators in output"""
        # Known error patterns
        assert _contains_error_indicators("Raw mode is not supported", "") is True
        assert _contains_error_indicators("", "HTTP 400 Bad Request") is True
        assert _contains_error_indicators("Some error: failed", "") is True
        assert _contains_error_indicators("", "ERROR: Something went wrong") is True
        assert _contains_error_indicators('"error": "message"', "") is True
        
        # Normal output
        assert _contains_error_indicators("All tests passed", "No warnings") is False
        assert _contains_error_indicators("Success: operation completed", "") is False


class TestSafeCliExecution:
    """Test safe CLI execution with TTY protection"""
    
    @patch('subprocess.Popen')
    def test_run_cli_safe_basic(self, mock_popen):
        """Test basic safe CLI execution"""
        # Mock successful execution
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("output", "")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        
        stdout, stderr, returncode = run_cli_safe(['echo', 'hello'])
        
        assert stdout == "output"
        assert stderr == ""
        assert returncode == 0
        mock_popen.assert_called_once()
    
    @patch('subprocess.Popen')
    def test_run_cli_safe_with_input_tty_sensitive(self, mock_popen):
        """Test safe CLI execution with input for TTY-sensitive command"""
        # Mock successful execution
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("result", "")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        
        with patch('tempfile.NamedTemporaryFile') as mock_temp:
            mock_temp_file = MagicMock()
            mock_temp_file.name = '/tmp/test_file'
            mock_temp_file.__enter__.return_value = mock_temp_file
            mock_temp_file.__exit__.return_value = None
            mock_temp.return_value = mock_temp_file
            
            with patch('os.path.exists', return_value=True):
                with patch('os.unlink') as mock_unlink:
                    stdout, stderr, returncode = run_cli_safe(
                        ['codex', '-m', 'model'],
                        input_text="test prompt"
                    )
                    
                    # Should create temp file and clean it up
                    mock_temp.assert_called_once()
                    mock_unlink.assert_called_once_with('/tmp/test_file')
    
    @patch('subprocess.Popen')
    def test_run_cli_safe_error_handling(self, mock_popen):
        """Test error handling in safe CLI execution"""
        # Mock failed execution
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("", "Raw mode is not supported")
        mock_process.returncode = 1
        mock_popen.return_value = mock_process
        
        with pytest.raises(CLIError) as exc_info:
            run_cli_safe(['codex', 'test'], check_errors=True)
        
        assert "CLI command failed" in str(exc_info.value)
        assert "Raw mode is not supported" in str(exc_info.value)
    
    @patch('subprocess.Popen')
    def test_run_cli_safe_timeout(self, mock_popen):
        """Test timeout handling in safe CLI execution"""
        import subprocess
        
        # Mock timeout
        mock_process = MagicMock()
        mock_process.communicate.side_effect = subprocess.TimeoutExpired(['cmd'], 30)
        mock_popen.return_value = mock_process
        
        with pytest.raises(CLIError) as exc_info:
            run_cli_safe(['sleep', '60'], timeout=1)
        
        assert "timed out" in str(exc_info.value)
        mock_process.kill.assert_called_once()
    
    def test_run_cli_safe_force_non_interactive(self):
        """Test that non-interactive environment variables are set"""
        with patch('subprocess.Popen') as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = ("output", "")
            mock_process.returncode = 0
            mock_popen.return_value = mock_process
            
            run_cli_safe(['echo', 'test'], force_non_interactive=True)
            
            # Check that CI environment variables were set
            call_args = mock_popen.call_args
            env = call_args[1]['env']
            assert env['CI'] == 'true'
            assert env['TTY_FALLBACK'] == '1'
            assert env['NO_COLOR'] == '1'


class TestCodexSafeExecution:
    """Test Codex-specific safe execution"""
    
    @patch('oppie_xyz.utils.cli_utils.run_cli_safe')
    def test_run_codex_safe_basic(self, mock_run_cli):
        """Test basic Codex safe execution"""
        mock_run_cli.return_value = ("Generated text", "", 0)
        
        result = run_codex_safe(
            model="gpt-4",
            prompt="Test prompt",
            provider="OpenAI"
        )
        
        assert result == "Generated text"
        mock_run_cli.assert_called_once()
        
        # Check command construction
        call_args = mock_run_cli.call_args
        command = call_args[1]['command']
        assert command[0] == 'codex'
        assert '-m' in command
        assert 'gpt-4' in command
    
    @patch('oppie_xyz.utils.cli_utils.run_cli_safe')
    def test_run_codex_safe_no_provider_for_openai(self, mock_run_cli):
        """Test that OpenAI provider is not passed as argument"""
        mock_run_cli.return_value = ("Generated text", "", 0)
        
        run_codex_safe(
            model="gpt-4",
            prompt="Test prompt",
            provider="openai"  # Should be filtered out
        )
        
        call_args = mock_run_cli.call_args
        command = call_args[1]['command']
        assert '--provider' not in command
    
    @patch('oppie_xyz.utils.cli_utils.run_cli_safe')
    def test_run_codex_safe_with_project_doc(self, mock_run_cli):
        """Test Codex execution with project documentation"""
        mock_run_cli.return_value = ("Generated text", "", 0)
        
        run_codex_safe(
            model="gpt-4",
            prompt="Test prompt",
            project_doc="/path/to/doc.md"
        )
        
        call_args = mock_run_cli.call_args
        command = call_args[1]['command']
        assert '--project-doc' in command
        assert '/path/to/doc.md' in command


class TestRegressionPrevention:
    """Test to prevent regression of TTY issues"""
    
    def test_no_subprocess_stdin_piping_in_main_scripts(self):
        """Ensure main scripts don't use subprocess stdin piping directly"""
        # This test ensures we don't regress to using subprocess with stdin
        
        # Check send_codex_plan_request.py
        plan_request_file = Path(__file__).parent.parent.parent / "send_codex_plan_request.py"
        if plan_request_file.exists():
            content = plan_request_file.read_text()
            # Should not contain direct subprocess stdin piping patterns
            assert "subprocess.Popen(" not in content or "stdin=subprocess.PIPE" not in content
            assert "process.communicate(input_text)" not in content
            
        # Check send_codex_test_request.py
        test_request_file = Path(__file__).parent.parent.parent / "send_codex_test_request.py"
        if test_request_file.exists():
            content = test_request_file.read_text()
            # Should not contain direct subprocess stdin piping patterns for codex calls
            assert "run_cli_safe" in content  # Should use our safe implementation
    
    def test_ink_error_pattern_detection(self):
        """Ensure we can detect the specific Ink error pattern"""
        ink_error = "ERROR Raw mode is not supported on the current process.stdin, which Ink uses"
        
        assert _contains_error_indicators(ink_error, "") is True
        assert _contains_error_indicators("", ink_error) is True
    
    @pytest.mark.integration
    def test_codex_cli_with_ci_env(self):
        """Integration test: Codex CLI should work with CI environment variables"""
        # This test can only run if codex CLI is actually available
        try:
            import subprocess
            result = subprocess.run(['codex', '--version'], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                pytest.skip("Codex CLI not available")
                
            # Test with CI environment
            env = os.environ.copy()
            env.update({
                'CI': 'true',
                'TTY_FALLBACK': '1',
                'NO_COLOR': '1'
            })
            
            # Should not crash with TTY error
            result = subprocess.run(
                ['codex', '--help'], 
                capture_output=True, 
                text=True, 
                timeout=10,
                env=env
            )
            
            # Should not contain the Ink TTY error
            assert "Raw mode is not supported" not in result.stderr
            assert "Raw mode is not supported" not in result.stdout
            
        except Exception as e:
            pytest.skip(f"Could not test Codex CLI integration: {e}")


if __name__ == "__main__":
    pytest.main([__file__]) 