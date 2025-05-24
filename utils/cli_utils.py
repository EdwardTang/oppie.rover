"""
CLI utilities for safe command execution with TTY detection.

This module provides utilities to safely execute CLI commands that may have
TTY requirements (like Ink-based CLIs) by avoiding stdin piping issues.
"""
import os
import sys
import subprocess
import tempfile
import logging
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class CLIError(RuntimeError):
    """Raised when a CLI command returns a non-zero exit code or error output."""


def is_tty_available() -> bool:
    """Check if TTY mode is available for the current process."""
    # Explicitly bypass TTY detection in CI environments
    if os.environ.get('CI') == 'true' and os.environ.get('TTY_FALLBACK') == '1':
        return False
    return sys.stdin.isatty() if hasattr(sys.stdin, 'isatty') else False


def is_raw_mode_supported() -> bool:
    """Check if raw mode is supported (similar to Ink's isRawModeSupported)."""
    if not is_tty_available():
        return False
    
    try:
        # On Windows, check if we can access terminal control
        if os.name == 'nt':
            import msvcrt
            return True
        else:
            # On Unix-like systems, check termios
            import termios
            import tty
            return True
    except (ImportError, AttributeError):
        return False


def run_cli_safe(
    command: List[str],
    input_text: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    check_errors: bool = True,
    force_non_interactive: bool = True
) -> Tuple[str, str, int]:
    """
    Safely run a CLI command without TTY issues.
    
    Args:
        command: List of command components (e.g., ["codex", "-m", "model"])
        input_text: Text to pass as input (will use temp file if needed)
        env: Environment variables to add/override
        timeout: Command timeout in seconds
        check_errors: Whether to raise CLIError on non-zero exit codes
        force_non_interactive: Whether to force non-interactive mode
        
    Returns:
        Tuple of (stdout, stderr, return_code)
        
    Raises:
        CLIError: If check_errors=True and command fails
    """
    # Prepare environment
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)
    
    # Add CI environment variables to force non-interactive mode
    if force_non_interactive:
        cmd_env.update({
            'CI': 'true',
            'TTY_FALLBACK': '1',
            'NO_COLOR': '1',  # Disable colored output
            'FORCE_COLOR': '0'  # Disable colored output
        })
    
    temp_file_path = None
    
    try:
        # If we have input text and the command might be TTY-sensitive
        if input_text and _is_tty_sensitive_command(command):
            # Check if command supports file input
            if _supports_file_input(command):
                # Create temporary file for input
                with tempfile.NamedTemporaryFile(
                    mode='w', 
                    suffix='.txt', 
                    delete=False, 
                    encoding='utf-8'
                ) as tmp_file:
                    tmp_file.write(input_text)
                    tmp_file.flush()
                    temp_file_path = tmp_file.name
                
                # Add file input argument
                command = command + ['--prompt-file', temp_file_path]
                input_text = None  # Don't use stdin
            else:
                # Use environment variable fallback
                cmd_env['CLI_INPUT'] = input_text
                cmd_env['CODEX_PROMPT'] = input_text  # Specific to Codex CLI
                input_text = None  # Don't use stdin
        
        # Run the command
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE if input_text else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            env=cmd_env,
            shell=os.name == 'nt'  # Use shell on Windows for better compatibility
        )
        
        stdout, stderr = process.communicate(input_text, timeout=timeout)
        
        # Log the execution
        logger.debug(f"Executed command: {' '.join(command)}")
        logger.debug(f"Return code: {process.returncode}")
        if stderr:
            logger.debug(f"Stderr: {stderr}")
        
        # Check for errors
        if check_errors and (
            process.returncode != 0 or 
            _contains_error_indicators(stdout, stderr)
        ):
            error_msg = (
                f"CLI command failed with exit code {process.returncode}.\n"
                f"Command: {' '.join(command)}\n"
                f"STDERR: {stderr}\n"
                f"STDOUT: {stdout}"
            )
            raise CLIError(error_msg)
        
        return stdout, stderr, process.returncode
        
    except subprocess.TimeoutExpired:
        process.kill()
        raise CLIError(f"Command timed out after {timeout} seconds: {' '.join(command)}")
    
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except OSError as e:
                logger.warning(f"Failed to delete temp file {temp_file_path}: {e}")


def _is_tty_sensitive_command(command: List[str]) -> bool:
    """Check if a command is known to be TTY-sensitive (like Ink-based CLIs)."""
    if not command:
        return False
    
    # Known TTY-sensitive commands
    tty_sensitive_commands = {
        'codex',  # OpenAI Codex CLI (uses Ink)
        'ink',    # Direct Ink usage
        'npx',    # NPX might run Ink-based tools
        'yarn',   # Yarn can use Ink for interactive features
        'npm'     # NPM can use Ink for interactive features
    }
    
    cmd_name = Path(command[0]).name.lower()
    return cmd_name in tty_sensitive_commands


def _supports_file_input(command: List[str]) -> bool:
    """Check if a command supports file-based input."""
    if not command:
        return False
    
    cmd_name = Path(command[0]).name.lower()
    
    # Commands known to support file input
    file_input_commands = {
        'codex': ['--prompt-file', '--file', '-f'],
        'npm': ['--'],
        'yarn': ['--']
    }
    
    return cmd_name in file_input_commands


def _contains_error_indicators(stdout: str, stderr: str) -> bool:
    """Check if output contains error indicators."""
    error_patterns = [
        '"error"',
        'Raw mode is not supported',
        'HTTP 400',
        'HTTP 500',
        'Error:',
        'ERROR:',
        'FAILED:',
        'Exception:'
    ]
    
    combined_output = f"{stdout}\n{stderr}".lower()
    return any(pattern.lower() in combined_output for pattern in error_patterns)


def run_codex_safe(
    model: str,
    prompt: str,
    provider: Optional[str] = None,
    project_doc: Optional[str] = None,
    max_tokens: int = 1000,
    **kwargs
) -> str:
    """
    Safely run Codex CLI with TTY protection.
    
    Args:
        model: Model name (e.g., "o3", "gpt-4")
        prompt: Input prompt text
        provider: Provider name (e.g., "OpenAI", "Gemini")
        project_doc: Path to project documentation file
        max_tokens: Maximum tokens to generate
        **kwargs: Additional arguments passed to run_cli_safe
        
    Returns:
        Generated text from Codex
        
    Raises:
        CLIError: If Codex execution fails
    """
    # Build command
    cmd = ["codex", "-a", "full-auto", "-m", model]
    
    # Add provider if specified (but not for OpenAI as it's default)
    if provider and provider.lower() != "openai":
        cmd.extend(["--provider", provider])
    
    # Add project doc if specified
    if project_doc:
        cmd.extend(["--project-doc", str(project_doc)])
    
    # Add max tokens if supported
    if max_tokens != 1000:  # Only add if non-default
        cmd.extend(["--max-tokens", str(max_tokens)])
    
    # Execute with TTY protection
    stdout, stderr, return_code = run_cli_safe(
        command=cmd,
        input_text=prompt,
        **kwargs
    )
    
    return stdout.strip() 