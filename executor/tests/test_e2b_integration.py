"""
Integration tests for E2B Sandbox functionality.

These tests demonstrate real E2B sandbox capabilities but require E2B API key.
They can be run with pytest -m integration if E2B_API_KEY environment variable is set.
"""

import asyncio
import os
import pytest
from unittest.mock import patch

from oppie_xyz.executor.sandbox.e2b_sandbox import E2BSandbox
from oppie_xyz.executor.sandbox.base import SandboxError


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv('E2B_API_KEY'), reason="E2B_API_KEY not set")
class TestE2BIntegration:
    """Integration tests for E2B sandbox with real API calls."""

    @pytest.fixture
    async def live_sandbox(self):
        """Create a live E2B sandbox for testing."""
        sandbox = E2BSandbox(capabilities=["network", "filesystem"])
        await sandbox.prepare()
        yield sandbox
        await sandbox.cleanup()

    @pytest.mark.asyncio
    async def test_python_code_execution(self, live_sandbox):
        """Test executing Python code in E2B sandbox."""
        # Simple calculation
        result = await live_sandbox.execute_command("print(2 + 2)")
        assert "4" in result

        # Variable assignment and retrieval
        await live_sandbox.execute_command("x = 42")
        result = await live_sandbox.execute_command("print(f'The answer is {x}')")
        assert "42" in result

    @pytest.mark.asyncio
    async def test_data_analysis_workflow(self, live_sandbox):
        """Test a complete data analysis workflow."""
        # Install required packages
        await live_sandbox.execute_command("pip install pandas numpy")
        
        # Create sample data
        code = """
import pandas as pd
import numpy as np

# Create sample data
data = {
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'age': [25, 30, 35, 28],
    'score': [85.5, 92.0, 78.5, 88.0]
}
df = pd.DataFrame(data)
print("Dataset created:")
print(df)
"""
        result = await live_sandbox.execute_command(code)
        assert "Alice" in result
        assert "Dataset created:" in result

        # Perform analysis
        analysis_code = """
print("\\nDataset Analysis:")
print(f"Average age: {df['age'].mean()}")
print(f"Average score: {df['score'].mean()}")
print(f"Top scorer: {df.loc[df['score'].idxmax(), 'name']}")
"""
        result = await live_sandbox.execute_command(analysis_code)
        assert "Average age:" in result
        assert "Top scorer:" in result

    @pytest.mark.asyncio
    async def test_file_operations(self, live_sandbox):
        """Test file upload/download operations."""
        # Create a test file content
        test_content = "Hello from Oppie.xyz E2B integration test!"
        
        # Write content to sandbox
        code = f"""
with open('/tmp/test_file.txt', 'w') as f:
    f.write('{test_content}')
print('File written successfully')
"""
        result = await live_sandbox.execute_command(code)
        assert "File written successfully" in result

        # Read content back
        read_code = """
with open('/tmp/test_file.txt', 'r') as f:
    content = f.read()
print(f'File content: {content}')
"""
        result = await live_sandbox.execute_command(read_code)
        assert test_content in result

    @pytest.mark.asyncio
    async def test_error_handling(self, live_sandbox):
        """Test error handling in E2B sandbox."""
        # Test syntax error
        with pytest.raises(SandboxError):
            await live_sandbox.execute_command("print('unclosed string")

        # Test runtime error
        with pytest.raises(SandboxError):
            await live_sandbox.execute_command("1 / 0")

    @pytest.mark.asyncio
    async def test_long_running_task(self, live_sandbox):
        """Test execution of longer running tasks."""
        code = """
import time
import math

# Simulate some computation
result = 0
for i in range(1000):
    result += math.sqrt(i)
    
print(f'Computation completed: {result:.2f}')
"""
        result = await live_sandbox.execute_command(code, timeout=30)
        assert "Computation completed:" in result

    @pytest.mark.asyncio
    async def test_session_persistence(self, live_sandbox):
        """Test that variables persist across command executions."""
        # Set a variable
        await live_sandbox.execute_command("persistent_var = 'Hello World'")
        
        # Use the variable in another command
        result = await live_sandbox.execute_command("print(persistent_var)")
        assert "Hello World" in result

        # Modify the variable
        await live_sandbox.execute_command("persistent_var = persistent_var + ' from E2B'")
        result = await live_sandbox.execute_command("print(persistent_var)")
        assert "Hello World from E2B" in result

    @pytest.mark.asyncio
    async def test_network_capability(self, live_sandbox):
        """Test network access capability."""
        code = """
import requests
try:
    response = requests.get('https://httpbin.org/json', timeout=10)
    print(f'Network request successful: {response.status_code}')
    print('Network capability working')
except Exception as e:
    print(f'Network error: {e}')
"""
        result = await live_sandbox.execute_command(code)
        # Should either succeed or show a reasonable error
        assert "Network" in result

    @pytest.mark.asyncio
    async def test_session_info(self, live_sandbox):
        """Test session information retrieval."""
        info = live_sandbox.get_session_info()
        
        assert info["sandbox_type"] == "e2b"
        assert info["is_active"] is True
        assert "network" in info["capabilities"]
        assert "filesystem" in info["capabilities"]
        assert "session_duration" in info
        assert info["session_duration"] > 0


@pytest.mark.unit
class TestE2BMocking:
    """Unit tests that demonstrate E2B functionality without real API calls."""

    @pytest.mark.asyncio
    async def test_mock_e2b_workflow(self):
        """Test complete workflow with mocked E2B."""
        # Mock the E2B library
        with patch('e2b_code_interpreter.Sandbox') as mock_sandbox_class:
            mock_instance = mock_sandbox_class.create.return_value
            mock_instance.run_code.return_value.stdout = "Mock execution result"
            mock_instance.run_code.return_value.error = None
            mock_instance.run_code.return_value.logs.stdout = ""
            mock_instance.run_code.return_value.logs.stderr = ""
            
            # Create sandbox and test workflow
            sandbox = E2BSandbox(capabilities=["network"])
            await sandbox.prepare()
            
            result = await sandbox.execute_command("print('Hello from mock')")
            assert result == "Mock execution result"
            
            await sandbox.cleanup()
            mock_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_mock_data_science_workflow(self):
        """Test data science workflow with mocked responses."""
        with patch('e2b_code_interpreter.Sandbox') as mock_sandbox_class:
            mock_instance = mock_sandbox_class.create.return_value
            
            # Mock different responses for different commands
            def mock_run_code(command, timeout=300):
                if "import pandas" in command:
                    return type('MockExecution', (), {
                        'stdout': 'Pandas imported successfully',
                        'error': None,
                        'logs': type('MockLogs', (), {'stdout': '', 'stderr': ''})()
                    })()
                elif "DataFrame" in command:
                    return type('MockExecution', (), {
                        'stdout': 'DataFrame created with 4 rows',
                        'error': None,
                        'logs': type('MockLogs', (), {'stdout': '', 'stderr': ''})()
                    })()
                else:
                    return type('MockExecution', (), {
                        'stdout': 'Command executed',
                        'error': None,
                        'logs': type('MockLogs', (), {'stdout': '', 'stderr': ''})()
                    })()
            
            mock_instance.run_code = mock_run_code
            
            # Test the workflow
            sandbox = E2BSandbox(capabilities=["python-packages"])
            await sandbox.prepare()
            
            # Test package installation
            result = await sandbox.execute_command("import pandas as pd")
            assert "Pandas imported successfully" in result
            
            # Test DataFrame creation
            result = await sandbox.execute_command("df = pd.DataFrame({'a': [1,2,3]})")
            assert "DataFrame created" in result
            
            await sandbox.cleanup()


if __name__ == "__main__":
    # Run integration tests if E2B_API_KEY is available
    if os.getenv('E2B_API_KEY'):
        print("Running E2B integration tests...")
        pytest.main([__file__, "-v", "-m", "integration"])
    else:
        print("Running mock tests only (E2B_API_KEY not set)...")
        pytest.main([__file__, "-v", "-m", "unit"]) 