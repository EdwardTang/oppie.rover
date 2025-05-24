import os
import subprocess
from pathlib import Path


def test_node_cli_no_raw_mode(tmp_path):
    script = Path(__file__).parent / "node_raw_mode_test.js"
    env = os.environ.copy()
    env["CI"] = "1"
    result = subprocess.run([
        "node",
        str(script)
    ], input="", text=True, capture_output=True, env=env)
    assert result.returncode == 0
    assert "Raw mode is not supported" not in result.stderr
