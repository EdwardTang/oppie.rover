# Windows Setup Guide for NATS Adapter

## Overview

This guide provides Windows-specific instructions for setting up and troubleshooting the NATS JetStream adapter in the Oppie.xyz project.

## Quick Setup

### Prerequisites

- Python 3.10+ with virtual environment
- Docker Desktop (for testing) or local NATS server
- PowerShell or CMD (recommended over Git Bash)

### Installation

1. **Activate virtual environment:**
   ```powershell
   # In PowerShell
   .\.venv\Scripts\Activate.ps1
   
   # In CMD
   .venv\Scripts\activate.bat
   ```

2. **Install dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Verify installation:**
   ```powershell
   python -c "from oppie_xyz.seam_comm.adapters.nats import NatsClient; print('NATS adapter ready')"
   ```

## Common Windows Issues & Solutions

### 1. Terminal Command Issues

**Problem:** Git commands showing `[200~` prefix and `~` suffix in Git Bash

**Solution:** Use PowerShell or CMD instead of Git Bash for better compatibility:
```powershell
# Instead of Git Bash, use PowerShell:
git status
git add .
git commit -m "Your message"
```

**Alternative:** If you must use Git Bash, disable bracketed paste mode:
```bash
# Add to ~/.bashrc
bind 'set enable-bracketed-paste off'
```

### 2. Docker Issues

**Problem:** Docker daemon not running or Docker Desktop not started

**Solution:**
1. Start Docker Desktop manually
2. Wait for it to fully initialize (green indicator)
3. Test with: `docker ps`

**Alternative (Local NATS):** Install NATS server locally:
```powershell
# Download and run NATS server
curl -L https://github.com/nats-io/nats-server/releases/download/v2.10.8/nats-server-v2.10.8-windows-amd64.tar.gz -o nats-server.tar.gz
# Extract and run: nats-server.exe -js -DV -p 4222
```

### 3. Import/Dependency Issues

**Problem:** `ModuleNotFoundError` or protobuf version conflicts

**Solution:**
```powershell
# Update protobuf to compatible version
pip install "protobuf>=4.23.0,<5.0.0"

# Install missing OpenTelemetry dependencies
pip install "opentelemetry-proto>=1.22.0"

# Verify imports work
python -c "from oppie_xyz.seam_comm.telemetry.tracer import inject_trace_context; print('✅ Functions available')"
```

### 4. NATS Connection Issues

**Problem:** Cannot connect to NATS server

**Solutions:**
1. **Check if NATS is running:**
   ```powershell
   # Test connection
   curl http://localhost:8222/healthz
   ```

2. **Start NATS via Docker:**
   ```powershell
   cd oppie_xyz/seam_comm/benchmarks
   docker-compose up -d nats
   docker ps  # Verify nats container is healthy
   ```

3. **Alternative ports:** If 4222 is busy, modify docker-compose.yml to use different ports

## Testing

### Basic Functionality Test

```powershell
# Test trace context injection (should work without NATS server)
python -c "
from oppie_xyz.seam_comm.telemetry.tracer import inject_trace_context, extract_trace_context
result = inject_trace_context()
print('Trace injection test:', 'PASS' if result is None else 'UNEXPECTED')
"
```

### Full Integration Test (requires NATS server)

```powershell
# Start NATS server first
docker-compose -f oppie_xyz/seam_comm/benchmarks/docker-compose.yml up -d nats

# Run persistence tests
python -m pytest oppie_xyz/tests/seam_comm/adapters/test_nats_persistence.py -v

# Run benchmark (optional)
python oppie_xyz/seam_comm/benchmarks/nats_bench.py --count 1000 --async-publish
```

## Performance Expectations

| Environment | Latency Range | Notes |
|------------|---------------|-------|
| Windows + Docker | 3-8ms | Typical for containerized NATS |
| Windows + Local NATS | 1-3ms | Better performance, direct connection |
| Windows + WSL2 + Docker | 2-5ms | Good middle ground |

## Troubleshooting Checklist

- [ ] ✅ PowerShell/CMD works for git commands (no `[200~` prefixes)
- [ ] ✅ Virtual environment activated
- [ ] ✅ Dependencies installed (`pip list` shows nats-py, opentelemetry-api)
- [ ] ✅ NATS server accessible (`curl localhost:8222/healthz`)
- [ ] ✅ Trace functions import successfully
- [ ] ✅ Basic tests pass

## Support

For additional Windows-specific issues:
1. Check PowerShell execution policy: `Get-ExecutionPolicy`
2. Ensure Windows Defender/antivirus isn't blocking Docker/Python
3. Try running PowerShell as Administrator if permissions are issues
4. Consider using WSL2 for better Linux compatibility 