#!/usr/bin/env python
"""
Protobuf Code Generation Script

Automatically generates Python code from .proto files, including regular Protobuf and gRPC services.
"""

import os
import sys
import subprocess
import re
from pathlib import Path
import traceback

def main():
    try:
        # Get the directory where the current script is located
        proto_dir = Path(__file__).parent.absolute()
        
        # Project root directory
        project_root = proto_dir.parent.parent.parent
        
        print(f"Project root directory: {project_root}")
        print(f"Proto directory: {proto_dir}")
        
        # Ensure output directory exists
        os.makedirs(proto_dir, exist_ok=True)
        
        # Get all .proto files
        proto_files = list(proto_dir.glob("*.proto"))
        print(f"Found {len(proto_files)} proto files: {[p.name for p in proto_files]}")
        
        # Check if grpc_tools is installed
        try:
            import grpc_tools.protoc  # noqa: F401
            print("Found grpc_tools module")
        except ImportError:
            print("Error: grpc_tools not installed. Please install using pip install grpcio-tools.")
            sys.exit(1)
        
        # Generate code for each proto file
        for proto_file in proto_files:
            print(f"\nProcessing {proto_file.name}...")
            
            # Use grpc_tools.protoc to generate regular protobuf code and gRPC code (if applicable)
            grpc_args = [
                "python", "-m", "grpc_tools.protoc",
                f"--proto_path={proto_dir}",
                f"--python_out={proto_dir}",
                f"{proto_file.name}"
            ]
            
            # If it's rpc_service.proto, add grpc_python_out parameter
            if proto_file.name == "rpc_service.proto":
                grpc_args.append(f"--grpc_python_out={proto_dir}")
            
            try:
                print(f"Executing command: {' '.join(grpc_args)}")
                result = subprocess.run(grpc_args, check=True, capture_output=True, text=True)
                print(f"Command output: {result.stdout}")
                
                if result.stderr:
                    print(f"Error output: {result.stderr}")
                
                # Generated pb2 file
                pb2_file = proto_dir / f"{proto_file.stem}_pb2.py"
                
                # Fix imports in pb2 file
                if pb2_file.exists():
                    fix_imports(pb2_file)
                
                # If gRPC file was generated, fix its imports
                if proto_file.name == "rpc_service.proto":
                    grpc_file = proto_dir / f"{proto_file.stem}_pb2_grpc.py"
                    if grpc_file.exists():
                        fix_imports(grpc_file)
                    print(f"✅ Successfully generated protobuf and gRPC code for {proto_file.name}")
                else:
                    print(f"✅ Successfully generated protobuf code for {proto_file.name}")
            except subprocess.SubprocessError as e:
                print(f"❌ Failed to generate code for {proto_file.name}: {e}")
                if hasattr(e, 'stderr'):
                    print(f"Error details: {e.stderr}")
        
        print("\nAll proto files processed!")
    except Exception as e:
        print(f"Error occurred during script execution: {e}")
        traceback.print_exc()

def fix_imports(file_path):
    """Fix import path issues in generated protobuf/gRPC code"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find patterns that directly import other pb2 modules
    import_pattern = r'^import ([a-zA-Z0-9_]+)_pb2'
    
    # Replace found imports with relative imports
    fixed_content = re.sub(
        import_pattern, 
        r'from . import \1_pb2',
        content,
        flags=re.MULTILINE
    )
    
    # Check if there were modifications
    if fixed_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        print(f"✅ Fixed import paths in {file_path.name}")
    else:
        print(f"ℹ️ {file_path.name} doesn't need import path fixes")

if __name__ == "__main__":
    main() 