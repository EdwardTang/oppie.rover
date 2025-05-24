#!/usr/bin/env python3
"""
Test runner script to execute tests and produce a report.
"""
import sys
import subprocess
import datetime
import json

def main():
    """Run tests and generate report"""
    print("Starting test report generation...")
    results = {
        "timestamp": datetime.datetime.now().isoformat(),
        "unit_tests": run_unit_tests(),
        "integration_tests": {
            "status": "skipped",
            "message": "Integration tests require Docker environment which is not properly configured"
        },
        "summary": {}
    }
    
    # Summarize results
    results["summary"]["all_passed"] = results["unit_tests"]["success"]
    results["summary"]["total_tests"] = results["unit_tests"].get("total_tests", 0)
    results["summary"]["passed_tests"] = results["unit_tests"].get("passed_tests", 0)
    results["summary"]["failed_tests"] = results["unit_tests"].get("failed_tests", 0)
    
    # Print report
    print("\n===== TEST REPORT =====")
    print(f"Timestamp: {results['timestamp']}")
    
    print("\n--- Unit Tests ---")
    print(f"Status: {'PASSED' if results['unit_tests']['success'] else 'FAILED'}")
    print(f"Total: {results['unit_tests'].get('total_tests', 'N/A')}")
    print(f"Passed: {results['unit_tests'].get('passed_tests', 'N/A')}")
    print(f"Failed: {results['unit_tests'].get('failed_tests', 'N/A')}")
    
    if not results["unit_tests"]["success"]:
        print("\nFailed tests:")
        for test in results["unit_tests"].get("failed_test_list", []):
            print(f"  - {test}")
    
    print("\n--- Integration Tests ---")
    print(f"Status: {results['integration_tests']['status']}")
    print(f"Message: {results['integration_tests']['message']}")
    
    print("\n--- Overall Summary ---")
    print(f"All tests passed: {results['summary']['all_passed']}")
    
    # Save report to file
    with open("test_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nReport saved to test_report.json")
    
    return 0 if results["summary"]["all_passed"] else 1

def run_unit_tests():
    """Run unit tests that don't depend on external services"""
    print("Running unit tests...")
    
    # Exclude integration tests by selecting specific directories
    cmd = ["python", "-m", "pytest", "oppie_xyz", "-v", "--collect-only"]
    
    try:
        # First collect test information
        proc = subprocess.run(cmd, capture_output=True, text=True)
        test_output = proc.stdout + proc.stderr
        
        # Count total tests
        collected_line = [line for line in test_output.split('\n') if "collected" in line and "item" in line]
        total_tests = 0
        if collected_line:
            parts = collected_line[0].split()
            for part in parts:
                if part.isdigit():
                    total_tests = int(part)
                    break
        
        # Now run tests that we can run without external dependencies
        print(f"Found {total_tests} tests to run")
        
        # Exclude problematic tests that require external services
        exclude_pattern = "integration or zeromq or nats_persistence or grpc"
        cmd = ["python", "-m", "pytest", "oppie_xyz", "-v", "-k", f"not ({exclude_pattern})"]
        
        proc = subprocess.run(cmd, capture_output=True, text=True)
        output = proc.stdout + proc.stderr
        
        # Extract test results
        passed_count = output.count("PASSED")
        failed_count = output.count("FAILED")
        error_count = output.count("ERROR")
        
        actual_run_tests = passed_count + failed_count + error_count
        
        # Extract failed test names
        failed_test_list = []
        for line in output.split('\n'):
            if "FAILED" in line or "ERROR" in line:
                test_name = line.split()[0]
                if "::" in test_name:
                    failed_test_list.append(test_name)
        
        result = {
            "success": failed_count == 0 and error_count == 0,
            "total_tests": actual_run_tests,
            "passed_tests": passed_count,
            "failed_tests": failed_count + error_count,
            "raw_output": output,
            "failed_test_list": failed_test_list
        }
        
        return result
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "raw_output": f"Exception: {e}"
        }

if __name__ == "__main__":
    sys.exit(main()) 