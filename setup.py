from setuptools import setup, find_packages

setup(
    name="oppie_xyz",
    version="0.1.0",
    description="Oppie.xyz - Autonomous AI Agent Ecosystem",
    author="Oppie.xyz Team",
    packages=find_packages(),
    install_requires=[
        "protobuf>=3.19.0,<4.0.0",
        "pyzmq>=24.0.0",
        "opentelemetry-api>=1.14.0",
        "opentelemetry-sdk>=1.14.0",
        "opentelemetry-exporter-otlp>=1.14.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-benchmark",
            "pytest-cov",
            "black",
            "isort",
            "pylint",
        ],
    },
    python_requires=">=3.9",
) 