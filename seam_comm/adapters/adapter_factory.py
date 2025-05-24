"""
适配器工厂

用于创建和管理通信适配器实例（ZeroMQ、gRPC、NATS等）。
提供统一的接口来根据配置或运行时需求选择适当的适配器。
"""

from typing import Dict, Any

from oppie_xyz.seam_comm.adapters.adapter_interface import ClientAdapterInterface, ServerAdapterInterface
from oppie_xyz.seam_comm.adapters.zeromq.client import ZeroMQClient
from oppie_xyz.seam_comm.adapters.zeromq.server import ZeroMQServer

# 引入gRPC适配器实现
from oppie_xyz.seam_comm.adapters.grpc.client import GrpcClient
from oppie_xyz.seam_comm.adapters.grpc.server import GrpcServer

# 引入NATS JetStream适配器实现
from oppie_xyz.seam_comm.adapters.nats.client import NatsClient
from oppie_xyz.seam_comm.adapters.nats.server import NatsServer

class AdapterType:
    """适配器类型常量"""
    ZEROMQ = "zeromq"
    GRPC = "grpc"
    NATS = "nats"

class AdapterFactory:
    """适配器工厂，用于创建通信适配器实例"""
    
    @staticmethod
    def create_client(adapter_type: str, config: Dict[str, Any] = None) -> ClientAdapterInterface:
        """创建客户端适配器
        
        Args:
            adapter_type: 适配器类型，如"zeromq"、"grpc"、"nats"
            config: 适配器配置参数
            
        Returns:
            ClientAdapterInterface: 客户端适配器实例
            
        Raises:
            ValueError: 无效的适配器类型
        """
        if config is None:
            config = {}
            
        if adapter_type.lower() == AdapterType.ZEROMQ:
            return ZeroMQClient(
                server_address=config.get("server_address", "tcp://localhost:5555"),
                timeout_ms=config.get("timeout_ms", 5000)
            )
        elif adapter_type.lower() == AdapterType.GRPC:
            return GrpcClient(
                server_address=config.get("server_address", "localhost:50051"),
                timeout_ms=config.get("timeout_ms", 5000),
                use_uds=config.get("use_uds", False)
            )
        elif adapter_type.lower() == AdapterType.NATS:
            return NatsClient(
                server_address=config.get("server_address", "nats://localhost:4222"),
                timeout_ms=config.get("timeout_ms", 5000),
                stream_name=config.get("stream_name", "oppie_stream"),
                consumer_name=config.get("consumer_name", "oppie_consumer")
            )
        else:
            raise ValueError(f"无效的适配器类型: {adapter_type}")
    
    @staticmethod
    def create_server(adapter_type: str, config: Dict[str, Any] = None) -> ServerAdapterInterface:
        """创建服务器适配器
        
        Args:
            adapter_type: 适配器类型，如"zeromq"、"grpc"、"nats"
            config: 适配器配置参数
            
        Returns:
            ServerAdapterInterface: 服务器适配器实例
            
        Raises:
            ValueError: 无效的适配器类型
        """
        if config is None:
            config = {}
            
        if adapter_type.lower() == AdapterType.ZEROMQ:
            return ZeroMQServer(
                bind_address=config.get("bind_address", "tcp://*:5555"),
                publisher_address=config.get("publisher_address", "tcp://*:5556")
            )
        elif adapter_type.lower() == AdapterType.GRPC:
            return GrpcServer(
                bind_address=config.get("bind_address", "[::]:50051"),
                max_workers=config.get("max_workers", 10),
                use_uds=config.get("use_uds", False)
            )
        elif adapter_type.lower() == AdapterType.NATS:
            return NatsServer(
                bind_address=config.get("bind_address", "nats://localhost:4222"),
                stream_name=config.get("stream_name", "oppie_stream"),
                subjects=config.get("subjects", None),
                retention_policy=config.get("retention_policy", "limits"),
                max_age=config.get("max_age", 86400)
            )
        else:
            raise ValueError(f"无效的适配器类型: {adapter_type}") 