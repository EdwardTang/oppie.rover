"""
通信适配器接口

定义所有通信适配器（ZeroMQ, gRPC, NATS）实现的统一接口。
这确保了在底层通信机制变化时，上层应用代码无需大幅修改。
"""

import abc
from typing import Dict, Any, Callable, List

class ClientAdapterInterface(abc.ABC):
    """客户端适配器接口，定义所有客户端适配器必须实现的方法"""
    
    @abc.abstractmethod
    def call(self, method: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """发送RPC请求并等待响应
        
        Args:
            method: 要调用的方法名
            params: 方法参数
            
        Returns:
            Dict: 包含响应数据的字典
            
        Raises:
            TimeoutError: 请求超时
            ConnectionError: 连接失败
            ValueError: 响应无效
        """
        pass
    
    @abc.abstractmethod
    def subscribe(self, 
                  event_types: List[str], 
                  callback: Callable[[Dict[str, Any]], None],
                  timeout_ms: int = None) -> None:
        """订阅事件
        
        Args:
            event_types: 要订阅的事件类型列表
            callback: 事件处理回调函数
            timeout_ms: 订阅操作超时时间（毫秒）
        """
        pass
    
    @abc.abstractmethod
    def close(self) -> None:
        """关闭连接并释放资源"""
        pass

class ServerAdapterInterface(abc.ABC):
    """服务器适配器接口，定义所有服务器适配器必须实现的方法"""
    
    @abc.abstractmethod
    def register_method(self, name: str, handler: Callable):
        """注册RPC方法处理函数
        
        Args:
            name: 方法名
            handler: 处理函数，接收params参数并返回结果
        """
        pass
    
    @abc.abstractmethod
    def start(self, threaded: bool = True):
        """启动服务器
        
        Args:
            threaded: 是否在单独线程中运行
        """
        pass
    
    @abc.abstractmethod
    def stop(self):
        """停止服务器"""
        pass
    
    @abc.abstractmethod
    def publish_event(self, event_type: str, data: Dict[str, Any], trace_context=None):
        """发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
            trace_context: OpenTelemetry trace context (可选)
        """
        pass 