"""
ZeroMQ客户端适配器

实现基于ZeroMQ的JSON-RPC 2.0客户端，支持请求-响应模式和SSE语义。
"""

import zmq
import json
import uuid
import time
import logging
import threading
from typing import Dict, Any, List, Callable

from oppie_xyz.seam_comm.adapters.adapter_interface import ClientAdapterInterface
from oppie_xyz.seam_comm.utils.serialization import protobuf_to_dict
from oppie_xyz.seam_comm.telemetry.tracer import get_current_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import record_latency, increment_counter

logger = logging.getLogger(__name__)

class ZeroMQClient(ClientAdapterInterface):
    """
    ZeroMQ客户端适配器，实现JSON-RPC 2.0语义
    支持请求-响应模式和SSE (Server-Sent Events)模式
    """
    
    def __init__(self, 
                 server_address: str = "tcp://localhost:5555", 
                 timeout_ms: int = 5000):
        """初始化ZeroMQ客户端
        
        Args:
            server_address: ZeroMQ服务器地址
            timeout_ms: 请求超时时间(毫秒)
        """
        self.server_address = server_address
        self.timeout_ms = timeout_ms
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(server_address)
        self.socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        self.socket.setsockopt(zmq.LINGER, 0)
        logger.info(f"ZeroMQ客户端连接到 {server_address}")
    
    def __del__(self):
        """析构函数，关闭套接字和上下文"""
        self.close()
    
    def close(self):
        """关闭客户端连接"""
        if hasattr(self, 'socket') and self.socket:
            self.socket.close()
        if hasattr(self, 'context') and self.context:
            self.context.term()
    
    def call(self, method: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """发送JSON-RPC 2.0请求并等待响应
        
        Args:
            method: 要调用的方法名
            params: 方法参数
            
        Returns:
            Dict: JSON-RPC响应对象
            
        Raises:
            TimeoutError: 请求超时
            ConnectionError: 连接失败
            ValueError: 响应无效
        """
        if params is None:
            params = {}
            
        # 创建请求对象并注入跟踪上下文
        request_id = str(uuid.uuid4())
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id
        }
        
        # 注入OpenTelemetry trace context
        trace_context = get_current_trace_context()
        if trace_context:
            request["trace_context"] = protobuf_to_dict(trace_context)
        
        # 序列化并发送请求
        request_json = json.dumps(request)
        start_time = time.time()
        
        try:
            logger.debug(f"发送请求: {request_json[:200]}...")
            self.socket.send(request_json.encode('utf-8'))
            
            # 增加请求计数指标
            increment_counter("rpc.client.requests", 1, {"method": method})
            
            # 等待响应
            response_bytes = self.socket.recv()
            latency_ms = (time.time() - start_time) * 1000
            
            # 记录延迟指标
            record_latency("rpc.client.latency", latency_ms, {"method": method})
            
            logger.debug(f"收到响应，延迟: {latency_ms:.2f}ms")
            
            # 解析响应
            response = json.loads(response_bytes.decode('utf-8'))
            
            # 验证响应
            if "jsonrpc" not in response or response["jsonrpc"] != "2.0":
                logger.error(f"无效的JSON-RPC 2.0响应: {response}")
                increment_counter("rpc.client.errors", 1, {"type": "invalid_response", "method": method})
                raise ValueError("无效的JSON-RPC 2.0响应")
            
            if "id" not in response or response["id"] != request_id:
                logger.error(f"响应ID不匹配: {response.get('id')} != {request_id}")
                increment_counter("rpc.client.errors", 1, {"type": "id_mismatch", "method": method})
                raise ValueError(f"响应ID不匹配: {response.get('id')} != {request_id}")
            
            if "error" in response:
                error = response["error"]
                logger.error(f"RPC调用错误: {error.get('message')}, 代码: {error.get('code')}")
                increment_counter("rpc.client.errors", 1, {
                    "type": "rpc_error", 
                    "method": method,
                    "code": str(error.get('code', -1))
                })
                # 保留错误对象供调用者处理
            else:
                # 成功响应
                increment_counter("rpc.client.success", 1, {"method": method})
            
            return response
            
        except zmq.error.Again:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(f"请求超时，已等待 {latency_ms:.2f}ms")
            increment_counter("rpc.client.errors", 1, {"type": "timeout", "method": method})
            raise TimeoutError(f"ZeroMQ请求超时 ({self.timeout_ms}ms)")
            
        except zmq.error.ZMQError as e:
            logger.error(f"ZeroMQ错误: {str(e)}")
            increment_counter("rpc.client.errors", 1, {"type": "zmq_error", "method": method})
            raise ConnectionError(f"ZeroMQ连接错误: {str(e)}")
            
        except Exception as e:
            logger.error(f"调用方法 {method} 时发生未知错误: {str(e)}")
            increment_counter("rpc.client.errors", 1, {"type": "unknown", "method": method})
            raise
    
    def subscribe(self, 
                  event_types: List[str], 
                  callback: Callable[[Dict[str, Any]], None],
                  timeout_ms: int = None) -> None:
        """
        使用SSE语义订阅事件
        注意：此方法是个简化实现，实际使用需要考虑线程安全和异步处理
        
        Args:
            event_types: 要订阅的事件类型列表
            callback: 事件处理回调函数
            timeout_ms: 订阅操作超时时间（毫秒）
        """
        # 在实际实现中，这将使用ZeroMQ的SUB套接字模式
        # 此处简化为通过RPC设置订阅，然后在另一个线程中轮询事件
        
        # 先通过RPC注册订阅
        self.call("subscribe", {"event_types": event_types})
        
        # 创建SUB套接字进行订阅
        # 注意：这是简化实现，实际项目中应该有更健壮的实现
        def subscription_thread():
            # 创建SUB套接字
            context = zmq.Context()
            socket = context.socket(zmq.SUB)
            
            # 连接到发布者地址（通常是独立端口）
            publisher_address = self.server_address.replace("5555", "5556")  # 假设发布者在5556端口
            socket.connect(publisher_address)
            
            # 订阅感兴趣的事件类型
            for event_type in event_types:
                socket.setsockopt_string(zmq.SUBSCRIBE, event_type)
            
            logger.info(f"已订阅事件类型: {event_types}，发布者地址: {publisher_address}")
            
            try:
                while True:
                    try:
                        # 接收事件（主题和消息）
                        topic, message = socket.recv_multipart(flags=zmq.NOBLOCK)
                        
                        # 解析事件
                        event = json.loads(message.decode('utf-8'))
                        logger.debug(f"收到事件: {topic.decode('utf-8')}, {event}")
                        
                        # 增加事件计数指标
                        increment_counter("rpc.client.events", 1, {"event_type": topic.decode('utf-8')})
                        
                        # 调用回调函数
                        callback(event)
                        
                    except zmq.error.Again:
                        # 没有消息，继续循环
                        time.sleep(0.001)  # 避免CPU满载
                        continue
                        
                    except Exception as e:
                        logger.error(f"处理事件时发生错误: {str(e)}")
                        increment_counter("rpc.client.event_errors", 1)
                        time.sleep(1.0)  # 错误后稍微等待，避免迅速重试
            
            finally:
                # 清理资源
                socket.close()
                context.term()
        
        # 启动订阅线程
        thread = threading.Thread(target=subscription_thread, daemon=True)
        thread.start()
        
        logger.info(f"已启动事件订阅线程，事件类型: {event_types}") 