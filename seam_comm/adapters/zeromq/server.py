"""
ZeroMQ服务器适配器

实现基于ZeroMQ的JSON-RPC 2.0服务器，支持请求-响应模式和SSE语义。
"""

import zmq
import json
import logging
import threading
import time
from typing import Dict, Any, Callable

from oppie_xyz.seam_comm.adapters.adapter_interface import ServerAdapterInterface
from oppie_xyz.seam_comm.utils.serialization import protobuf_to_dict
from oppie_xyz.seam_comm.telemetry.tracer import extract_trace_context, with_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import record_latency, increment_counter

logger = logging.getLogger(__name__)

class ZeroMQServer(ServerAdapterInterface):
    """
    ZeroMQ服务器适配器，实现JSON-RPC 2.0语义
    支持请求-响应模式和SSE (Server-Sent Events)模式
    """
    
    def __init__(self, 
                 bind_address: str = "tcp://*:5555",
                 publisher_address: str = "tcp://*:5556"):
        """初始化ZeroMQ服务器
        
        Args:
            bind_address: 请求套接字绑定地址
            publisher_address: 发布者套接字绑定地址（用于SSE）
        """
        self.bind_address = bind_address
        self.publisher_address = publisher_address
        self.methods = {}  # 注册的RPC方法
        self.subscriptions = set()  # 记录订阅的事件类型
        self.running = False
        self.context = zmq.Context()
        
        # 创建REP套接字用于请求-响应
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(bind_address)
        
        # 创建PUB套接字用于事件发布
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(publisher_address)
        
        # 初始化指标
        increment_counter("rpc.server.started", 1)
        
        logger.info(f"ZeroMQ服务器绑定到 {bind_address}, 发布者绑定到 {publisher_address}")
    
    def __del__(self):
        """析构函数，清理资源"""
        self.stop()
        if hasattr(self, 'socket') and self.socket:
            self.socket.close()
        if hasattr(self, 'pub_socket') and self.pub_socket:
            self.pub_socket.close()
        if hasattr(self, 'context') and self.context:
            self.context.term()
    
    def register_method(self, name: str, handler: Callable):
        """注册RPC方法处理函数
        
        Args:
            name: 方法名
            handler: 处理函数，接收params参数并返回结果
        """
        self.methods[name] = handler
        logger.debug(f"注册RPC方法: {name}")
    
    def start(self, threaded: bool = True):
        """启动服务器
        
        Args:
            threaded: 是否在单独线程中运行
        """
        self.running = True
        
        # 内置订阅处理
        self.register_method("subscribe", self._handle_subscribe)
        
        if threaded:
            self.server_thread = threading.Thread(target=self._run_server)
            self.server_thread.daemon = True
            self.server_thread.start()
            logger.info("ZeroMQ服务器在后台线程中启动")
        else:
            logger.info("ZeroMQ服务器在主线程中启动")
            self._run_server()
    
    def stop(self):
        """停止服务器"""
        self.running = False
        if hasattr(self, 'server_thread') and self.server_thread:
            self.server_thread.join(timeout=1.0)
            logger.info("ZeroMQ服务器已停止")
    
    def _run_server(self):
        """服务器主循环"""
        logger.info("ZeroMQ服务器开始接收请求")
        
        while self.running:
            try:
                # 等待请求
                request_bytes = self.socket.recv(flags=zmq.NOBLOCK)
                start_time = time.time()
                
                # 增加请求计数指标
                increment_counter("rpc.server.requests.received", 1)
                
                # 解析JSON-RPC请求
                try:
                    request = json.loads(request_bytes.decode('utf-8'))
                    logger.debug(f"收到请求: {request_bytes[:200]}...")
                    
                    # 处理请求并发送响应
                    response = self._handle_request(request)
                    
                    # 仅发送非null响应（通知类请求返回null）
                    if response is not None:
                        response_json = json.dumps(response)
                        self.socket.send(response_json.encode('utf-8'))
                        
                        # 计算并记录请求处理延迟
                        latency_ms = (time.time() - start_time) * 1000
                        record_latency("rpc.server.request.latency", latency_ms, {
                            "method": request.get("method", "unknown")
                        })
                        
                        logger.debug(f"已发送响应, 耗时: {latency_ms:.2f}ms")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"JSON解析错误: {str(e)}")
                    increment_counter("rpc.server.errors", 1, {"type": "parse_error"})
                    error_response = {
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32700,
                            "message": "Parse error"
                        },
                        "id": None
                    }
                    self.socket.send(json.dumps(error_response).encode('utf-8'))
                    
                except Exception as e:
                    logger.error(f"处理请求时发生错误: {str(e)}")
                    increment_counter("rpc.server.errors", 1, {"type": "internal_error"})
                    error_response = {
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}"
                        },
                        "id": request.get("id") if isinstance(request, dict) else None
                    }
                    self.socket.send(json.dumps(error_response).encode('utf-8'))
                    
            except zmq.error.Again:
                # 没有消息，继续循环
                time.sleep(0.001)  # 避免CPU满载
                continue
                
            except Exception as e:
                logger.error(f"服务器循环中发生错误: {str(e)}")
                increment_counter("rpc.server.errors", 1, {"type": "loop_error"})
                time.sleep(1.0)  # 错误后稍微等待，避免迅速重试
    
    def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理JSON-RPC请求
        
        Args:
            request: JSON-RPC请求对象
            
        Returns:
            Dict: JSON-RPC响应对象
        """
        # 验证JSON-RPC 2.0请求
        if "jsonrpc" not in request or request["jsonrpc"] != "2.0":
            increment_counter("rpc.server.errors", 1, {"type": "invalid_request"})
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32600,
                    "message": "Invalid Request: Not a valid JSON-RPC 2.0 request"
                },
                "id": request.get("id")
            }
        
        # 提取trace_context并设置当前跟踪上下文
        trace_context = None
        if "trace_context" in request:
            trace_context_dict = request.get("trace_context")
            trace_context = extract_trace_context(trace_context_dict)
        
        # 准备响应ID
        response_id = request.get("id")
        
        # 如果没有ID，则为通知，不需要响应
        if response_id is None:
            method_name = request.get("method", "unknown")
            increment_counter("rpc.server.notifications", 1, {"method": method_name})
            logger.debug(f"收到通知请求 (ID=None): {method_name}")
            return None
        
        # 验证方法
        method_name = request.get("method")
        if not method_name:
            increment_counter("rpc.server.errors", 1, {"type": "method_missing"})
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32600,
                    "message": "Invalid Request: Method not specified"
                },
                "id": response_id
            }
        
        # 查找方法处理程序
        handler = self.methods.get(method_name)
        if not handler:
            increment_counter("rpc.server.errors", 1, {"type": "method_not_found", "method": method_name})
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method_name}"
                },
                "id": response_id
            }
        
        # 获取参数
        params = request.get("params", {})
        
        # 使用提取的trace_context执行处理程序
        try:
            with with_trace_context(trace_context):
                increment_counter("rpc.server.method.calls", 1, {"method": method_name})
                result = handler(params)
                
            return {
                "jsonrpc": "2.0",
                "result": result,
                "id": response_id
            }
            
        except Exception as e:
            logger.error(f"执行方法 {method_name} 时发生错误: {str(e)}")
            increment_counter("rpc.server.method.errors", 1, {"method": method_name})
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                },
                "id": response_id
            }
    
    def _handle_subscribe(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """处理订阅请求（内置方法）
        
        Args:
            params: 包含event_types的参数字典
            
        Returns:
            Dict: 订阅结果
        """
        event_types = params.get("event_types", [])
        if not isinstance(event_types, list):
            raise ValueError("event_types must be a list")
        
        # 记录订阅的事件类型
        for event_type in event_types:
            self.subscriptions.add(event_type)
            increment_counter("rpc.server.subscriptions", 1, {"event_type": event_type})
        
        logger.info(f"客户端订阅了事件类型: {event_types}")
        
        return {
            "status": "success",
            "subscribed_events": list(self.subscriptions)
        }
    
    def publish_event(self, event_type: str, data: Dict[str, Any], trace_context=None):
        """发布事件（SSE语义）
        
        Args:
            event_type: 事件类型
            data: 事件数据
            trace_context: OpenTelemetry trace context (可选)
        """
        # 如果没有客户端订阅此事件类型，可以选择跳过发布
        # 注意：在实际系统中，此检查可能不是必要的，因为ZeroMQ PUB/SUB是单向的
        if event_type not in self.subscriptions and len(self.subscriptions) > 0:
            logger.debug(f"跳过发布未订阅的事件类型: {event_type}")
            return
        
        event = {
            "event": event_type,
            "data": data,
            "id": str(time.time()),  # 事件ID（SSE语义）
        }
        
        # 注入trace context
        if trace_context:
            event["trace_context"] = protobuf_to_dict(trace_context)
        
        # 发布事件（事件类型作为ZeroMQ主题）
        message = json.dumps(event)
        self.pub_socket.send_multipart([
            event_type.encode('utf-8'),  # 主题
            message.encode('utf-8')      # 消息
        ])
        
        # 增加事件发布计数
        increment_counter("rpc.server.events.published", 1, {"event_type": event_type})
        
        logger.debug(f"已发布事件 {event_type}: {message[:100]}...") 