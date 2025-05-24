"""
gRPC客户端适配器

实现基于gRPC的客户端，支持请求-响应模式和服务器流式传输的事件订阅。
作为Oppie.xyz项目的M1阶段通信层实现。
"""

import json
import uuid
import logging
import threading
import time
from typing import Dict, Any, List, Callable

import grpc

from oppie_xyz.seam_comm.adapters.adapter_interface import ClientAdapterInterface

from oppie_xyz.seam_comm.telemetry.tracer import get_current_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import record_latency, increment_counter

# 导入生成的gRPC代码
# 注意：需要先运行generate_proto.py生成这些模块
from oppie_xyz.seam_comm.proto import rpc_service_pb2
from oppie_xyz.seam_comm.proto import rpc_service_pb2_grpc

logger = logging.getLogger(__name__)

class GrpcClient(ClientAdapterInterface):
    """
    gRPC客户端适配器，实现与服务器的请求-响应和事件流订阅
    """
    
    def __init__(self, 
                 server_address: str = "localhost:50051", 
                 timeout_ms: int = 5000,
                 use_uds: bool = False):
        """初始化gRPC客户端
        
        Args:
            server_address: gRPC服务器地址（host:port或UDS路径）
            timeout_ms: 请求超时时间(毫秒)
            use_uds: 是否使用Unix Domain Socket
        """
        self.server_address = server_address
        self.timeout_ms = timeout_ms
        self.use_uds = use_uds
        self.timeout_seconds = timeout_ms / 1000.0
        
        # 创建gRPC通道和存根（将在需要时初始化）
        self.channel = None
        self.stub = None
        self.is_connected = False
        
        # 存储活动的订阅
        self.active_subscriptions = {}
        self.subscription_threads = {}
        
        logger.info(f"gRPC客户端已创建，服务器地址: {server_address}, UDS模式: {use_uds}")
    
    def _ensure_connected(self):
        """确保gRPC连接已建立"""
        if self.is_connected and self.channel and not self.channel.closed():
            return
        
        # 清理旧连接
        self._cleanup_connection()
        
        # 创建新连接
        try:
            if self.use_uds:
                # Unix Domain Socket连接
                # 注意：Windows上使用Named Pipe
                options = [
                    ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
                    ('grpc.max_send_message_length', 100 * 1024 * 1024)      # 100MB
                ]
                self.channel = grpc.insecure_channel(f"unix:{self.server_address}", options=options)
            else:
                # 标准TCP连接
                options = [
                    ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
                    ('grpc.max_send_message_length', 100 * 1024 * 1024)      # 100MB
                ]
                self.channel = grpc.insecure_channel(self.server_address, options=options)
            
            # 创建存根
            self.stub = rpc_service_pb2_grpc.OppieRpcServiceStub(self.channel)
            
            # 标记为已连接
            self.is_connected = True
            logger.info(f"已连接到gRPC服务器: {self.server_address}")
            
        except Exception as e:
            logger.error(f"连接gRPC服务器失败: {str(e)}")
            self.is_connected = False
            raise ConnectionError(f"无法连接到gRPC服务器: {str(e)}")
    
    def _cleanup_connection(self):
        """清理现有连接"""
        if self.channel:
            try:
                self.channel.close()
            except Exception as e:
                logger.warning(f"关闭gRPC通道时发生错误: {str(e)}")
            
            self.channel = None
            self.stub = None
            self.is_connected = False
    
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
        if params is None:
            params = {}
        
        # 确保连接已建立
        self._ensure_connected()
        
        # 创建请求ID
        request_id = str(uuid.uuid4())
        
        # 序列化参数
        params_json = json.dumps(params).encode('utf-8')
        
        # 获取跟踪上下文
        trace_context = get_current_trace_context()
        
        # 构建请求消息
        request = rpc_service_pb2.RpcRequest(
            method=method,
            params=params_json,
            id=request_id
        )
        
        # 添加跟踪上下文（如果有）
        if trace_context:
            request.trace_context.CopyFrom(trace_context)
        
        start_time = time.time()
        
        try:
            # 记录请求计数
            increment_counter("rpc.client.requests", 1, {"method": method})
            
            # 发送请求
            # 注意：此处使用的Call方法在rpc_service.proto中定义
            response = self.stub.Call(
                request, 
                timeout=self.timeout_seconds
            )
            
            # 计算延迟
            latency_ms = (time.time() - start_time) * 1000
            record_latency("rpc.client.latency", latency_ms, {"method": method})
            
            logger.debug(f"收到gRPC响应，延迟: {latency_ms:.2f}ms")
            
            # 检查错误
            if response.HasField('error'):
                error = response.error
                logger.error(f"RPC调用错误: {error.message}, 代码: {error.code}")
                increment_counter("rpc.client.errors", 1, {
                    "type": "rpc_error", 
                    "method": method,
                    "code": str(error.code)
                })
                
                return {
                    "jsonrpc": "2.0",
                    "id": response.id,
                    "error": {
                        "code": error.code,
                        "message": error.message,
                        "data": json.loads(error.data) if error.data else None
                    }
                }
            
            # 解析结果
            result_json = response.result
            result = json.loads(result_json) if result_json else {}
            
            # 构建标准响应
            standard_response = {
                "jsonrpc": "2.0",
                "id": response.id,
                "result": result
            }
            
            # 记录成功请求
            increment_counter("rpc.client.success", 1, {"method": method})
            
            return standard_response
            
        except grpc.RpcError as e:
            latency_ms = (time.time() - start_time) * 1000
            
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                logger.error(f"gRPC请求超时，已等待 {latency_ms:.2f}ms")
                increment_counter("rpc.client.errors", 1, {"type": "timeout", "method": method})
                raise TimeoutError(f"gRPC请求超时 ({self.timeout_ms}ms)")
            else:
                logger.error(f"gRPC错误: {e.code().name}: {e.details()}")
                increment_counter("rpc.client.errors", 1, {"type": "grpc_error", "method": method})
                raise ConnectionError(f"gRPC通信错误: {e.code().name}: {e.details()}")
                
        except Exception as e:
            logger.error(f"调用方法 {method} 时发生未知错误: {str(e)}")
            increment_counter("rpc.client.errors", 1, {"type": "unknown", "method": method})
            raise
    
    def subscribe(self, 
                  event_types: List[str], 
                  callback: Callable[[Dict[str, Any]], None],
                  timeout_ms: int = None) -> None:
        """订阅事件
        
        Args:
            event_types: 要订阅的事件类型列表
            callback: 事件处理回调函数
            timeout_ms: 订阅操作超时时间（毫秒），默认使用构造函数中的值
        """
        if not timeout_ms:
            timeout_ms = self.timeout_ms
        
        # 确保连接已建立
        self._ensure_connected()
        
        # 防止重复订阅
        subscription_key = ",".join(sorted(event_types))
        if subscription_key in self.active_subscriptions:
            logger.warning(f"已存在对事件类型的订阅: {event_types}")
            return
        
        # 获取当前跟踪上下文
        trace_context = get_current_trace_context()
        
        # 创建订阅请求
        subscription_request = rpc_service_pb2.SubscriptionRequest(
            event_types=event_types
        )
        
        # 添加跟踪上下文（如果有）
        if trace_context:
            subscription_request.trace_context.CopyFrom(trace_context)
        
        def subscription_thread_func():
            """订阅处理线程"""
            try:
                logger.info(f"开始订阅事件类型: {event_types}")
                
                # 调用gRPC流式方法
                # 注意：此处的Subscribe方法在rpc_service.proto中定义
                event_stream = self.stub.Subscribe(subscription_request)
                
                # 处理接收到的事件
                for event in event_stream:
                    try:
                        # 解析事件数据
                        event_data = json.loads(event.data)
                        
                        # 构建标准格式的事件
                        standard_event = {
                            "event": event.type,
                            "data": event_data,
                            "id": event.id
                        }
                        
                        # 增加事件计数
                        increment_counter("rpc.client.events", 1, {"event_type": event.type})
                        
                        # 调用回调函数
                        callback(standard_event)
                        
                    except Exception as e:
                        logger.error(f"处理事件时发生错误: {str(e)}")
                        increment_counter("rpc.client.event_errors", 1)
                
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.CANCELLED:
                    logger.info("事件订阅已取消")
                else:
                    logger.error(f"事件流错误: {e.code().name}: {e.details()}")
                    increment_counter("rpc.client.subscription_errors", 1, {"type": "grpc_error"})
            except Exception as e:
                logger.error(f"事件订阅线程发生错误: {str(e)}")
                increment_counter("rpc.client.subscription_errors", 1, {"type": "unknown"})
            finally:
                logger.info(f"事件订阅线程结束: {event_types}")
                
                # 从活动订阅中移除
                if subscription_key in self.active_subscriptions:
                    del self.active_subscriptions[subscription_key]
                if subscription_key in self.subscription_threads:
                    del self.subscription_threads[subscription_key]
        
        # 启动订阅线程
        subscription_thread = threading.Thread(
            target=subscription_thread_func,
            daemon=True
        )
        subscription_thread.start()
        
        # 记录活动订阅
        self.active_subscriptions[subscription_key] = event_types
        self.subscription_threads[subscription_key] = subscription_thread
        
        logger.info(f"已启动事件订阅线程，事件类型: {event_types}")
    
    def close(self) -> None:
        """关闭连接并释放资源"""
        # 取消所有订阅
        if self.channel:
            try:
                # 关闭通道，这会导致所有流终止
                self.channel.close()
            except Exception as e:
                logger.warning(f"关闭gRPC通道时发生错误: {str(e)}")
        
        # 等待所有订阅线程结束
        for key, thread in list(self.subscription_threads.items()):
            thread.join(timeout=1.0)
            if thread.is_alive():
                logger.warning(f"订阅线程未能正常终止: {key}")
        
        # 清理资源
        self.active_subscriptions.clear()
        self.subscription_threads.clear()
        self._cleanup_connection()
        
        logger.info("gRPC客户端已关闭") 