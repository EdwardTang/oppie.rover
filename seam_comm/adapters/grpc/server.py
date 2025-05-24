"""
gRPC服务器适配器

实现基于gRPC的服务器，支持请求-响应模式和流式传输事件。
作为Oppie.xyz项目的M1阶段通信层实现。
"""

import json
import logging
import threading
import time
import uuid
from concurrent import futures
from typing import Dict, Any, Callable

import grpc
from google.protobuf.empty_pb2 import Empty

from oppie_xyz.seam_comm.adapters.adapter_interface import ServerAdapterInterface

from oppie_xyz.seam_comm.telemetry.tracer import with_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import record_latency, increment_counter

# 导入生成的gRPC代码
# 注意：需要先运行generate_proto.py生成这些模块
from oppie_xyz.seam_comm.proto import rpc_service_pb2
from oppie_xyz.seam_comm.proto import rpc_service_pb2_grpc

logger = logging.getLogger(__name__)

class OppieRpcServicer(rpc_service_pb2_grpc.OppieRpcServiceServicer):
    """gRPC服务实现类"""
    
    def __init__(self, server_instance):
        self.server = server_instance
        self.methods = server_instance.methods
    
    def Call(self, request, context):
        """处理RPC调用请求"""
        start_time = time.time()
        
        # 记录请求
        method_name = request.method
        request_id = request.id
        increment_counter("rpc.server.requests.received", 1, {"method": method_name})
        
        logger.debug(f"收到gRPC请求: method={method_name}, id={request_id}")
        
        # 创建响应对象
        response = rpc_service_pb2.RpcResponse(id=request_id)
        
        # 提取跟踪上下文
        trace_context = None
        if request.HasField('trace_context'):
            trace_context = request.trace_context
        
        # 查找方法处理程序
        handler = self.methods.get(method_name)
        if not handler:
            error = rpc_service_pb2.ErrorInfo(
                code=-32601,
                message=f"Method not found: {method_name}"
            )
            response.error.CopyFrom(error)
            
            increment_counter("rpc.server.errors", 1, {"type": "method_not_found", "method": method_name})
            return response
        
        try:
            # 解析参数
            params_json = request.params
            params = json.loads(params_json) if params_json else {}
            
            # 使用提取的trace_context执行处理程序
            with with_trace_context(trace_context):
                increment_counter("rpc.server.method.calls", 1, {"method": method_name})
                result = handler(params)
                
                # 序列化结果
                result_json = json.dumps(result).encode('utf-8')
                response.result = result_json
                
                # 添加trace_context（如果有）
                if trace_context:
                    response.trace_context.CopyFrom(trace_context)
        
        except Exception as e:
            logger.error(f"执行方法 {method_name} 时发生错误: {str(e)}")
            increment_counter("rpc.server.method.errors", 1, {"method": method_name})
            
            error = rpc_service_pb2.ErrorInfo(
                code=-32603,
                message=f"Internal error: {str(e)}"
            )
            response.error.CopyFrom(error)
        
        # 计算并记录请求处理延迟
        latency_ms = (time.time() - start_time) * 1000
        record_latency("rpc.server.request.latency", latency_ms, {"method": method_name})
        
        logger.debug(f"已发送gRPC响应，延迟: {latency_ms:.2f}ms")
        return response
    
    def Subscribe(self, request, context):
        """处理事件订阅请求"""
        event_types = list(request.event_types)
        
        logger.info(f"收到事件订阅请求: {event_types}")
        
        # 记录订阅
        for event_type in event_types:
            self.server.subscriptions.add(event_type)
            increment_counter("rpc.server.subscriptions", 1, {"event_type": event_type})
        
        # 提取跟踪上下文
        trace_context = None
        if request.HasField('trace_context'):
            trace_context = request.trace_context
        
        # 设置上下文取消回调
        def on_cancel():
            logger.info(f"事件订阅已取消: {event_types}")
        
        context.add_callback(on_cancel)
        
        # 添加客户端到订阅队列
        client_id = str(uuid.uuid4())
        subscription = {
            "event_types": set(event_types),
            "trace_context": trace_context,
            "context": context
        }
        self.server.add_subscriber(client_id, subscription)
        
        try:
            # 保持连接直到客户端断开或取消
            while not context.cancelled():
                time.sleep(1.0)
        finally:
            # 移除订阅
            self.server.remove_subscriber(client_id)
        
        return Empty()

class GrpcServer(ServerAdapterInterface):
    """
    gRPC服务器适配器，实现请求-响应和事件推送功能
    """
    
    def __init__(self, 
                 bind_address: str = "[::]:50051",
                 max_workers: int = 10,
                 use_uds: bool = False):
        """初始化gRPC服务器
        
        Args:
            bind_address: 绑定地址（host:port或UDS路径）
            max_workers: 最大工作线程数
            use_uds: 是否使用Unix Domain Socket
        """
        self.bind_address = bind_address
        self.max_workers = max_workers
        self.use_uds = use_uds
        
        # 存储注册的方法
        self.methods = {}
        
        # 存储事件订阅
        self.subscriptions = set()
        self.subscribers = {}
        
        # 服务器状态
        self.server = None
        self.running = False
        self.server_thread = None
        
        logger.info(f"gRPC服务器已创建，绑定地址: {bind_address}, UDS模式: {use_uds}")
    
    def register_method(self, name: str, handler: Callable):
        """注册RPC方法处理函数
        
        Args:
            name: 方法名
            handler: 处理函数，接收params参数并返回结果
        """
        self.methods[name] = handler
        logger.debug(f"注册gRPC方法: {name}")
    
    def start(self, threaded: bool = True):
        """启动服务器
        
        Args:
            threaded: 是否在单独线程中运行
        """
        if self.running:
            logger.warning("gRPC服务器已在运行")
            return
        
        self.running = True
        
        server_start_func = self._run_server
        
        if threaded:
            self.server_thread = threading.Thread(target=server_start_func)
            self.server_thread.daemon = True
            self.server_thread.start()
            logger.info("gRPC服务器在后台线程中启动")
        else:
            logger.info("gRPC服务器在主线程中启动")
            server_start_func()
    
    def _run_server(self):
        """运行gRPC服务器"""
        try:
            # 创建服务器
            self.server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=self.max_workers),
                options=[
                    ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
                    ('grpc.max_send_message_length', 100 * 1024 * 1024)      # 100MB
                ]
            )
            
            # 添加服务实现
            servicer = OppieRpcServicer(self)
            rpc_service_pb2_grpc.add_OppieRpcServiceServicer_to_server(servicer, self.server)
            
            # 绑定端口
            if self.use_uds:
                # Unix Domain Socket
                self.server.add_insecure_port(f"unix:{self.bind_address}")
            else:
                # 标准TCP
                self.server.add_insecure_port(self.bind_address)
            
            # 启动服务器
            self.server.start()
            logger.info(f"gRPC服务器启动成功，绑定地址: {self.bind_address}")
            
            # 增加指标
            increment_counter("rpc.server.started", 1)
            
            # 等待服务器终止
            while self.running:
                time.sleep(1.0)
                
        except Exception as e:
            logger.error(f"启动gRPC服务器时发生错误: {str(e)}")
            self.running = False
            raise
    
    def stop(self):
        """停止服务器"""
        if not self.running:
            logger.warning("gRPC服务器未运行")
            return
        
        self.running = False
        
        if self.server:
            # 优雅关闭：先停止接收新请求，然后等待现有请求完成
            grace_period = 5.0  # 秒
            try:
                self.server.stop(grace_period)
                logger.info(f"gRPC服务器已停止，等待期: {grace_period}秒")
            except Exception as e:
                logger.error(f"停止gRPC服务器时发生错误: {str(e)}")
                
            self.server = None
        
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=10.0)
            if self.server_thread.is_alive():
                logger.warning("gRPC服务器线程未能正常终止")
            
            self.server_thread = None
    
    def publish_event(self, event_type: str, data: Dict[str, Any], trace_context=None):
        """发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
            trace_context: OpenTelemetry trace context (可选)
        """
        # 检查是否有订阅此事件类型的客户端
        if event_type not in self.subscriptions:
            logger.debug(f"跳过发布未订阅的事件类型: {event_type}")
            return
        
        # 序列化事件数据
        data_json = json.dumps(data).encode('utf-8')
        
        # 生成事件ID
        event_id = str(uuid.uuid4())
        
        # 创建事件消息
        event = rpc_service_pb2.Event(
            type=event_type,
            data=data_json,
            id=event_id
        )
        
        # 添加跟踪上下文（如果有）
        if trace_context:
            event.trace_context.CopyFrom(trace_context)
        
        # 向订阅的客户端发送事件
        to_remove = []
        for client_id, subscription in list(self.subscribers.items()):
            if event_type in subscription["event_types"]:
                context = subscription["context"]
                
                try:
                    # 检查客户端连接是否已取消
                    if context.cancelled():
                        to_remove.append(client_id)
                        continue
                    
                    # 发送事件
                    context.write(event)
                    
                    # 增加事件发布计数
                    increment_counter("rpc.server.events.published", 1, {"event_type": event_type})
                    
                except Exception as e:
                    logger.error(f"向客户端 {client_id} 发送事件时发生错误: {str(e)}")
                    to_remove.append(client_id)
        
        # 移除断开的客户端
        for client_id in to_remove:
            self.remove_subscriber(client_id)
        
        logger.debug(f"已发布事件 {event_type} 到 {len(self.subscribers) - len(to_remove)} 个客户端")
    
    def add_subscriber(self, client_id: str, subscription: Dict[str, Any]):
        """添加事件订阅者
        
        Args:
            client_id: 客户端ID
            subscription: 订阅信息
        """
        self.subscribers[client_id] = subscription
        logger.info(f"添加事件订阅者: {client_id}, 事件类型: {subscription['event_types']}")
    
    def remove_subscriber(self, client_id: str):
        """移除事件订阅者
        
        Args:
            client_id: 客户端ID
        """
        if client_id in self.subscribers:
            subscription = self.subscribers[client_id]
            del self.subscribers[client_id]
            logger.info(f"移除事件订阅者: {client_id}, 事件类型: {subscription['event_types']}") 