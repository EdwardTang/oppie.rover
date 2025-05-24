"""NATS JetStream server adapter.

Implements a server using NATS JetStream that supports RPC handling and event
publishing. It is part of the Oppie.xyz project communication layer and
provides message persistence, replay capability and at-least-once semantics.
"""

import json
import uuid
import logging
import threading
import asyncio
import time
from typing import Dict, Any, List, Callable

from oppie_xyz.seam_comm.adapters.adapter_interface import ServerAdapterInterface
from oppie_xyz.seam_comm.telemetry.tracer import extract_trace_context, inject_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import record_latency, increment_counter
from oppie_xyz.seam_comm.adapters.nats.utils import (
    ensure_stream,
    generate_deterministic_msg_id,
    BackoffTimer,
)

# 导入nats-py库，使用条件导入避免强制依赖
try:
    import nats
    from nats.errors import ConnectionClosedError, DisconnectedError
    NATS_AVAILABLE = True
except ImportError:
    NATS_AVAILABLE = False
    # 创建虚拟的异常类以避免导入错误
    class NatsTimeoutError(Exception):
        pass

    class ConnectionClosedError(Exception):
        pass

    class DisconnectedError(Exception):
        pass

logger = logging.getLogger(__name__)

class NatsServer(ServerAdapterInterface):
    """NATS JetStream adapter supporting RPC handling and event publishing."""
    
    def __init__(self, 
                 bind_address: str = "nats://localhost:4222",
                 stream_name: str = "oppie.main",
                 subjects: List[str] = None,
                 durable_name: str = "oppie.server",
                 retention_policy: str = "limits",
                 max_age: int = 86400,  # 默认1天
                 max_reconnect_attempts: int = 10,
                 reconnect_time_wait: int = 1000):
        """Initialize a NATS JetStream server.

        Args:
            bind_address: NATS server address.
            stream_name: JetStream stream name.
            subjects: List of subject patterns for the stream.
            durable_name: Base durable subscription name.
            retention_policy: Storage policy; "limits", "interest" or "workqueue".
            max_age: Maximum message retention time in seconds.
            max_reconnect_attempts: Maximum reconnect attempts.
            reconnect_time_wait: Reconnect wait time in milliseconds.
        """
        if not NATS_AVAILABLE:
            logger.warning("NATS服务器初始化: nats-py库未安装，将使用模拟模式")
            
        self.bind_address = bind_address
        self.stream_name = stream_name
        self.durable_name = durable_name
        
        # 设置默认主题模式
        if subjects is None:
            subjects = ["oppie.>", "rpc.*", "events.*"]
        self.subjects = subjects
        
        self.retention_policy = retention_policy
        self.max_age = max_age
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_time_wait = reconnect_time_wait
        
        # 初始化NATS连接和JetStream对象
        self.nc = None  # NATS连接
        self.js = None  # JetStream上下文
        self.is_connected = False
        self._connection_lock = threading.RLock()  # 连接锁，防止并发重连
        
        # 注册的方法处理程序
        self.method_handlers = {}
        
        # 服务器控制
        self.running = False
        self.server_thread = None
        self.stop_event = threading.Event()
        
        # 活动订阅
        self.subscriptions = {}
        
        # 处理过的消息ID集合，用于去重
        self.processed_msg_ids = set()
        # 限制集合大小，避免内存泄漏
        self.max_processed_ids = 10000
        
        # 重试计时器
        self.backoff = BackoffTimer(
            initial_ms=100,
            max_ms=10000,
            factor=1.5,
            jitter=0.1
        )
        
        logger.info(f"NATS JetStream服务器已创建，服务器地址: {bind_address}")
    
    async def _async_connect(self) -> bool:
        """异步连接到NATS服务器
        
        Returns:
            bool: 连接是否成功
        """
        if not NATS_AVAILABLE:
            logger.warning("_async_connect: nats-py库未安装，将使用模拟模式")
            self.is_connected = True
            return True
        
        # 清理现有连接
        await self._async_cleanup_connection()
        
        try:
            # 连接选项
            options = {
                "servers": [self.bind_address],
                "reconnected_cb": self._on_reconnected,
                "disconnected_cb": self._on_disconnected,
                "error_cb": self._on_error,
                "max_reconnect_attempts": self.max_reconnect_attempts,
                "reconnect_time_wait": self.reconnect_time_wait
            }
            
            # 连接到NATS服务器
            self.nc = await nats.connect(**options)
            
            # 获取JetStream上下文
            self.js = self.nc.jetstream()
            
            # 创建或获取流
            stream_config = {
                "name": self.stream_name,
                "subjects": self.subjects,
                "retention": self.retention_policy,
                "max_age": self.max_age,
                "storage": "file",
                "discard": "old",
                "max_bytes": 1073741824,  # 1 GB
                "num_replicas": 1,
                "duplicate_window": 120_000_000_000,  # 2分钟（纳秒）
            }
            success = await ensure_stream(self.js, stream_config)
            if not success:
                raise ConnectionError("无法创建或验证JetStream流")
            
            # 标记为已连接
            self.is_connected = True
            logger.info(f"已连接到NATS服务器: {self.bind_address}")
            
            # 重置重试计时器
            self.backoff.reset()
            
            return True
            
        except Exception as e:
            logger.error(f"连接NATS服务器失败: {str(e)}")
            self.is_connected = False
            self.nc = None
            self.js = None
            return False
    
    async def _async_cleanup_connection(self):
        """异步清理现有连接"""
        if not NATS_AVAILABLE:
            self.is_connected = False
            return
            
        # 首先清理订阅
        for subject, sub in self.subscriptions.items():
            try:
                await sub.unsubscribe()
                logger.debug(f"已取消订阅: {subject}")
            except Exception as e:
                logger.warning(f"取消订阅 {subject} 时发生错误: {str(e)}")
        
        self.subscriptions.clear()
        
        # 然后关闭连接
        if self.nc:
            try:
                await self.nc.drain()
                await self.nc.close()
            except Exception as e:
                logger.warning(f"关闭NATS连接时发生错误: {str(e)}")
            
            self.nc = None
            self.js = None
            self.is_connected = False
    
    def _ensure_connected(self) -> bool:
        """确保连接已建立
        
        如果连接不存在或已断开，尝试重新连接
        
        Returns:
            bool: 连接是否成功
        """
        # 原子操作检查连接状态
        with self._connection_lock:
            if self.is_connected and self.nc and (not NATS_AVAILABLE or self.nc.is_connected):
                return True
            
            # 尝试连接
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                connected = loop.run_until_complete(self._async_connect())
                return connected
            except Exception as e:
                logger.error(f"连接失败: {str(e)}")
                return False
            finally:
                loop.close()
    
    # 回调函数
    def _on_reconnected(self):
        """NATS重连成功回调"""
        logger.info(f"已重新连接到NATS服务器: {self.bind_address}")
        
        # 重置退避计时器
        self.backoff.reset()
        
        # 标记为已连接
        self.is_connected = True
        
        # 重新初始化订阅
        if self.running and self.server_thread:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._reinitialize_subscriptions())
            except Exception as e:
                logger.error(f"重新初始化订阅失败: {str(e)}")
            finally:
                loop.close()
    
    def _on_disconnected(self):
        """NATS断开连接回调"""
        logger.warning(f"与NATS服务器断开连接: {self.bind_address}")
        
        # 标记为未连接
        self.is_connected = False
    
    def _on_error(self, error):
        """NATS错误回调"""
        logger.error(f"NATS错误: {error}")
    
    async def _reinitialize_subscriptions(self):
        """重新初始化所有订阅"""
        # 清理现有订阅
        self.subscriptions.clear()
        
        # 创建新的RPC订阅
        try:
            sub = await self.nc.subscribe("rpc.*", cb=self._rpc_message_handler)
            self.subscriptions["rpc.*"] = sub
            logger.info("已重新创建RPC订阅")
        except Exception as e:
            logger.error(f"重新创建RPC订阅失败: {str(e)}")
    
    def register_method(self, name: str, handler: Callable):
        """Register an RPC handler.

        Args:
            name: Method name.
            handler: Callable that accepts ``params`` and returns a result.
        """
        if name in self.method_handlers:
            logger.warning(f"方法 '{name}' 已注册，将被覆盖")
        
        self.method_handlers[name] = handler
        logger.info(f"已注册方法: {name}")
    
    async def _rpc_message_handler(self, msg):
        """Handle an incoming RPC request message."""
        if not NATS_AVAILABLE:
            return
            
        try:
            # 解析请求
            request_json = msg.data.decode('utf-8')
            request = json.loads(request_json)
            
            # 提取请求信息
            method = request.get("method", "")
            params = request.get("params", {})
            request_id = request.get("id", "unknown")
            
            # 提取跟踪上下文（如果有）
            trace_context = request.get("trace_context")
            if trace_context:
                extract_trace_context(trace_context)
            
            # 获取消息ID（如果有）
            headers = msg.headers
            msg_id = headers.get("Nats-Msg-Id") if headers else None
            
            # 检查是否为重复消息
            if msg_id and msg_id in self.processed_msg_ids:
                logger.warning(f"收到重复消息，跳过处理: {msg_id}")
                # 确认消息以避免重新传递
                await msg.ack()
                return
            
            logger.debug(f"收到RPC请求: {method}, id: {request_id}")
            increment_counter("rpc.server.requests", 1, {"method": method, "adapter": "nats"})
            
            start_time = time.time()
            
            # 检查方法是否已注册
            if method not in self.method_handlers:
                error_response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"方法 '{method}' 未找到"
                    }
                }
                
                increment_counter("rpc.server.errors", 1, {"type": "method_not_found", "method": method, "adapter": "nats"})
                
                # 发送错误响应
                await msg.respond(json.dumps(error_response).encode('utf-8'))
                # 确认消息
                await msg.ack()
                return
            
            # 调用方法处理程序
            try:
                result = self.method_handlers[method](params)
                
                # 创建成功响应
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": result
                }
                
                # 注入跟踪上下文
                trace_context = inject_trace_context()
                if trace_context:
                    response["trace_context"] = trace_context
                
                # 计算延迟
                latency_ms = (time.time() - start_time) * 1000
                record_latency("rpc.server.latency", latency_ms, {"method": method, "adapter": "nats"})
                
                # 记录成功
                increment_counter("rpc.server.success", 1, {"method": method, "adapter": "nats"})
                
                # 将消息ID添加到已处理集合
                if msg_id:
                    self._add_to_processed_ids(msg_id)
                
                # 发送成功响应
                await msg.respond(json.dumps(response).encode('utf-8'))
                # 确认消息
                await msg.ack()
                
            except Exception as e:
                logger.error(f"处理方法 '{method}' 时发生错误: {str(e)}")
                
                # 创建错误响应
                error_response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32000,
                        "message": f"内部错误: {str(e)}"
                    }
                }
                
                increment_counter("rpc.server.errors", 1, {"type": "handler_error", "method": method, "adapter": "nats"})
                
                # 发送错误响应
                await msg.respond(json.dumps(error_response).encode('utf-8'))
                
                # 消息处理失败，可以选择不确认（让消息重新传递）或确认但延迟（推迟重新传递）
                await msg.nak(delay=5000)  # 延迟5秒重新传递
                return
            
        except json.JSONDecodeError:
            logger.error("无法解析请求JSON")
            increment_counter("rpc.server.errors", 1, {"type": "json_decode", "adapter": "nats"})
            
            # 发送解析错误响应
            try:
                await msg.respond(json.dumps({
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": "无效的JSON格式"
                    }
                }).encode('utf-8'))
                # 这种情况下可以直接确认消息，因为重试也不会解决格式问题
                await msg.ack()
            except Exception as e:
                logger.error(f"回复JSON解析错误消息失败: {str(e)}")
            
        except Exception as e:
            logger.error(f"处理RPC消息时发生错误: {str(e)}")
            increment_counter("rpc.server.errors", 1, {"type": "unknown", "adapter": "nats"})
            
            # 尝试发送通用错误响应
            try:
                await msg.respond(json.dumps({
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32603,
                        "message": "内部错误"
                    }
                }).encode('utf-8'))
                # 消息处理失败，延迟重新传递
                await msg.nak(delay=5000)
            except Exception as e:
                logger.error(f"回复通用错误消息失败: {str(e)}")
    
    def _add_to_processed_ids(self, msg_id: str):
        """将消息ID添加到已处理集合，并管理集合大小
        
        Args:
            msg_id: 消息ID
        """
        self.processed_msg_ids.add(msg_id)
        
        # 如果集合超过最大大小，清理最旧的一半
        if len(self.processed_msg_ids) > self.max_processed_ids:
            # 转换为列表，取最新的一半
            id_list = list(self.processed_msg_ids)
            self.processed_msg_ids = set(id_list[len(id_list)//2:])
            logger.info(f"已清理过期消息ID，当前集合大小: {len(self.processed_msg_ids)}")
    
    async def _run_server(self):
        """异步运行服务器主循环"""
        if not NATS_AVAILABLE:
            logger.warning("_run_server: nats-py库未安装，将模拟服务器运行")
            # 模拟服务器运行
            while not self.stop_event.is_set():
                await asyncio.sleep(1)
            logger.info("NATS JetStream服务器已停止（模拟）")
            return
            
        # 连接到NATS
        if not await self._async_connect():
            logger.error("服务器启动失败: 无法连接到NATS")
            return
        
        # 订阅RPC主题
        try:
            sub = await self.nc.subscribe("rpc.*", cb=self._rpc_message_handler)
            self.subscriptions["rpc.*"] = sub
            logger.info("已创建RPC订阅")
        except Exception as e:
            logger.error(f"创建RPC订阅失败: {str(e)}")
            await self._async_cleanup_connection()
            return
        
        logger.info("NATS JetStream服务器已启动")
        
        # 保持服务器运行直到停止事件被触发
        while not self.stop_event.is_set():
            try:
                # 检查连接状态
                if not self.nc.is_connected:
                    logger.warning("NATS连接已断开，尝试重新连接")
                    if await self._async_connect():
                        await self._reinitialize_subscriptions()
                    else:
                        # 连接失败，等待后重试
                        await asyncio.sleep(2)
                        continue
                
                # 正常运行
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"服务器循环发生错误: {str(e)}")
                await asyncio.sleep(1)
        
        # 关闭连接
        await self._async_cleanup_connection()
        logger.info("NATS JetStream服务器已停止")
    
    def _server_thread_func(self):
        """服务器线程函数"""
        # 创建事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # 运行服务器
            loop.run_until_complete(self._run_server())
        except Exception as e:
            logger.error(f"服务器线程发生错误: {str(e)}")
        finally:
            # 清理事件循环
            loop.close()
    
    def start(self, threaded: bool = True):
        """启动服务器
        
        Args:
            threaded: 是否在单独线程中运行
        """
        if self.running:
            logger.warning("服务器已经在运行")
            return
        
        # 重置停止事件
        self.stop_event.clear()
        
        if threaded:
            # 在新线程中启动服务器
            self.server_thread = threading.Thread(
                target=self._server_thread_func,
                daemon=True
            )
            self.server_thread.start()
            self.running = True
            logger.info("NATS JetStream服务器在后台线程中启动")
        else:
            # 直接在当前线程运行（不推荐）
            logger.warning("在当前线程启动NATS JetStream服务器（不推荐）")
            loop = asyncio.get_event_loop()
            try:
                self.running = True
                loop.run_until_complete(self._run_server())
            except KeyboardInterrupt:
                logger.info("收到键盘中断，正在停止服务器")
            finally:
                self.running = False
    
    def stop(self):
        """停止服务器"""
        if not self.running:
            logger.warning("服务器未运行")
            return
        
        # 触发停止事件
        self.stop_event.set()
        
        # 等待服务器线程结束
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5.0)
            if self.server_thread.is_alive():
                logger.warning("服务器线程未能在超时时间内退出")
        
        self.running = False
        logger.info("NATS JetStream服务器已停止")
    
    async def _async_publish_event(self, event_type: str, data: Dict[str, Any], 
                                  task_id: str = None, msg_id: str = None,
                                  max_retries: int = 3) -> bool:
        """异步发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
            task_id: 任务ID（用于生成消息ID）
            msg_id: 消息ID（用于去重，如果提供则不使用task_id）
            max_retries: 最大重试次数
            
        Returns:
            bool: 发布是否成功
        """
        if not self.is_connected:
            await self._async_connect()
            if not self.is_connected:
                logger.warning("NATS未连接，无法发布事件")
                return False
        
        # 创建事件ID
        event_id = str(uuid.uuid4())
        
        # 创建事件消息
        event = {
            "event": event_type,
            "data": data,
            "id": event_id,
            "timestamp": time.time()
        }
        
        # 注入跟踪上下文
        # 尝试获取当前跟踪上下文
        current_trace_context = inject_trace_context()
        if current_trace_context:
            event["trace_context"] = current_trace_context
        
        # 序列化事件
        event_json = json.dumps(event)
        
        # 准备头信息
        headers = {}
        
        # 如果没有提供msg_id但提供了task_id，则生成一个确定性的msg_id
        if msg_id:
            headers["Nats-Msg-Id"] = msg_id
        elif task_id:
            msg_id = await generate_deterministic_msg_id(task_id)
            headers["Nats-Msg-Id"] = msg_id
            
        if task_id:
            headers["Task-Id"] = task_id
        
        # 序列化消息
        msg_data = event_json.encode('utf-8')
        
        for retry in range(max_retries):
            try:
                # 使用JetStream发布到事件主题
                ack = await self.js.publish(
                    f"events.{event_type}",
                    msg_data,
                    headers=headers if headers else None
                )
                
                increment_counter("events.published", 1, {"event_type": event_type, "adapter": "nats"})
                logger.debug(f"已发布事件: {event_type}, id: {event_id}, seq: {ack.seq}")
                return True
                
            except NatsTimeoutError:
                logger.warning(f"事件发布超时: {event_type}, 尝试: {retry+1}/{max_retries}")
                if retry >= max_retries - 1:
                    logger.error(
                        f"Failed to publish event: {event_type}, reached max retries"
                    )
                    break

                # Wait and retry
                await self.backoff.sleep()
                
            except (ConnectionClosedError, DisconnectedError) as e:
                logger.error(f"Connection error while publishing event: {str(e)}")
                
                # Attempt to reconnect
                try:
                    success = await self._async_connect()
                    if not success:
                        logger.error("Reconnection failed, unable to publish event")
                        if retry >= max_retries - 1:
                            break

                        # Wait and retry
                        await self.backoff.sleep()
                        continue

                    logger.info("Publish connection restored, retrying ...")
                    # Give the connection a moment to stabilize
                    await asyncio.sleep(0.5)
                except Exception as e2:
                    logger.error(f"Reconnect failed: {str(e2)}")
                    if retry >= max_retries - 1:
                        break

                    # Wait and retry
                    await self.backoff.sleep()

            except Exception as e:
                logger.error(f"Error publishing event: {str(e)}")
                if retry >= max_retries - 1:
                    break

                # Wait and retry
                await self.backoff.sleep()
        
        increment_counter("events.errors", 1, {"event_type": event_type, "adapter": "nats"})
        return False
    
    def publish_event(self, event_type: str, data: Dict[str, Any], trace_context=None, 
                     task_id: str = None, msg_id: str = None):
        """发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
            trace_context: OpenTelemetry trace context (可选)
            task_id: 任务ID（用于生成消息ID）
            msg_id: 消息ID（用于去重，如果提供则不使用task_id）
        """
        if not NATS_AVAILABLE:
            logger.warning(f"publish_event: nats-py库未安装，将模拟发布事件: {event_type}")
            return
            
        if not self.is_connected:
            if not self._ensure_connected():
                logger.warning("NATS未连接，无法发布事件")
                return
        
        # 创建和运行异步发布
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(
                self._async_publish_event(event_type, data, task_id, msg_id)
            )
            
            if not success:
                logger.error(f"发布事件失败: {event_type}")
        finally:
            loop.close() 