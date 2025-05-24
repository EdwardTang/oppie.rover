"""
NATS JetStream客户端适配器

实现基于NATS JetStream的客户端，支持请求-响应模式和事件订阅。
作为Oppie.xyz项目的M2阶段通信层实现。
支持消息持久化、消息重放和at-least-once语义。
"""

import json
import uuid
import logging
import threading
import time
import asyncio
from typing import Dict, Any, List, Callable

from oppie_xyz.seam_comm.adapters.adapter_interface import ClientAdapterInterface
from oppie_xyz.seam_comm.telemetry.tracer import get_current_trace_context
from oppie_xyz.seam_comm.telemetry.metrics import record_latency, increment_counter
from oppie_xyz.seam_comm.adapters.nats.utils import (
    ensure_stream, 
    ensure_durable_consumer,
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

class NatsClient(ClientAdapterInterface):
    """
    NATS JetStream客户端适配器，实现与服务器的请求-响应和事件流订阅
    """
    
    def __init__(self, 
                 server_address: str = "nats://localhost:4222", 
                 timeout_ms: int = 5000,
                 stream_name: str = "oppie.main",
                 consumer_name: str = "oppie.client",
                 max_reconnect_attempts: int = 10,
                 reconnect_time_wait: int = 1000):
        """初始化NATS JetStream客户端
        
        Args:
            server_address: NATS服务器地址
            timeout_ms: 请求超时时间(毫秒)
            stream_name: JetStream流名称
            consumer_name: JetStream消费者名称
            max_reconnect_attempts: 最大重连尝试次数
            reconnect_time_wait: 重连等待时间(毫秒)
        """
        if not NATS_AVAILABLE:
            logger.warning("NATS客户端初始化: nats-py库未安装，将使用模拟模式")
            
        self.server_address = server_address
        self.timeout_ms = timeout_ms
        self.timeout_seconds = timeout_ms / 1000.0
        self.stream_name = stream_name
        self.consumer_name = consumer_name
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_time_wait = reconnect_time_wait
        
        # 初始化NATS连接和JetStream对象
        self.nc = None  # NATS连接
        self.js = None  # JetStream上下文
        self.is_connected = False
        self._connection_lock = threading.RLock()  # 连接锁，防止并发重连
        
        # 存储活动的订阅
        self.active_subscriptions = {}
        self.subscription_objects = {}
        
        # 重试计时器
        self.backoff = BackoffTimer(
            initial_ms=100,
            max_ms=10000,
            factor=1.5,
            jitter=0.1
        )
        
        logger.info(f"NATS JetStream客户端已创建，服务器地址: {server_address}")
    
    async def _async_connect(self) -> bool:
        """异步建立NATS JetStream连接
        
        Returns:
            bool: 连接是否成功
        """
        if not NATS_AVAILABLE:
            logger.warning("_async_connect: nats-py库未安装，将使用模拟模式")
            self.is_connected = True
            return True
            
        # 清理旧连接
        await self._async_cleanup_connection()
        
        # 创建新连接
        try:
            # 连接选项
            options = {
                "servers": [self.server_address],
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
            
            # 确保流存在
            success = await ensure_stream(self.js, {"name": self.stream_name})
            if not success:
                raise ConnectionError("无法创建或验证JetStream流")
            
            # 标记为已连接
            self.is_connected = True
            logger.info(f"已连接到NATS服务器: {self.server_address}")
            
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
            
        if self.nc:
            try:
                await self.nc.close()
            except Exception as e:
                logger.warning(f"关闭NATS连接时发生错误: {str(e)}")
            
            self.nc = None
            self.js = None
            self.is_connected = False
    
    def _ensure_connected(self):
        """确保NATS JetStream连接已建立
        
        如果连接不存在或已断开，尝试重新连接
        
        Raises:
            ConnectionError: 如果连接失败
        """
        # 原子操作检查连接状态
        with self._connection_lock:
            if self.is_connected and self.nc and (not NATS_AVAILABLE or self.nc.is_connected):
                return
            
            # 尝试连接
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                connected = loop.run_until_complete(self._async_connect())
                
                if not connected:
                    raise ConnectionError(f"无法连接到NATS服务器: {self.server_address}")
                    
            finally:
                loop.close()
    
    def _cleanup_connection(self):
        """清理现有连接"""
        with self._connection_lock:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._async_cleanup_connection())
            finally:
                loop.close()
    
    # 回调函数
    def _on_reconnected(self):
        """NATS重连成功回调"""
        logger.info(f"已重新连接到NATS服务器: {self.server_address}")
        
        # 重置退避计时器
        self.backoff.reset()
        
        # 标记为已连接
        self.is_connected = True
    
    def _on_disconnected(self):
        """NATS断开连接回调"""
        logger.warning(f"与NATS服务器断开连接: {self.server_address}")
        
        # 标记为未连接
        self.is_connected = False
    
    def _on_error(self, error):
        """NATS错误回调"""
        logger.error(f"NATS错误: {error}")
    
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
        
        # 创建JSON-RPC请求
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id
        }
        
        # 获取跟踪上下文
        trace_context = get_current_trace_context()
        if trace_context:
            request["trace_context"] = trace_context
        
        # 序列化请求
        request_json = json.dumps(request)
        
        if not NATS_AVAILABLE:
            logger.warning("call: nats-py库未安装，将返回模拟响应")
            # 返回模拟响应
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"message": "NATS JetStream适配器尚未安装nats-py库，这是模拟响应"}
            }
        
        start_time = time.time()
        
        # 创建和运行异步请求
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            
            # 记录请求计数
            increment_counter("rpc.client.requests", 1, {"method": method, "adapter": "nats"})
            
            # 执行异步请求
            response = loop.run_until_complete(
                self._async_request(method, request_json, request_id)
            )
            
            # 计算延迟
            latency_ms = (time.time() - start_time) * 1000
            record_latency("rpc.client.latency", latency_ms, {"method": method, "adapter": "nats"})
            
            logger.debug(f"收到NATS响应，延迟: {latency_ms:.2f}ms")
            
            # 记录成功请求
            increment_counter("rpc.client.success", 1, {"method": method, "adapter": "nats"})
            
            return response
            
        except NatsTimeoutError:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(f"NATS请求超时，已等待 {latency_ms:.2f}ms")
            increment_counter("rpc.client.errors", 1, {"type": "timeout", "method": method, "adapter": "nats"})
            raise TimeoutError(f"NATS请求超时 ({self.timeout_ms}ms)")
        
        except (ConnectionClosedError, DisconnectedError) as e:
            logger.error(f"NATS连接错误: {str(e)}")
            increment_counter("rpc.client.errors", 1, {"type": "connection", "method": method, "adapter": "nats"})
            
            # 尝试重新连接
            try:
                self._ensure_connected()
                raise ConnectionError(f"NATS连接已断开，请重试请求: {str(e)}")
            except Exception as e2:
                raise ConnectionError(f"NATS重连失败，无法完成请求: {str(e2)}")
                
        except Exception as e:
            logger.error(f"调用方法 {method} 时发生未知错误: {str(e)}")
            increment_counter("rpc.client.errors", 1, {"type": "unknown", "method": method, "adapter": "nats"})
            raise
        finally:
            loop.close()
    
    async def _async_request(self, method: str, request_json: str, request_id: str) -> Dict[str, Any]:
        """异步执行NATS请求
        
        Args:
            method: 方法名
            request_json: 请求JSON字符串
            request_id: 请求ID
            
        Returns:
            Dict: 包含响应数据的字典
            
        Raises:
            NatsTimeoutError: 请求超时
            Exception: 其他错误
        """
        try:
            # 发布请求并等待响应
            response_msg = await self.nc.request(
                f"rpc.{method}",
                request_json.encode('utf-8'),
                timeout=self.timeout_seconds
            )
            
            # 解析响应
            response_json = response_msg.data.decode('utf-8')
            response = json.loads(response_json)
            
            # 检查错误
            if "error" in response:
                error = response["error"]
                logger.error(f"RPC调用错误: {error.get('message')}, 代码: {error.get('code')}")
                increment_counter("rpc.client.errors", 1, {
                    "type": "rpc_error", 
                    "method": method,
                    "code": str(error.get('code')),
                    "adapter": "nats"
                })
                
            return response
                
        except Exception as e:
            logger.error(f"异步请求错误: {str(e)}")
            raise
    
    async def _async_publish_with_js(self, subject: str, payload: Dict[str, Any], 
                                     msg_id: str = None, task_id: str = None,
                                     attempt: int = 0, 
                                     max_retries: int = 3) -> bool:
        """使用JetStream发布消息并等待确认
        
        Args:
            subject: 发布主题
            payload: 消息负载
            msg_id: 消息ID (用于去重)
            task_id: 任务ID
            attempt: 尝试次数
            max_retries: 最大重试次数
            
        Returns:
            bool: 发布是否成功
        """
        # 如果没有提供msg_id但提供了task_id，则生成一个确定性的msg_id
        headers = {}
        if msg_id:
            headers["Nats-Msg-Id"] = msg_id
        elif task_id:
            msg_id = await generate_deterministic_msg_id(task_id, attempt)
            headers["Nats-Msg-Id"] = msg_id
        
        # 添加尝试次数和任务ID到头信息
        if attempt > 0:
            headers["Attempt"] = str(attempt)
        if task_id:
            headers["Task-Id"] = task_id
        
        # 序列化消息
        msg_data = json.dumps(payload).encode('utf-8')
        
        for retry in range(max_retries):
            try:
                # 使用JetStream发布
                ack = await self.js.publish(
                    subject, 
                    msg_data,
                    headers=headers if headers else None,
                    timeout=self.timeout_seconds
                )
                logger.debug(f"消息已发布并确认: {subject}, 序列号: {ack.seq}")
                return True
                
            except NatsTimeoutError:
                logger.warning(f"JetStream发布超时: {subject}, 尝试: {retry+1}/{max_retries}")
                if retry >= max_retries - 1:
                    logger.error(f"JetStream发布失败: {subject}, 达到最大重试次数")
                    return False
                    
                # 等待并重试
                await self.backoff.sleep()
                
            except (ConnectionClosedError, DisconnectedError) as e:
                logger.error(f"发布时连接错误: {str(e)}")
                
                # 尝试重新连接
                try:
                    await self._async_connect()
                    logger.info("发布连接已恢复，重试中...")
                    # 给连接一点时间稳定
                    await asyncio.sleep(0.5)
                except Exception as e2:
                    logger.error(f"重新连接失败: {str(e2)}")
                    return False
                
                # 如果重连成功但这是最后一次重试，就放弃
                if retry >= max_retries - 1:
                    logger.error("达到最大重试次数，发布失败")
                    return False
                    
                # 等待并重试
                await self.backoff.sleep()
                
            except Exception as e:
                logger.error(f"发布消息时发生错误: {str(e)}")
                if retry >= max_retries - 1:
                    return False
                    
                # 等待并重试
                await self.backoff.sleep()
        
        return False
    
    def publish(self, subject: str, payload: Dict[str, Any], 
               msg_id: str = None, task_id: str = None,
               max_retries: int = 3) -> bool:
        """发布消息到JetStream流
        
        Args:
            subject: 发布主题
            payload: 消息负载
            msg_id: 消息ID (用于去重)
            task_id: 任务ID
            max_retries: 最大重试次数
            
        Returns:
            bool: 发布是否成功
        """
        # 确保连接已建立
        self._ensure_connected()
        
        if not NATS_AVAILABLE:
            logger.warning(f"publish: nats-py库未安装，将模拟发布到主题: {subject}")
            return True
        
        # 创建和运行异步发布
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(
                self._async_publish_with_js(subject, payload, msg_id, task_id, 0, max_retries)
            )
        except Exception as e:
            logger.error(f"发布消息失败: {str(e)}")
            return False
        finally:
            loop.close()
    
    def subscribe(self, 
                  event_types: List[str], 
                  callback: Callable[[Dict[str, Any]], None],
                  durable_name: str = None,
                  timeout_ms: int = None) -> None:
        """订阅事件
        
        Args:
            event_types: 要订阅的事件类型列表
            callback: 事件处理回调函数
            durable_name: 持久订阅名称（如果为None，则使用构造函数中的值）
            timeout_ms: 订阅操作超时时间（毫秒），默认使用构造函数中的值
        """
        if not timeout_ms:
            timeout_ms = self.timeout_ms
            
        if not durable_name:
            durable_name = self.consumer_name
        
        # 确保连接已建立
        self._ensure_connected()
        
        # 防止重复订阅
        subscription_key = ",".join(sorted(event_types))
        if subscription_key in self.active_subscriptions:
            logger.warning(f"已存在对事件类型的订阅: {event_types}")
            return
        
        if not NATS_AVAILABLE:
            logger.warning(f"subscribe: nats-py库未安装，将模拟订阅事件类型: {event_types}")
            self.active_subscriptions[subscription_key] = event_types
            return
        
        # 创建和运行异步订阅
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            subscriptions = loop.run_until_complete(
                self._async_subscribe(event_types, callback, durable_name)
            )
            
            # 记录活动订阅
            if subscriptions:
                self.active_subscriptions[subscription_key] = event_types
                self.subscription_objects[subscription_key] = subscriptions
                logger.info(f"已创建NATS JetStream事件订阅，事件类型: {event_types}")
            else:
                logger.error(f"无法创建NATS JetStream事件订阅: {event_types}")
                
        except Exception as e:
            logger.error(f"订阅事件失败: {str(e)}")
        finally:
            loop.close()
    
    async def _async_subscribe(self, 
                               event_types: List[str], 
                               callback: Callable[[Dict[str, Any]], None],
                               durable_name: str) -> List:
        """异步创建订阅
        
        Args:
            event_types: 要订阅的事件类型列表
            callback: 事件处理回调函数
            durable_name: 持久订阅名称
            
        Returns:
            List: 订阅对象列表
        """
        # 创建事件处理函数
        async def message_handler(msg):
            try:
                # 解析事件数据
                event_json = msg.data.decode('utf-8')
                event = json.loads(event_json)
                
                # 提取事件类型
                event_type = event.get("event", "unknown")
                
                # 增加事件计数
                increment_counter("rpc.client.events", 1, {"event_type": event_type, "adapter": "nats"})
                
                # 获取 nats-msg-id
                headers = msg.headers
                msg_id = headers.get("Nats-Msg-Id") if headers else None
                
                if msg_id:
                    logger.debug(f"收到消息，ID: {msg_id}, 类型: {event_type}")
                else:
                    logger.debug(f"收到消息，类型: {event_type}")
                
                # 确认消息收到
                try:
                    await msg.ack()
                except Exception as e:
                    logger.warning(f"确认消息失败: {str(e)}")
                
                # 调用回调函数
                callback(event)
                
            except Exception as e:
                logger.error(f"处理NATS事件时发生错误: {str(e)}")
                increment_counter("rpc.client.event_errors", 1, {"adapter": "nats"})
                
                # 尝试拒绝消息
                try:
                    await msg.nak()
                except Exception:
                    pass
        
        # 为每个事件类型创建订阅
        subscriptions = []
        for event_type in event_types:
            try:
                # 构建消费者名称
                consumer_name = f"{durable_name}_{event_type}"
                
                # 创建消费者配置
                consumer_config = {
                    "durable_name": consumer_name,
                    "deliver_policy": "all",
                    "ack_policy": "explicit",
                    "filter_subject": f"events.{event_type}"
                }
                
                # 确保消费者存在
                result = await ensure_durable_consumer(self.js, self.stream_name, consumer_config)
                if not result:
                    logger.error(f"创建消费者失败: {consumer_name}")
                    continue
                
                # 创建拉取订阅
                subscription = await self.js.pull_subscribe(
                    f"events.{event_type}",
                    durable=consumer_name
                )
                
                # 创建拉取循环的异步任务
                async def pull_messages_loop(subscription, event_type):
                    while self.is_connected:
                        try:
                            # 批量拉取消息
                            messages = await subscription.fetch(batch=10, timeout=1)
                            for msg in messages:
                                # 处理消息
                                await message_handler(msg)
                        except NatsTimeoutError:
                            # 拉取超时，正常情况
                            pass
                        except Exception as e:
                            logger.error(f"拉取消息循环错误: {str(e)}")
                            await asyncio.sleep(1)  # 避免紧密循环消耗资源
                
                # 启动拉取循环
                asyncio.create_task(pull_messages_loop(subscription, event_type))
                
                subscriptions.append(subscription)
                logger.info(f"已创建消费者: {consumer_name}")
                
            except Exception as e:
                logger.error(f"为事件类型 {event_type} 创建订阅时发生错误: {str(e)}")
        
        return subscriptions
    
    def close(self) -> None:
        """关闭连接并释放资源"""
        if not NATS_AVAILABLE:
            logger.info("close: nats-py库未安装，将模拟关闭连接")
            self.active_subscriptions.clear()
            self._cleanup_connection()
            return
            
        # 创建和运行异步关闭
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._async_close())
        except Exception as e:
            logger.error(f"关闭连接时发生错误: {str(e)}")
        finally:
            loop.close()
            
        # 清理订阅记录
        self.active_subscriptions.clear()
        self.subscription_objects.clear()
    
    async def _async_close(self):
        """异步关闭所有连接和订阅"""
        # 首先关闭所有订阅
        for key, subscriptions in self.subscription_objects.items():
            for subscription in subscriptions:
                try:
                    await subscription.unsubscribe()
                except Exception as e:
                    logger.warning(f"取消订阅时发生错误: {str(e)}")
        
        # 清理连接
        await self._async_cleanup_connection() 