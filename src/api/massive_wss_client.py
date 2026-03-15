
import os
import json
import asyncio
import websockets
import inspect
from typing import Callable, List
from src.utils.logger import app_logger
class MassiveWssClient:
    def __init__(self, on_message_callback: Callable[[list], None]):
        """
        :param on_message_callback: 外部传入的回调函数，用于把清洗好的数据写进 ClickHouse
        """
        self.api_key = os.getenv("MASSIVE_API_KEY", "")
        delay = os.getenv("MASSIVE_DELAY", "true").lower()
        self.wss_url = "wss://delayed.massive.com/stocks" if delay == "true" else "wss://socket.massive.com/stocks"
        websockets.ClientConnection
        self.on_message_callback = on_message_callback
        self.ws_connection = None
        self._is_running = False
        self.subscriptions = []  # 记录当前订阅的频道，用于重连后恢复订阅

    async def _authenticate(self):
        """🌟 鉴权握手：连接成功后的第一件事必须是发送 API Key"""
        auth_msg = {"action": "auth", "params": self.api_key}
        if self.ws_connection and self.ws_connection.protocol.state== websockets.State.OPEN:
            await self.ws_connection.send(json.dumps(auth_msg))
            app_logger.info("WSS 鉴权请求已发送...")

    async def subscribe(self, channels: List[str]):
        """
        订阅频道，例如: ["AM.*"]
        注意：实盘中如果是中途添加订阅，要确保 ws_connection 是通的
        """
        self.subscriptions.extend(channels)
        # 去重
        self.subscriptions = list(set(self.subscriptions)) 
        
        if self.ws_connection and self.ws_connection.protocol.state== websockets.State.OPEN:
            sub_msg = {"action": "subscribe", "params": ",".join(self.subscriptions)}
            await self.ws_connection.send(json.dumps(sub_msg))
            app_logger.info(f"WSS 已发送订阅请求: {self.subscriptions}")

    async def _message_handler(self):
        """核心监听循环：持续接收数据"""
        try:
            if self.ws_connection:
                async for message in self.ws_connection:
                    data = json.loads(message)
                    
                    # 很多 WSS 服务器会发回确认消息 [{ev: 'status', status: 'auth_success'}]
                    # 在这里做基础的异常拦截和状态打印
                    if isinstance(data, list) and len(data) > 0:
                        if data[0].get("ev") == "status":
                            app_logger.info(f"WSS 状态通知: {data[0].get('message')}")
                            continue
                            
                    # 🌟 解耦的精髓：自己不处理业务，直接把纯净的数据扔给外部的回调函数
                    # 这里建议使用 asyncio.create_task 扔到后台，防止回调函数里的数据库 IO 阻塞了 WSS 的接收
                    if self.on_message_callback:
                        asyncio.create_task(self._safe_callback(data))
                    
        except websockets.exceptions.ConnectionClosed as e:
            app_logger.warning(f"⚠️ WSS 连接被关闭: {e.code} - {e.reason}")
        except Exception as e:
            app_logger.error(f"❌ WSS 消息处理异常: {e}")

    async def _safe_callback(self, data):
        """保证回调函数如果写库报错，不会导致整个 WSS 客户端崩溃"""
        try:
            # 如果你的回调是同步的，可以用 asyncio.to_thread 包装
            if inspect.iscoroutinefunction(self.on_message_callback):
                await self.on_message_callback(data)
            else:
                self.on_message_callback(data)
        except Exception as e:
            app_logger.error(f"WSS 回调执行失败: {e}")

    async def run_forever(self):
        """🌟 永不宕机的核心：指数级断线重连机制"""
        self._is_running = True
        reconnect_delay = 1  # 初始重连延迟 1 秒

        while self._is_running:
            try:
                app_logger.info(f"正在连接 WSS: {self.wss_url}")
                # ping_interval 保持心跳，防止被防火墙悄悄掐断
                async with websockets.connect(self.wss_url, ping_interval=20, ping_timeout=20) as ws:
                    self.ws_connection = ws
                    reconnect_delay = 1  # 连接成功，重置重连延迟
                    
                    await self._authenticate()
                    
                    # 如果之前有订阅记录，断线重连后自动恢复订阅！
                    if self.subscriptions:
                        await self.subscribe(self.subscriptions)
                        
                    # 阻塞在这里，疯狂收数据
                    await self._message_handler()
                    
            except websockets.exceptions.InvalidURI:
                app_logger.error("WSS URI 错误，停止重连")
                break
            except Exception as e:
                app_logger.error(f"⚠️ WSS 连接异常: {e}")
            
            # 只有当连接断开（跳出 with 块），才会执行到这里
            if self._is_running:
                app_logger.info(f"⏳ {reconnect_delay} 秒后尝试重连...")
                await asyncio.sleep(reconnect_delay)
                # 指数退避，最大延迟 60 秒
                reconnect_delay = min(reconnect_delay * 2, 60)

    def stop(self):
        """优雅关闭"""
        self._is_running = False
        if self.ws_connection:
            asyncio.create_task(self.ws_connection.close())
        app_logger.info("WSS 客户端已发出停止信号")