#!/usr/bin/env python3
"""
ws_tunnel/client.py — WebSocket 后端客户端（容器端）

同步版本，使用 websocket-client 库以支持 HTTP 代理穿透。
启动交互式 shell 子进程，通过 WebSocket 接收命令并返回输出。
"""

import ssl
import subprocess
import threading
import time
import logging
from urllib.parse import urlparse

import websocket

logger = logging.getLogger(__name__)

_RECONNECT_MAX_DELAY = 300  # 最大重连间隔 5 分钟
_HEARTBEAT_INTERVAL = 30    # 心跳间隔秒数


def _heartbeat(ws, reconnect_event):
    """心跳线程：定期发送 __PING__，失败时触发重连"""
    while not reconnect_event.is_set():
        try:
            ws.send("__PING__")
            time.sleep(_HEARTBEAT_INTERVAL)
        except Exception:
            logger.warning("Heartbeat failed, triggering reconnection")
            reconnect_event.set()
            break


def run_client(
    server_url: str,
    proxy: str | None = None,
    reconnect_interval: int = 5,
    token: str | None = None,
    insecure: bool = False,
    shell: str = "/bin/bash",
):
    """启动 WebSocket 后端客户端

    连接中继服务器，注册为后端，并启动交互式 shell 子进程。
    断开时自动重连（指数退避），内置心跳保活。

    Args:
        server_url: 中继服务器地址，如 ws://1.2.3.4:8080
        proxy: HTTP 代理地址，如 http://127.0.0.1:18080。None 表示直连。
        reconnect_interval: 初始重连间隔秒数，默认 5
        token: 认证令牌。与中继端 --token 保持一致。
        insecure: 跳过 TLS 证书验证（自签名证书）。
        shell: shell 路径，默认 /bin/bash
    """
    proxy_host = proxy_port = None
    if proxy:
        p = urlparse(proxy)
        proxy_host = p.hostname
        proxy_port = p.port

    attempt = 0
    while True:
        reconnect_event = threading.Event()
        try:
            ws = websocket.WebSocket(
                sslopt={"cert_reqs": ssl.CERT_NONE} if insecure else None,
            )
            ws.settimeout(60)
            ws.connect(
                server_url,
                http_proxy_host=proxy_host,
                http_proxy_port=proxy_port,
            )
            logger.info(f"Connected to {server_url}")

            # 发送注册消息（带 token 或向后兼容无 token）
            if token:
                ws.send(f"IAM_BACKEND:{token}")
            else:
                ws.send("IAM_BACKEND")
            logger.info("Registered as backend")

            # 连接成功，重置重连计数
            attempt = 0

            # 启动心跳线程
            hb = threading.Thread(
                target=_heartbeat, args=(ws, reconnect_event), daemon=True
            )
            hb.start()

            # 启动 shell 进程
            shell_proc = subprocess.Popen(
                [shell, "-i"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=0,
            )

            def read_and_forward():
                """读取 shell 输出，按行缓冲后通过 WebSocket 发送"""
                buf = bytearray()
                try:
                    while True:
                        byte = shell_proc.stdout.read(1)
                        if not byte:
                            break
                        buf.extend(byte)
                        # 遇到换行符或缓冲足够大时发送
                        if byte == b"\n" or len(buf) >= 4096:
                            ws.send(buf.decode("utf-8", errors="replace"))
                            buf.clear()
                    # 发送剩余内容
                    if buf:
                        ws.send(buf.decode("utf-8", errors="replace"))
                except Exception:
                    pass
                logger.info("Shell output thread exited")

            t = threading.Thread(target=read_and_forward, daemon=True)
            t.start()

            # 主线程：接收 WebSocket 命令，写入 shell
            try:
                while not reconnect_event.is_set():
                    cmd = ws.recv()
                    if not cmd:
                        break
                    # ── 心跳回包，直接忽略 ──
                    if cmd == "__PONG__":
                        continue
                    # ── 正常命令，转发给 shell ──
                    logger.debug(f"Command: {cmd.strip()[:60]}")
                    shell_proc.stdin.write((cmd + "\n").encode())
                    shell_proc.stdin.flush()
            except websocket.WebSocketConnectionClosedException:
                logger.warning("WebSocket connection closed")
            except websocket.WebSocketTimeoutException:
                # 超时正常（心跳线程在保活），不做处理
                pass
            except Exception as e:
                logger.error(f"Receive error: {e}")
            finally:
                reconnect_event.set()  # 停止心跳
                shell_proc.kill()
                ws.close()

        except Exception as e:
            attempt += 1
            delay = min(
                reconnect_interval * (2 ** (attempt - 1)),
                _RECONNECT_MAX_DELAY,
            )
            logger.error(
                f"Client error: {e}, reconnecting in {delay}s "
                f"(attempt {attempt})"
            )
            time.sleep(delay)
        else:
            break
