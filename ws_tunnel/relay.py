#!/usr/bin/env python3
"""
ws_tunnel/relay.py — WebSocket 中继服务（VPS 端）

支持多后端（多容器）注册，支持 PTY 二进制帧透明转发。

角色注册机制:
  - "IAM_BACKEND:<token>:<name>:<pty_flag>" → 注册为后端（新格式）
  - "IAM_BACKEND:<token>:<name>"            → 注册为后端（兼容）
  - "IAM_BACKEND:<token>"                    → 注册为后端，自动命名
  - "AUTH:<token>"                           → 注册为前端
  - 不设 token 时保持向后兼容

前端命令路由:
  - USE <name>     → 切换当前后端（之后命令无需 @ 前缀）
  - USE            → 查看当前使用的后端
  - LIST           → 列举所有已连接的后端
  - @name <cmd>    → 临时发给指定后端（不影响当前选择）
  - <cmd>          → 发送给当前后端（USE 选中的 / 第一个注册的）

数据流:
  前端发送命令/二进制 → relay → 指定后端
  后端输出文本/二进制 → relay → 所有前端（文本输出含 [@name] 标签）
"""

import asyncio
import logging
import ssl
from urllib.parse import urlparse, parse_qs

import websockets

logger = logging.getLogger(__name__)

_backend_counter = 0


def _make_handler(token: str | None):
    """创建 handler 闭包"""
    backends: dict[str, object] = {}  # name -> websocket
    backend_modes: dict[str, str] = {}  # name -> "pty" | "pipe"
    frontends: set = set()
    frontend_targets: dict = {}  # frontend ws -> current backend name (None=auto)
    _count = 0

    def _extract_url_token(websocket):
        """从 WebSocket 连接 URL 中提取 token 参数"""
        try:
            # websockets 10.x: websocket.path
            # websockets 12+: websocket.request.path
            path = '/'
            if hasattr(websocket, 'request'):
                path = websocket.request.path
            elif hasattr(websocket, 'path'):
                path = websocket.path
            parsed = urlparse(path)
            params = parse_qs(parsed.query)
            tokens = params.get('token', [])
            return tokens[0] if tokens else None
        except Exception:
            return None

    async def _register_frontend(websocket):
        """注册前端并进入消息循环"""
        frontends.add(websocket)
        frontend_targets[websocket] = None
        logger.info(f"Frontend authenticated (total {len(frontends)})")
        await _send_backend_list(websocket, backends, backend_modes, None)
        try:
            async for message in websocket:
                await _handle_frontend_message(
                    websocket, message, backends, frontends,
                    backend_modes, frontend_targets
                )
        except websockets.exceptions.ConnectionClosed:
            pass
        frontends.discard(websocket)
        frontend_targets.pop(websocket, None)
        logger.info(f"Frontend disconnected (total {len(frontends)})")

    async def handler(websocket, _path=None):
        nonlocal backends, frontends, _count

        # ── URL token 自动认证 ──
        url_token = _extract_url_token(websocket)
        if url_token and token and url_token == token:
            await websocket.send("AUTH_OK")
            await _register_frontend(websocket)
            return

        try:
            first = await asyncio.wait_for(websocket.recv(), timeout=30)

            # ── 后端注册 ──
            backend_info = _parse_backend_auth(first, token)
            if backend_info:
                name, mode = backend_info
                if name in backends:
                    await websocket.close(1008, f"Backend '{name}' already registered")
                    return
                backends[name] = websocket
                backend_modes[name] = mode
                logger.info(
                    f"Backend registered: '{name}' mode={mode} "
                    f"(total {len(backends)})"
                )
                # 通知所有前端后端列表变化
                await _broadcast_backend_list(
                    frontends, backends, backend_modes, frontend_targets
                )
                try:
                    async for message in websocket:
                        # ── 心跳（仅文本） ──
                        if isinstance(message, str) and message == "__PING__":
                            try:
                                await websocket.send("__PONG__")
                            except Exception:
                                pass
                            continue
                        # ── 二进制帧：PTY 原始输出，直接转发 ──
                        if isinstance(message, bytes):
                            await _forward_binary_to_frontends(
                                frontends, message,
                                name if len(backends) > 1 else None,
                            )
                            continue
                        # ── 文本帧：转发给所有前端，多后端时加标签 ──
                        await _forward_to_frontends(
                            frontends, message, name if len(backends) > 1 else None
                        )
                except websockets.exceptions.ConnectionClosed:
                    pass
                backends.pop(name, None)
                backend_modes.pop(name, None)
                # 如果断开的后端正好是某个前端的当前目标，清除它
                for f, target in list(frontend_targets.items()):
                    if target == name:
                        frontend_targets[f] = None
                logger.info(f"Backend disconnected: '{name}' (total {len(backends)})")
                await _broadcast_backend_list(
                    frontends, backends, backend_modes, frontend_targets
                )

            # ── 前端注册（AUTH 消息认证）──
            elif _is_frontend_auth(first, token):
                await websocket.send("AUTH_OK")
                await _register_frontend(websocket)

            else:
                await websocket.send("AUTH_FAIL")
                await websocket.close(1008, "Authentication failed")

        except asyncio.TimeoutError:
            await websocket.close(1002, "Protocol error: expected auth message")
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.exception(f"Handler error: {e}")
        finally:
            # 清理断开的连接
            for n, ws in list(backends.items()):
                if ws == websocket:
                    backends.pop(n, None)
                    backend_modes.pop(n, None)
                    break
            frontends.discard(websocket)
            frontend_targets.pop(websocket, None)

    return handler


def _parse_backend_auth(msg, token):
    """解析后端认证消息，返回 (name, mode) 元组，认证失败返回 None

    支持格式:
      IAM_BACKEND:<token>:<name>:<pty_flag>  → (name, "pty"|"pipe")
      IAM_BACKEND:<token>:<name>             → (name, "pipe")  # 兼容旧客户端
      IAM_BACKEND:<token>                    → (auto, "pipe")
      IAM_BACKEND:<name>:<pty_flag>          → (name, "pty"|"pipe")  # 无 token
      IAM_BACKEND:<pty_flag>                 → (auto, "pty"|"pipe")  # 无 token
      IAM_BACKEND                            → (auto, "pipe")        # 最旧兼容
    """
    global _backend_counter

    # 只处理文本消息
    if isinstance(msg, bytes):
        return None

    msg = msg.strip()

    if token:
        prefix = f"IAM_BACKEND:{token}"
    else:
        prefix = "IAM_BACKEND"

    if not msg.startswith(prefix):
        return None

    rest = msg[len(prefix):]

    # 收集所有冒号分隔的部分，过滤掉空字符串
    parts = [p.strip() for p in rest.split(":") if p.strip()] if rest else []

    # 尝试解析 mode 标记（最后一部分如果是 "pty" 或 "pipe"）
    mode = "pipe"
    if parts and parts[-1] in ("pty", "pipe"):
        mode = parts.pop()

    # 提取 name
    name = parts[0] if parts else None

    if not name:
        _backend_counter += 1
        name = f"backend-{_backend_counter}"

    return (name, mode)


def _is_frontend_auth(msg, token):
    """检查消息是否为合法的前端认证"""
    if isinstance(msg, bytes):
        return False
    msg = msg.strip()
    if token:
        return msg == f"AUTH:{token}"
    return True


def _resolve_target(frontend_targets, ws, backends):
    """解析前端当前的目标后端，返回 (name, ws_backend) 或 None"""
    target_name = frontend_targets.get(ws)
    if target_name and target_name in backends:
        return (target_name, backends[target_name])
    # auto: 使用第一个注册的后端
    if backends:
        return next(iter(backends.items()))
    return None


async def _forward_to_frontends(frontends, message, tag=None):
    """转发文本消息给所有前端，可选加标签"""
    if tag:
        payload = f"[@{tag}] {message}"
    else:
        payload = message
    dead = set()
    for f in frontends:
        try:
            await f.send(payload)
        except Exception:
            dead.add(f)
    frontends -= dead


async def _forward_binary_to_frontends(frontends, data, tag=None):
    """转发二进制帧给所有前端"""
    dead = set()
    for f in frontends:
        try:
            await f.send(data)
        except Exception:
            dead.add(f)
    frontends -= dead


async def _send_backend_list(ws, backends, backend_modes=None, current=None):
    """向前端发送当前后端列表"""
    if not backends:
        await ws.send("[Info] No backends connected")
    else:
        names = []
        for n in backends:
            mode = (backend_modes or {}).get(n, "pipe")
            marker = " *" if n == current else ""
            names.append(f"{n}({mode}){marker}")
        lines = [
            f"[Info] Connected backends: {', '.join(names)}",
        ]
        if current:
            lines.append(f"[Info] Current: {current}")
        await ws.send("\n".join(lines))


async def _broadcast_backend_list(
    frontends, backends, backend_modes, frontend_targets
):
    """广播后端列表给所有前端"""
    dead = set()
    for f in frontends:
        try:
            current = frontend_targets.get(f)
            await _send_backend_list(f, backends, backend_modes, current)
        except Exception:
            dead.add(f)
    frontends -= dead


async def _handle_frontend_message(
    ws, message, backends, frontends, backend_modes, frontend_targets
):
    """处理前端发出的命令（支持文本和二进制）"""

    # ── 二进制帧：原始按键输入，转发给当前后端 ──
    if isinstance(message, bytes):
        target = _resolve_target(frontend_targets, ws, backends)
        if target:
            name, ws_backend = target
            try:
                await ws_backend.send(message)
            except Exception:
                await ws.send(f"[Error] Backend '{name}' disconnected")
                backends.pop(name, None)
        else:
            await ws.send("[Error] No backends connected.")
        return

    # ── 以下是文本消息处理 ──
    msg = message.strip()

    # ── LIST: 列举后端 ──
    if msg.upper() == "LIST":
        current = frontend_targets.get(ws)
        await _send_backend_list(ws, backends, backend_modes, current)
        return

    # ── USE <name>: 切换当前后端 ──
    if msg.upper() == "USE" or msg.upper().startswith("USE "):
        parts = msg.split(None, 1)
        if len(parts) == 1:
            # USE (无参数): 显示当前目标
            current = frontend_targets.get(ws)
            if current:
                await ws.send(f"[Info] Current backend: {current}")
            else:
                # auto 模式，显示默认目标
                target = _resolve_target(frontend_targets, ws, backends)
                if target:
                    await ws.send(f"[Info] Current backend: {target[0]} (auto)")
                else:
                    await ws.send("[Info] No backends connected")
        else:
            name = parts[1].strip()
            if name in backends:
                frontend_targets[ws] = name
                mode = backend_modes.get(name, "pipe")
                await ws.send(f"[Info] Switched to backend: {name}({mode})")
                logger.info(f"Frontend switched to backend '{name}'")
            else:
                await ws.send(
                    f"[Error] Backend '{name}' not found. "
                    f"Use LIST to see available backends."
                )
        return

    # ── __RESIZE:rows,cols / __SIGNAL:SIGXXX: 控制命令，转发给当前后端 ──
    if msg.startswith("__RESIZE:") or msg.startswith("__SIGNAL:"):
        target = _resolve_target(frontend_targets, ws, backends)
        if target:
            name, ws_backend = target
            try:
                await ws_backend.send(msg)
            except Exception:
                await ws.send(f"[Error] Backend '{name}' disconnected")
                backends.pop(name, None)
        return

    # ── @name <cmd>: 临时发给指定后端（不影响当前选择）──
    if msg.startswith("@"):
        space = msg.find(" ")
        if space == -1:
            await ws.send("[Error] Usage: @backend_name <command>")
            return
        name = msg[1:space]
        cmd = msg[space + 1:]
        if name in backends:
            try:
                await backends[name].send(cmd)
            except Exception:
                await ws.send(f"[Error] Backend '{name}' disconnected")
                backends.pop(name, None)
        else:
            await ws.send(
                f"[Error] Backend '{name}' not found. "
                f"Use LIST to see available backends."
            )
        return

    # ── 普通命令: 发送给当前后端 ──
    target = _resolve_target(frontend_targets, ws, backends)
    if target:
        name, ws_backend = target
        try:
            await ws_backend.send(msg)
        except Exception:
            await ws.send(f"[Error] Backend '{name}' disconnected")
            backends.pop(name, None)
    else:
        await ws.send("[Error] No backends connected. Use LIST to check.")


def _create_ssl_context(cert_path: str, key_path: str) -> ssl.SSLContext:
    """从证书文件创建 SSL 上下文"""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_path, key_path)
    logger.info(f"TLS enabled (cert={cert_path})")
    return ctx


async def _run_async(host: str, port: int, handler, ssl_context=None):
    """异步运行中继服务"""
    async with websockets.serve(
        handler, host, port,
        ssl=ssl_context,
        ping_interval=20,
        ping_timeout=10,
    ):
        logger.info(f"Relay running on {'wss://' if ssl_context else 'ws://'}{host}:{port}")
        logger.info("Heartbeat: ping every 20s, timeout 10s")
        await asyncio.Future()


def run_relay(host: str = "0.0.0.0", port: int = 8080, token: str | None = None,
              cert_path: str | None = None, key_path: str | None = None):
    """启动 WebSocket 中继服务

    Args:
        host: 监听地址，默认 0.0.0.0
        port: 监听端口，默认 8080
        token: 可选认证令牌。None = 不开启认证（向后兼容）
    """
    if token:
        logger.info(f"Authentication enabled (token={token[:8]}...)")
    else:
        logger.warning("No token set — anyone can connect!")

    ssl_context = None
    if cert_path:
        ssl_context = _create_ssl_context(cert_path, key_path or cert_path)

    handler = _make_handler(token)
    asyncio.run(_run_async(host, port, handler, ssl_context))
