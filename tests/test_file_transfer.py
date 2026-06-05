"""tests/test_file_transfer.py — 文件传输功能的单元测试"""

import base64
import os
import tempfile

import pytest

from wsstunnel.client import _b64, _unb64, _handle_file_cmd, _send_file
from wsstunnel.relay import _forward_to_frontends_untagged


class SyncMockWebSocket:
    """同步版 Mock WebSocket（client.py 使用同步 websocket-client 库）。"""

    def __init__(self):
        self.sent: list[str | bytes] = []
        self.closed: bool = False

    def send(self, message: str | bytes) -> None:
        self.sent.append(message)

    def close(self) -> None:
        self.closed = True

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other


# ──────────────────────────────────────────────
#  _b64 / _unb64 编解码测试
# ──────────────────────────────────────────────

class TestB64RoundTrip:
    """Base64 路径编解码的往返测试。"""

    def test_simple_path(self):
        path = "/etc/passwd"
        assert _unb64(_b64(path)) == path

    def test_path_with_spaces(self):
        path = "/home/user/my file.txt"
        assert _unb64(_b64(path)) == path

    def test_unicode_path(self):
        path = "/home/user/测试文件.txt"
        assert _unb64(_b64(path)) == path

    def test_deep_path(self):
        path = "/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z"
        assert _unb64(_b64(path)) == path

    def test_empty_string(self):
        assert _b64("") == ""
        assert _unb64("") == ""

    def test_special_chars(self):
        path = "/home/user/file with (parens) & [brackets].txt"
        assert _unb64(_b64(path)) == path


# ──────────────────────────────────────────────
#  _handle_file_cmd — 上传测试
# ──────────────────────────────────────────────

class TestHandleFileUpload:
    """文件上传命令处理测试。"""

    def test_non_file_command_returns_false(self):
        """非文件命令应返回 False。"""
        ws = SyncMockWebSocket()
        assert _handle_file_cmd("whoami", ws) is False
        assert _handle_file_cmd("", ws) is False
        assert _handle_file_cmd("__PONG__", ws) is False

    def test_upload_begin_creates_file(self):
        """__FILE_BEGIN 应创建文件并发送确认。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            result = _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:100", ws)
            assert result is True
            # 确认消息已发送（后端回复 __FILE_OK:）
            assert any(f"__FILE_OK:{b64_path}:100" in str(m) for m in ws.sent)
            # 文件应已创建（空文件）
            assert os.path.exists(path)

    def test_upload_begin_creates_subdirs(self):
        """__FILE_BEGIN 应自动创建父目录。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sub", "dir", "test.bin")
            b64_path = _b64(path)
            result = _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:50", ws)
            assert result is True
            assert os.path.exists(path)

    def test_upload_chunk_writes_data(self):
        """__FILE_CHUNK 应写入文件。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            # 开始上传
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:10", ws)
            # 发送数据块
            data = b"HelloWorld"
            b64_data = base64.b64encode(data).decode()
            result = _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:0:{b64_data}", ws)
            assert result is True
            # 验证文件内容
            with open(path, "rb") as f:
                assert f.read() == data

    def test_upload_multiple_chunks(self):
        """多个 __FILE_CHUNK 应正确拼接。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:20", ws)
            # 两块各 10 字节
            chunk1 = b"AAAAAAAAAA"
            chunk2 = b"BBBBBBBBBB"
            _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:0:{base64.b64encode(chunk1).decode()}", ws)
            _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:1:{base64.b64encode(chunk2).decode()}", ws)
            with open(path, "rb") as f:
                assert f.read() == b"AAAAAAAAAABBBBBBBBBB"

    def test_upload_end_closes_file(self):
        """__FILE_END 应关闭文件并发送完成确认。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:5", ws)
            data = b"Hello"
            _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:0:{base64.b64encode(data).decode()}", ws)
            # 清除之前的消息
            ws.sent.clear()
            result = _handle_file_cmd(f"__FILE_END:{b64_path}", ws)
            assert result is True
            # 后端回复 __FILE_DONE:（非 __FILE_END:，避免与下载混淆）
            assert any(f"__FILE_DONE:{b64_path}:5" in str(m) for m in ws.sent)

    def test_upload_end_no_state(self):
        """没有对应上传状态的 __FILE_END 应静默忽略。"""
        ws = SyncMockWebSocket()
        b64_path = _b64("/nonexistent/file")
        result = _handle_file_cmd(f"__FILE_END:{b64_path}", ws)
        assert result is True  # 仍被识别为文件命令

    def test_upload_cancel_removes_file(self):
        """__FILE_CANCEL 应删除已创建的文件。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:10", ws)
            _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:0:{base64.b64encode(b'data').decode()}", ws)
            # 取消
            result = _handle_file_cmd(f"__FILE_CANCEL:{b64_path}", ws)
            assert result is True
            assert not os.path.exists(path)

    def test_upload_cancel_no_state(self):
        """没有对应上传状态的 __FILE_CANCEL 应静默忽略。"""
        ws = SyncMockWebSocket()
        b64_path = _b64("/nonexistent/file")
        result = _handle_file_cmd(f"__FILE_CANCEL:{b64_path}", ws)
        assert result is True

    def test_upload_begin_invalid_b64(self):
        """__FILE_BEGIN 传入非法 base64 应不崩溃。"""
        ws = SyncMockWebSocket()
        result = _handle_file_cmd("__FILE_BEGIN:!!!this is not base64!!!:100", ws)
        assert result is True

    def test_upload_begin_invalid_size(self):
        """__FILE_BEGIN 传入非数字 size 应不崩溃。"""
        ws = SyncMockWebSocket()
        b64_path = _b64("/tmp/test.txt")
        result = _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:notanumber", ws)
        assert result is True

    def test_upload_multiple_files_concurrent(self):
        """同时上传多个文件应互不干扰。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.bin")
            path_b = os.path.join(tmpdir, "b.bin")
            b64_a = _b64(path_a)
            b64_b = _b64(path_b)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_a}:10", ws)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_b}:10", ws)
            _handle_file_cmd(f"__FILE_CHUNK:{b64_a}:0:{base64.b64encode(b'AAAAAAAAAA').decode()}", ws)
            _handle_file_cmd(f"__FILE_CHUNK:{b64_b}:0:{base64.b64encode(b'BBBBBBBBBB').decode()}", ws)
            with open(path_a, "rb") as f:
                assert f.read() == b"AAAAAAAAAA"
            with open(path_b, "rb") as f:
                assert f.read() == b"BBBBBBBBBB"

    def test_upload_binary_data(self):
        """二进制数据（非文本）应正确写入。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "binary.bin")
            b64_path = _b64(path)
            binary_data = bytes(range(256))  # 0x00 - 0xFF
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:256", ws)
            b64_data = base64.b64encode(binary_data).decode()
            _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:0:{b64_data}", ws)
            _handle_file_cmd(f"__FILE_END:{b64_path}", ws)
            with open(path, "rb") as f:
                assert f.read() == binary_data


# ──────────────────────────────────────────────
#  _handle_file_cmd — 下载测试
# ──────────────────────────────────────────────

class TestHandleFileDownload:
    """文件下载命令处理测试。"""

    def test_download_existing_file(self):
        """__FILE_DOWNLOAD 应读取文件并分块发送。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "download.bin")
            data = b"Hello, World! " * 1000  # ~14KB
            with open(path, "wb") as f:
                f.write(data)
            b64_path = _b64(path)
            result = _handle_file_cmd(f"__FILE_DOWNLOAD:{b64_path}", ws)
            assert result is True
            # 应收到 BEGIN、若干 CHUNK、END
            assert any(f"__FILE_BEGIN:{b64_path}:{len(data)}" in str(m) for m in ws.sent)
            assert any(m.startswith("__FILE_CHUNK:") for m in ws.sent)
            assert any(f"__FILE_END:{b64_path}:{len(data)}" in str(m) for m in ws.sent)

    def test_download_file_not_found(self):
        """不存在的文件应返回错误。"""
        ws = SyncMockWebSocket()
        b64_path = _b64("/tmp/nonexistent_file_xyz")
        result = _handle_file_cmd(f"__FILE_DOWNLOAD:{b64_path}", ws)
        assert result is True
        assert any("__FILE_ERROR:" in str(m) and "not found" in str(m) for m in ws.sent)

    def test_download_empty_file(self):
        """空文件应正确发送（无 CHUNK，只有 BEGIN/END）。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.bin")
            with open(path, "wb") as f:
                pass  # 空文件
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_DOWNLOAD:{b64_path}", ws)
            # 空文件没有 CHUNK
            chunks = [m for m in ws.sent if m.startswith("__FILE_CHUNK:")]
            assert len(chunks) == 0

    def test_download_large_file(self):
        """大文件（跨越多个分块）应正确分块。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "large.bin")
            # 大于 64KB 分块大小的数据
            data = os.urandom(200000)  # ~195KB
            with open(path, "wb") as f:
                f.write(data)
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_DOWNLOAD:{b64_path}", ws)
            # 应有 4 个 CHUNK（64K + 64K + 64K + ~8K）
            chunks = [m for m in ws.sent if m.startswith("__FILE_CHUNK:")]
            assert len(chunks) == 4  # ceil(200000/65536)
            # 验证数据和顺序
            received = bytearray()
            for chunk in chunks:
                parts = chunk.split(":", 3)
                received.extend(base64.b64decode(parts[3]))
            assert bytes(received) == data

    def test_download_invalid_b64_path(self):
        """非法 base64 路径应不崩溃。"""
        ws = SyncMockWebSocket()
        result = _handle_file_cmd("__FILE_DOWNLOAD:!!!invalid!!!", ws)
        assert result is True


# ──────────────────────────────────────────────
#  _send_file 测试
# ──────────────────────────────────────────────

class TestSendFile:
    """_send_file 直接调用测试。"""

    def test_send_file_permission_denied(self):
        """不可读文件应返回权限错误。"""
        # 跳过 Windows，只测试 Unix
        if os.name == "nt":
            pytest.skip("Permission test requires Unix")
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "noperm.bin")
            with open(path, "wb") as f:
                f.write(b"data")
            os.chmod(path, 0o000)
            try:
                _send_file(path, ws)
                assert any("Permission denied" in str(m) for m in ws.sent)
            finally:
                os.chmod(path, 0o644)


# ──────────────────────────────────────────────
#  _forward_to_frontends_untagged 测试
# ──────────────────────────────────────────────

class TestForwardUntagged:
    """relay 无标签广播函数测试。"""

    @pytest.mark.asyncio
    async def test_forward_without_tag(self):
        """转发时不应添加 [@tag] 前缀。"""
        from tests.test_relay import MockWebSocket as AsyncMockWS
        ws = AsyncMockWS()
        frontends = {ws}
        await _forward_to_frontends_untagged(frontends, "output")
        assert "output" in ws.sent
        assert "[@tag]" not in str(ws.sent[0])

    @pytest.mark.asyncio
    async def test_forward_removes_dead_connections(self):
        """应自动清理已断开的前端。"""
        from tests.test_relay import MockWebSocket as AsyncMockWS, FailingWebSocket
        ws_ok = AsyncMockWS()
        ws_dead = FailingWebSocket()
        frontends = {ws_ok, ws_dead}
        await _forward_to_frontends_untagged(frontends, "output")
        assert ws_ok in frontends
        assert ws_dead not in frontends

    @pytest.mark.asyncio
    async def test_forward_empty_frontends(self):
        """空前端集合不应报错。"""
        frontends = set()
        await _forward_to_frontends_untagged(frontends, "output")


# ──────────────────────────────────────────────
#  _handle_file_cmd — 边界和健壮性测试
# ──────────────────────────────────────────────

class TestFileCmdEdgeCases:
    """文件命令的边界情况测试。"""

    def test_malformed_begin_too_few_parts(self):
        """__FILE_BEGIN 参数不足时不崩溃。"""
        ws = SyncMockWebSocket()
        result = _handle_file_cmd("__FILE_BEGIN:", ws)
        assert result is True

    def test_malformed_chunk_too_few_parts(self):
        """__FILE_CHUNK 参数不足时不崩溃。"""
        ws = SyncMockWebSocket()
        result = _handle_file_cmd("__FILE_CHUNK:", ws)
        assert result is True

    def test_malformed_chunk_invalid_b64_data(self):
        """__FILE_CHUNK 的 data 非法时不崩溃。"""
        ws = SyncMockWebSocket()
        b64_path = _b64("/tmp/test.txt")
        result = _handle_file_cmd(f"__FILE_CHUNK:{b64_path}:0:!!!invalid!!!", ws)
        assert result is True

    def test_chunk_to_stale_upload(self):
        """结束后的上传再发 CHUNK 应忽略。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:10", ws)
            _handle_file_cmd(f"__FILE_END:{b64_path}", ws)
            ws.sent.clear()
            # 文件已结束，再发 CHUNK
            _handle_file_cmd(
                f"__FILE_CHUNK:{b64_path}:0:{base64.b64encode(b'extra').decode()}",
                ws,
            )
            # 应忽略，文件内容不变
            with open(path, "rb") as f:
                assert f.read() == b""

    def test_upload_resume_after_cancel(self):
        """取消后可重新上传同名文件。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.bin")
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:5", ws)
            _handle_file_cmd(f"__FILE_CANCEL:{b64_path}", ws)
            # 重新上传
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:5", ws)
            _handle_file_cmd(
                f"__FILE_CHUNK:{b64_path}:0:{base64.b64encode(b'Hello').decode()}",
                ws,
            )
            _handle_file_cmd(f"__FILE_END:{b64_path}", ws)
            with open(path, "rb") as f:
                assert f.read() == b"Hello"

    def test_upload_with_unicode_filename(self):
        """Unicode 文件名上传应正确。"""
        ws = SyncMockWebSocket()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "测 试/文件.txt")
            b64_path = _b64(path)
            _handle_file_cmd(f"__FILE_BEGIN:{b64_path}:6", ws)
            data = "你好世界".encode("utf-8")
            _handle_file_cmd(
                f"__FILE_CHUNK:{b64_path}:0:{base64.b64encode(data).decode()}",
                ws,
            )
            _handle_file_cmd(f"__FILE_END:{b64_path}", ws)
            with open(path, "rb") as f:
                assert f.read() == data
