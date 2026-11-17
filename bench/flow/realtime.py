"""Bounded RFC 6455 peers and the frozen duplex resume workload (stdlib only)."""

import base64
from collections import deque
import hashlib
import http.client
import json
import os
import socket
import struct
import time
import urllib.parse

TIMEOUT = 12
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
CLOSE = struct.pack("!H", 1000) + b"flow complete"
MEDIA_PACKET = bytes(range(256)) * 512
MEDIA_BASE64 = base64.b64encode(MEDIA_PACKET).decode()


def require(ok, message):
    if not ok:
        raise AssertionError(message)


def text(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()

def semantic_unit(unit):
    opcode, data = unit
    if opcode != 1:
        return unit
    value = json.loads(data)
    if isinstance(value, dict):
        field = {"response.audio.delta": "delta", "input_audio_buffer.append": "audio"}.get(value.get("type"))
        if field:
            value = {**value, field: base64.b64decode(value[field], validate=True)}
    return opcode, value



class WebSocket:
    def __init__(self, sock, reader=None, masked=True):
        self.sock = sock
        self.reader = reader or sock.makefile("rb")
        self.masked = masked
        self.sock.settimeout(TIMEOUT)

    @classmethod
    def connect(cls, port, path, headers=()):
        sock = socket.create_connection(("127.0.0.1", port), TIMEOUT)
        reader = sock.makefile("rb")
        key = base64.b64encode(os.urandom(16)).decode()
        request = [("Host", f"127.0.0.1:{port}"), ("Upgrade", "websocket"),
                   ("Connection", "Upgrade"), ("Sec-WebSocket-Version", "13"),
                   ("Sec-WebSocket-Key", key), *headers]
        try:
            sock.sendall((f"GET {path} HTTP/1.1\r\n" + "".join(
                f"{k}: {v}\r\n" for k, v in request) + "\r\n").encode())
            status = reader.readline(8192).split()
            require(len(status) >= 2 and status[1] == b"101",
                    f"WS {path} returned {b' '.join(status)!r}; realtime/attach support is required")
            response = http.client.parse_headers(reader)
            expected = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
            require(response.get("Sec-WebSocket-Accept") == expected, "invalid WS handshake accept")
            require(response.get("Upgrade", "").lower() == "websocket", "missing WS upgrade header")
            require("upgrade" in [token.strip().lower() for token in response.get("Connection", "").split(",")],
                    "missing WS Connection upgrade token")
            return cls(sock, reader), response
        except BaseException:
            reader.close()
            sock.close()
            raise

    def exact(self, count):
        data = self.reader.read(count)
        require(len(data) == count, "WS disconnected before a complete frame")
        return data

    def send(self, opcode, data):
        size = len(data)
        require(size <= 1 << 20, "fixture WS message exceeded 1 MiB")
        head = bytes([0x80 | opcode])
        mask = 0x80 if self.masked else 0
        head += (bytes([mask | size]) if size < 126 else
                 bytes([mask | 126]) + struct.pack("!H", size) if size < 65536 else
                 bytes([mask | 127]) + struct.pack("!Q", size))
        if self.masked:
            key = os.urandom(4)
            head += key
            data = bytes(v ^ key[i % 4] for i, v in enumerate(data))
        self.sock.sendall(head + data)

    def receive(self):
        chunks, message_opcode = [], None
        while True:
            a, b = self.exact(2)
            opcode, size = a & 15, b & 127
            require(not a & 0x70, "WS reserved bits set")
            require(bool(b & 0x80) != self.masked, "WS masking direction changed")
            if size == 126:
                size = struct.unpack("!H", self.exact(2))[0]
            elif size == 127:
                size = struct.unpack("!Q", self.exact(8))[0]
            require(size <= 1 << 20, "WS frame exceeded fixture bound")
            key = self.exact(4) if b & 0x80 else None
            data = self.exact(size)
            if key:
                data = bytes(v ^ key[i % 4] for i, v in enumerate(data))
            if opcode in (8, 9, 10):
                require(a & 0x80 and size <= 125, "invalid WS control frame")
                if opcode == 9:
                    self.send(10, data)
                    continue
                if opcode == 10:
                    continue
                return opcode, data
            require(opcode in (0, 1, 2), "invalid WS data opcode")
            require((opcode == 0) == (message_opcode is not None), "invalid WS continuation")
            message_opcode = message_opcode or opcode
            chunks.append(data)
            require(sum(map(len, chunks)) <= 1 << 20, "WS message exceeded fixture bound")
            if a & 0x80:
                return message_opcode, b"".join(chunks)

    def json(self, value):
        self.send(1, text(value))

    def abort(self):
        # Drop both file and socket references: makefile alone keeps a socket alive.
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.reader.close()
        self.sock.close()


def marker(direction, number):
    return {"type": "x_ck.cursor", "direction": direction, "after": f"v1.e.{number}"}


def expect_json(ws, expected, label):
    opcode, data = ws.receive()
    require(opcode == 1 and json.loads(data) == expected, f"{label} changed: {data[:240]!r}")


class DuplexReader:
    """Request acknowledgments may interleave with the retained response suffix."""
    def __init__(self, ws):
        self.ws = ws
        self.inputs, self.outputs = deque(), deque()

    def receive(self):
        opcode, data = self.ws.receive()
        value = json.loads(data) if opcode == 1 else None
        if isinstance(value, dict) and value.get("type") == "x_ck.cursor":
            require(value.get("direction") == "request", "response cursor has no preceding output unit")
            self.inputs.append(value)
            return
        unit = opcode, data
        while True:
            opcode, data = self.ws.receive()
            require(opcode == 1, "output unit has no following cursor marker")
            value = json.loads(data)
            require(isinstance(value, dict) and value.get("type") == "x_ck.cursor", "output marker missing")
            if value.get("direction") == "request":
                self.inputs.append(value)
                continue
            require(value.get("direction") == "response", "invalid cursor direction")
            self.outputs.append((unit, value))
            require(len(self.outputs) <= 82, "unexpected unbounded WS output before request acknowledgment")
            return

    def request(self, number):
        while not self.inputs:
            self.receive()
        require(self.inputs.popleft() == marker("request", number), "accepted request cursor changed")

    def output(self, number, expected):
        while not self.outputs:
            self.receive()
        unit, after = self.outputs.popleft()
        require(semantic_unit(unit) == semantic_unit(expected),
                f"WS output {number} lost, repeated, reordered or modified")
        require(after == marker("response", number), "response cursor changed")


def input_unit(ws, number, opcode, data, reader=None):
    ws.json({"type": "x_ck.input", "unit": number, "direction": "request"})
    ws.send(opcode, data)
    if reader:
        reader.request(number)
    else:
        expect_json(ws, marker("request", number), "accepted request cursor")


def output_unit(ws, number, expected, reader=None):
    if reader:
        reader.output(number, expected)
        return
    require(semantic_unit(ws.receive()) == semantic_unit(expected),
            f"WS output {number} lost, repeated, reordered or modified")
    expect_json(ws, marker("response", number), "response cursor")


def traffic(index):
    if index % 8 == 0:
        return 1, text({"type": "rate_limits.updated", "rate_limits": [
            {"name": "requests", "limit": 1000, "remaining": 1000 - index, "reset_seconds": index / 10}]})
    if index % 2:
        return 2, MEDIA_PACKET + struct.pack("!I", index)
    return 1, text({"type": "response.audio.delta", "response_id": "flow-turn",
                    "delta": MEDIA_BASE64, "index": index})


def accept(handler):
    key = handler.headers.get("Sec-WebSocket-Key", "")
    require(len(base64.b64decode(key)) == 16, "invalid mock WS handshake key")
    handler.send_response(101)
    handler.send_header("Upgrade", "websocket")
    handler.send_header("Connection", "Upgrade")
    handler.send_header("Sec-WebSocket-Accept", base64.b64encode(
        hashlib.sha1((key + GUID).encode()).digest()).decode())
    handler.end_headers()
    return WebSocket(handler.connection, handler.rfile, masked=False)


def serve_upstream(handler):
    """Provider peer: no gateway boundary controls may reach this socket."""
    up = handler.server
    up.sessions += 1
    started = time.monotonic()
    ws = accept(handler)
    try:
        for _ in range(2):
            up.received.append(ws.receive())
        ws.json({"type": "session.updated", "session": {"instructions": "flow control α"}})
        require(up.release.wait(TIMEOUT), "realtime driver did not release producer")
        for index in range(1, 81):
            ws.send(*traffic(index))
            up.sent += len(traffic(index)[1])
            # Keep a session active over multiple publication intervals.
            time.sleep(0.025)
        up.detached.set()
        for _ in range(4):
            up.received.append(ws.receive())
        ws.json({"type": "response.done", "response": {"id": "flow-turn", "status": "cancelled"}})
        up.received.append(ws.receive())
        require(up.received[-1] == (8, CLOSE), "explicit client close was not forwarded")
        ws.send(8, CLOSE)
    except (AssertionError, OSError, ValueError) as exc:
        up.error = str(exc)
    finally:
        up.session_elapsed = time.monotonic() - started
        up.done.set()


def exercise_session(connect, upstream, path, key):
    """Drive real frames; connect is replaceable only for local regression controls."""
    started = time.monotonic()
    ws, headers = connect(path, [("Authorization", f"Bearer {key}")])
    last, accepted = 0, 0
    try:
        stream_id = headers.get("X-Ck-Stream-Id")
        require(stream_id, "missing realtime X-Ck-Stream-Id")
        require(headers.get("X-Ck-Stream-Cursor") == "v1.e.0", "initial WS cursor is not zero")
        reader = DuplexReader(ws)
        reader.request(0)
        update = text({"type": "session.update", "session": {"instructions": "flow control α"}})
        live_update = text({"type": "session.update", "session": {"instructions": "flow midstream β"}})
        create = text({"type": "response.create", "response": {"modalities": ["audio", "text"]}})
        for number, data in ((1, update), (2, create)):
            input_unit(ws, number, 1, data, reader)
            accepted = number
        output_unit(ws, 1, (1, text({"type": "session.updated", "session": {"instructions": "flow control α"}})), reader)
        last = 1
        # The fixture waits for this driver signal, so no inference sleeps race the disconnect.
        upstream.release.set()
        for index in range(1, 65):
            output_unit(ws, last + 1, traffic(index), reader)
            last += 1
            if index == 32:
                input_unit(ws, 3, 1, live_update, reader)
                accepted = 3
        pending = ws.receive()
        require(semantic_unit(pending) == semantic_unit(traffic(65)), "lost-marker pending payload changed")
        # Do not consume its marker. The pending unit is deliberately discarded.
        ws.abort()
        require(upstream.detached.wait(TIMEOUT), "upstream did not publish while reader was detached")
        require(not upstream.done.is_set(), "downstream loss cancelled the producer")
        ws, attached = connect("/v1/streams?after=" + urllib.parse.quote(f"v1.e.{last}"),
                               [("Authorization", f"Bearer {key}"), ("X-Ck-Stream-Id", stream_id)])
        require(attached.get("X-Ck-Stream-Id") == stream_id, "WS reattach identity changed")
        require(attached.get("X-Ck-Stream-Cursor") == f"v1.e.{last}", "WS resume cursor changed")
        reader = DuplexReader(ws)
        reader.request(accepted)
        # Replay an acknowledged input number: no second session update or create may execute.
        input_unit(ws, accepted, 1, live_update, reader)
        for index in range(65, 81):
            output_unit(ws, last + 1, traffic(index), reader)
            last += 1
        audio = text({"type": "input_audio_buffer.append", "audio": base64.b64encode(b"\x00\xff" * 2048).decode()})
        input_unit(ws, 4, 1, audio, reader)
        input_unit(ws, 5, 2, MEDIA_PACKET, reader)
        input_unit(ws, 6, 1, text({"type": "response.cancel", "response_id": "flow-turn"}), reader)
        output_unit(ws, last + 1, (1, text({"type": "response.done", "response": {
            "id": "flow-turn", "status": "cancelled"}})), reader)
        last += 1
        require(not reader.inputs and not reader.outputs, "unexpected extra duplex cursor or output")
        ws.send(8, CLOSE)
        require(ws.receive() == (8, CLOSE), "explicit close code/reason changed")
        require(upstream.done.wait(TIMEOUT), "explicit close did not stop upstream")
        require(upstream.error is None, f"realtime upstream failed: {upstream.error}")
        expected = [(1, update), (1, create), (1, live_update), (1, audio), (2, MEDIA_PACKET),
                    (1, text({"type": "response.cancel", "response_id": "flow-turn"})), (8, CLOSE)]
        require([semantic_unit(unit) for unit in upstream.received] == [semantic_unit(unit) for unit in expected],
                "request payloads lost, repeated, reordered or boundary controls leaked upstream")
        ws.abort()
        ws, terminal = connect(f"/v1/streams?after=v1.e.{last}",
                               [("Authorization", f"Bearer {key}"), ("X-Ck-Stream-Id", stream_id)])
        require(terminal.get("X-Ck-Stream-Id") == stream_id and
                terminal.get("X-Ck-Stream-Cursor") == f"v1.e.{last}", "terminal WS attachment cursor/identity changed")
        reader = DuplexReader(ws)
        reader.request(6)
        opcode, close = ws.receive()
        require(not reader.outputs and opcode == 8 and len(close) >= 2 and close[:2] == b"\x03\xe8",
                "terminal cursor replayed output instead of normal EOF")
        require(upstream.sessions == 1, "reattach created a new upstream session")
        return {"response_units": last, "request_units": 6, "upstream_sessions": upstream.sessions,
                "media_bytes": sum(len(traffic(i)[1]) for i in range(1, 81) if i % 8),
                "session_elapsed_s": time.monotonic() - started}
    finally:
        ws.abort()
