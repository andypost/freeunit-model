"""C Types and Memory Layout Mirrors for FreeUnit IPC and Protocol.

Provides byte-exact ctypes.Structure definitions and high-level dataclasses
with serialization/deserialization for:
- Port message headers (nxt_port_msg_t)
- Shared memory descriptors (nxt_port_mmap_msg_t)
- App queue items and circular queue (nxt_app_queue_item_t, nxt_app_queue_t)
- Request and response headers (nxt_unit_request_t, nxt_unit_response_t, nxt_unit_field_t)
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional, Tuple, Union


# --- Geometry & Protocol Constants ---

PORT_MMAP_CHUNK_SIZE = 16 * 1024       # 16 KiB
PORT_MMAP_HEADER_SIZE = 4 * 1024      # 4 KiB
PORT_MMAP_DATA_SIZE = 10 * 1024 * 1024 # 10 MiB
PORT_MMAP_SIZE = PORT_MMAP_HEADER_SIZE + PORT_MMAP_DATA_SIZE
PORT_MMAP_CHUNK_COUNT = PORT_MMAP_DATA_SIZE // PORT_MMAP_CHUNK_SIZE # 640 chunks
NXT_PORT_MMAPS_MAX = 65536

NXT_APP_NNCQ_SIZE = 131072
NXT_APP_QUEUE_SIZE = NXT_APP_NNCQ_SIZE
NXT_APP_QUEUE_MSG_SIZE = 31

NXT_UNIT_NONE_FIELD = 0xFFFFFFFF
NXT_UNIT_MAX_PLAIN_SIZE = 1024
NXT_PHP_RESPONSE_ROOM = 8192
NXT_HAVE_SIGNALFD = 1

NXT_OK = 0
NXT_ERROR = -1
NXT_EAGAIN = 11
NXT_AGAIN = NXT_EAGAIN
NXT_EINTR = 4


# --- Port Message Types & Flags ---

class PortMsgFlag(IntEnum):
    LAST = 0x100
    CLOSE_FD = 0x200
    SYNC = 0x400
    MASK = 0xFF


class PortMsgType(IntEnum):
    RPC_READY = 0
    RPC_ERROR = 1
    START_PROCESS = 2
    SOCKET = 3
    SOCKET_UNLINK = 4
    MODULES = 5
    CONF_STORE = 6
    CERT_GET = 7
    CERT_DELETE = 8
    SCRIPT_GET = 9
    SCRIPT_DELETE = 10
    ACCESS_LOG = 11
    CHANGE_FILE = 12
    NEW_PORT = 13
    GET_PORT = 14
    PORT_ACK = 15
    MMAP = 16
    GET_MMAP = 17
    PROCESS_CREATED = 18
    PROCESS_READY = 19
    WHOAMI = 20
    REMOVE_PID = 21
    QUIT = 22
    REQ_HEADERS = 23
    REQ_HEADERS_ACK = 24
    REQ_BODY = 25
    WEBSOCKET = 26
    DATA = 27
    APP_RESTART = 28
    STATUS = 29
    OOSM = 30
    SHM_ACK = 31
    READ_QUEUE = 32
    READ_SOCKET = 33
    DETACHED = 34
    REMOVE_CHILD_PID = 35
    CERT_STORE = 36
    SCRIPT_STORE = 37


class QuitMode(IntEnum):
    NORMAL = 0
    GRACEFUL = 1


class DetachedMode(IntEnum):
    START = 0
    FINISH = 1


class UnitHash(IntEnum):
    CONTENT_LENGTH = 0x1EA0
    CONTENT_TYPE = 0x5F7D
    COOKIE = 0x23F2


# --- ctypes Struct Definitions (Linux x86_64 ABI) ---

class CNxtPortMsg(ctypes.Structure):
    _fields_ = [
        ("stream", ctypes.c_uint32),
        ("pid", ctypes.c_int32),
        ("reply_port", ctypes.c_uint16),
        ("type", ctypes.c_uint8),
        ("last", ctypes.c_uint8),
        ("mmap", ctypes.c_uint8),
        ("nf", ctypes.c_uint8),
        ("mf", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 1),  # 1 byte padding to 16 bytes
    ]


class CNxtPortMmapMsg(ctypes.Structure):
    _fields_ = [
        ("mmap_id", ctypes.c_uint32),
        ("chunk_id", ctypes.c_uint32),
        ("size", ctypes.c_uint32),
    ]


class CNxtAppQueueItem(ctypes.Structure):
    _fields_ = [
        ("size", ctypes.c_uint8),
        ("data", ctypes.c_uint8 * NXT_APP_QUEUE_MSG_SIZE),
        ("tracking", ctypes.c_uint32),
    ]


class CNxtUnitSptr(ctypes.Union):
    _fields_ = [
        ("base", ctypes.c_uint8 * 1),
        ("offset", ctypes.c_uint32),
    ]


class CNxtUnitField(ctypes.Structure):
    _fields_ = [
        ("hash", ctypes.c_uint16),
        ("skip", ctypes.c_uint8, 1),
        ("hopbyhop", ctypes.c_uint8, 1),
        ("_reserved", ctypes.c_uint8, 6),
        ("name_length", ctypes.c_uint8),
        ("value_length", ctypes.c_uint32),
        ("name_offset", ctypes.c_uint32),
        ("value_offset", ctypes.c_uint32),
    ]


class CNxtUnitRequest(ctypes.Structure):
    _fields_ = [
        ("method_length", ctypes.c_uint8),
        ("version_length", ctypes.c_uint8),
        ("remote_length", ctypes.c_uint8),
        ("local_addr_length", ctypes.c_uint8),
        ("local_port_length", ctypes.c_uint8),
        ("tls", ctypes.c_uint8),
        ("websocket_handshake", ctypes.c_uint8),
        ("app_target", ctypes.c_uint8),
        ("server_name_length", ctypes.c_uint32),
        ("target_length", ctypes.c_uint32),
        ("path_length", ctypes.c_uint32),
        ("query_length", ctypes.c_uint32),
        ("fields_count", ctypes.c_uint32),
        ("content_length_field", ctypes.c_uint32),
        ("content_type_field", ctypes.c_uint32),
        ("cookie_field", ctypes.c_uint32),
        ("authorization_field", ctypes.c_uint32),
        ("content_length", ctypes.c_uint64),
        ("method_offset", ctypes.c_uint32),
        ("version_offset", ctypes.c_uint32),
        ("remote_offset", ctypes.c_uint32),
        ("local_addr_offset", ctypes.c_uint32),
        ("local_port_offset", ctypes.c_uint32),
        ("server_name_offset", ctypes.c_uint32),
        ("target_offset", ctypes.c_uint32),
        ("path_offset", ctypes.c_uint32),
        ("query_offset", ctypes.c_uint32),
        ("preread_content_offset", ctypes.c_uint32),
    ]


class CNxtUnitResponse(ctypes.Structure):
    _fields_ = [
        ("content_length", ctypes.c_uint64),
        ("fields_count", ctypes.c_uint32),
        ("piggyback_content_length", ctypes.c_uint32),
        ("status", ctypes.c_uint16),
        ("_pad", ctypes.c_uint16),
        ("piggyback_content_offset", ctypes.c_uint32),
    ]


class CSignalfdSiginfo(ctypes.Structure):
    """Linux kernel struct signalfd_siginfo layout (128 bytes)."""
    _fields_ = [
        ("ssi_signo", ctypes.c_uint32),
        ("ssi_errno", ctypes.c_int32),
        ("ssi_code", ctypes.c_int32),
        ("ssi_pid", ctypes.c_uint32),
        ("ssi_uid", ctypes.c_uint32),
        ("ssi_fd", ctypes.c_int32),
        ("ssi_tid", ctypes.c_uint32),
        ("ssi_band", ctypes.c_uint32),
        ("ssi_overrun", ctypes.c_uint32),
        ("ssi_trapno", ctypes.c_uint32),
        ("ssi_status", ctypes.c_int32),
        ("ssi_int", ctypes.c_int32),
        ("ssi_ptr", ctypes.c_uint64),
        ("ssi_utime", ctypes.c_uint64),
        ("ssi_stime", ctypes.c_uint64),
        ("ssi_addr", ctypes.c_uint64),
        ("ssi_addr_lsb", ctypes.c_uint16),
        ("_pad2", ctypes.c_uint16),
        ("ssi_syscall", ctypes.c_int32),
        ("ssi_call_addr", ctypes.c_uint64),
        ("ssi_arch", ctypes.c_uint32),
        ("_pad", ctypes.c_uint8 * 28),
    ]


# --- High-Level Python Dataclasses with Serialization ---

@dataclass
class PortMsg:
    stream: int = 0
    pid: int = 0
    reply_port: int = 0
    type: int = PortMsgType.DATA
    last: bool = False
    mmap: bool = False
    nf: bool = False
    mf: bool = False

    def to_bytes(self) -> bytes:
        c = CNxtPortMsg()
        c.stream = self.stream
        c.pid = self.pid
        c.reply_port = self.reply_port
        c.type = self.type
        c.last = 1 if self.last else 0
        c.mmap = 1 if self.mmap else 0
        c.nf = 1 if self.nf else 0
        c.mf = 1 if self.mf else 0
        return bytes(c)

    @classmethod
    def from_bytes(cls, data: bytes) -> PortMsg:
        c = CNxtPortMsg.from_buffer_copy(data)
        return cls(
            stream=c.stream,
            pid=c.pid,
            reply_port=c.reply_port,
            type=c.type,
            last=bool(c.last),
            mmap=bool(c.mmap),
            nf=bool(c.nf),
            mf=bool(c.mf),
        )


@dataclass
class MmapMsg:
    mmap_id: int
    chunk_id: int
    size: int

    def to_bytes(self) -> bytes:
        c = CNxtPortMmapMsg()
        c.mmap_id = self.mmap_id
        c.chunk_id = self.chunk_id
        c.size = self.size
        return bytes(c)

    @classmethod
    def from_bytes(cls, data: bytes) -> MmapMsg:
        c = CNxtPortMmapMsg.from_buffer_copy(data)
        return cls(mmap_id=c.mmap_id, chunk_id=c.chunk_id, size=c.size)


@dataclass
class QueueItem:
    data: bytes
    size: int = 0
    tracking: int = 0

    def __post_init__(self) -> None:
        if self.size == 0:
            self.size = len(self.data)


    def to_bytes(self) -> bytes:
        if len(self.data) > NXT_APP_QUEUE_MSG_SIZE:
            raise ValueError(f"Queue data ({len(self.data)}B) exceeds max {NXT_APP_QUEUE_MSG_SIZE}B")
        c = CNxtAppQueueItem()
        c.size = len(self.data)
        for i, b in enumerate(self.data):
            c.data[i] = b
        c.tracking = self.tracking
        return bytes(c)

    @classmethod
    def from_bytes(cls, raw: bytes) -> QueueItem:
        c = CNxtAppQueueItem.from_buffer_copy(raw)
        return cls(
            size=c.size,
            data=bytes(c.data[:c.size]),
            tracking=c.tracking,
        )


@dataclass
class HttpHeaderField:
    name: str
    value: str
    hash: int = 0
    skip: bool = False
    hopbyhop: bool = False


@dataclass
class UnitRequest:
    method: str = "GET"
    version: str = "HTTP/1.1"
    remote: str = "127.0.0.1"
    local_addr: str = "127.0.0.1"
    local_port: str = "8080"
    tls: bool = False
    websocket_handshake: bool = False
    app_target: int = 0
    server_name: str = "localhost"
    target: str = "/"
    path: str = "/"
    query: str = ""
    headers: List[HttpHeaderField] = field(default_factory=list)
    preread_content: bytes = b""
    content_length: int = 0

    def serialize(self) -> bytes:
        """Serializes request into an exact nxt_unit_request_t byte buffer."""
        # Calculate offsets
        req_hdr_size = ctypes.sizeof(CNxtUnitRequest)
        fields_size = len(self.headers) * ctypes.sizeof(CNxtUnitField)
        payload_start = req_hdr_size + fields_size

        buf = bytearray(payload_start)
        strings_dict: Dict[str, int] = {}

        def put_str(s: str) -> Tuple[int, int]:
            if not s:
                return 0, 0
            b = s.encode("utf-8")
            off = len(buf)
            buf.extend(b)
            return len(b), off

        # Method, version, remote, addrs
        mlen, moff = put_str(self.method)
        vlen, voff = put_str(self.version)
        rlen, roff = put_str(self.remote)
        lalen, laoff = put_str(self.local_addr)
        lplen, lpoff = put_str(self.local_port)
        snlen, snoff = put_str(self.server_name)
        tglen, tgoff = put_str(self.target)
        plen, poff = put_str(self.path)
        qlen, qoff = put_str(self.query)

        # Preread content
        if self.preread_content:
            pcoff = len(buf)
            buf.extend(self.preread_content)
        else:
            pcoff = 0

        # Construct C header
        creq = CNxtUnitRequest()
        creq.method_length = mlen
        creq.version_length = vlen
        creq.remote_length = rlen
        creq.local_addr_length = lalen
        creq.local_port_length = lplen
        creq.tls = 1 if self.tls else 0
        creq.websocket_handshake = 1 if self.websocket_handshake else 0
        creq.app_target = self.app_target
        creq.server_name_length = snlen
        creq.target_length = tglen
        creq.path_length = plen
        creq.query_length = qlen
        creq.fields_count = len(self.headers)
        creq.content_length = self.content_length or len(self.preread_content)

        # Offsets relative to the sptr field address
        # In FreeUnit: sptr->offset = (uint8_t *) ptr - sptr->base
        creq.method_offset = moff - CNxtUnitRequest.method_offset.offset
        creq.version_offset = voff - CNxtUnitRequest.version_offset.offset
        creq.remote_offset = roff - CNxtUnitRequest.remote_offset.offset
        creq.local_addr_offset = laoff - CNxtUnitRequest.local_addr_offset.offset
        creq.local_port_offset = lpoff - CNxtUnitRequest.local_port_offset.offset
        creq.server_name_offset = snoff - CNxtUnitRequest.server_name_offset.offset
        creq.target_offset = tgoff - CNxtUnitRequest.target_offset.offset
        creq.path_offset = poff - CNxtUnitRequest.path_offset.offset
        creq.query_offset = qoff - CNxtUnitRequest.query_offset.offset
        creq.preread_content_offset = (pcoff - CNxtUnitRequest.preread_content_offset.offset) if pcoff else 0

        buf[:req_hdr_size] = bytes(creq)

        # Append fields
        field_off = req_hdr_size
        for idx, h in enumerate(self.headers):
            cf = CNxtUnitField()
            cf.hash = h.hash
            cf.skip = 1 if h.skip else 0
            cf.hopbyhop = 1 if h.hopbyhop else 0
            nlen, noff = put_str(h.name)
            vlen, voff = put_str(h.value)
            cf.name_length = nlen
            cf.value_length = vlen
            cf.name_offset = noff - (field_off + CNxtUnitField.name_offset.offset)
            cf.value_offset = voff - (field_off + CNxtUnitField.value_offset.offset)
            buf[field_off : field_off + ctypes.sizeof(CNxtUnitField)] = bytes(cf)
            field_off += ctypes.sizeof(CNxtUnitField)

        return bytes(buf)

    @classmethod
    def deserialize(cls, data: bytes) -> UnitRequest:
        """Deserializes a byte buffer containing CNxtUnitRequest into a UnitRequest."""
        req_hdr_size = ctypes.sizeof(CNxtUnitRequest)
        if len(data) < req_hdr_size:
            return cls(method="GET", path="/")

        creq = CNxtUnitRequest.from_buffer_copy(data[:req_hdr_size])

        def get_str(length: int, sptr_offset: int, field_def_offset: int) -> str:
            if length == 0:
                return ""
            abs_off = field_def_offset + sptr_offset
            if abs_off + length > len(data):
                return ""
            return data[abs_off : abs_off + length].decode("utf-8", errors="replace")

        method = get_str(creq.method_length, creq.method_offset, CNxtUnitRequest.method_offset.offset) or "GET"
        version = get_str(creq.version_length, creq.version_offset, CNxtUnitRequest.version_offset.offset) or "HTTP/1.1"
        remote = get_str(creq.remote_length, creq.remote_offset, CNxtUnitRequest.remote_offset.offset) or "127.0.0.1"
        local_addr = get_str(creq.local_addr_length, creq.local_addr_offset, CNxtUnitRequest.local_addr_offset.offset) or "127.0.0.1"
        local_port = get_str(creq.local_port_length, creq.local_port_offset, CNxtUnitRequest.local_port_offset.offset) or "8080"
        server_name = get_str(creq.server_name_length, creq.server_name_offset, CNxtUnitRequest.server_name_offset.offset) or "localhost"
        target = get_str(creq.target_length, creq.target_offset, CNxtUnitRequest.target_offset.offset) or "/"
        path = get_str(creq.path_length, creq.path_offset, CNxtUnitRequest.path_offset.offset) or "/"
        query = get_str(creq.query_length, creq.query_offset, CNxtUnitRequest.query_offset.offset)

        preread = b""
        if creq.preread_content_offset:
            abs_pc = CNxtUnitRequest.preread_content_offset.offset + creq.preread_content_offset
            content_len = creq.content_length or 0
            if abs_pc + content_len <= len(data):
                preread = data[abs_pc : abs_pc + content_len]

        fields: List[HttpHeaderField] = []
        field_size = ctypes.sizeof(CNxtUnitField)
        offset = req_hdr_size
        for _ in range(creq.fields_count):
            if offset + field_size > len(data):
                break
            cf = CNxtUnitField.from_buffer_copy(data[offset : offset + field_size])
            name_abs = offset + CNxtUnitField.name_offset.offset + cf.name_offset
            val_abs = offset + CNxtUnitField.value_offset.offset + cf.value_offset
            name = data[name_abs : name_abs + cf.name_length].decode("utf-8", errors="replace") if name_abs + cf.name_length <= len(data) else ""
            val = data[val_abs : val_abs + cf.value_length].decode("utf-8", errors="replace") if val_abs + cf.value_length <= len(data) else ""
            fields.append(HttpHeaderField(
                name=name,
                value=val,
                hash=cf.hash,
                skip=bool(cf.skip),
                hopbyhop=bool(cf.hopbyhop),
            ))
            offset += field_size

        return cls(
            method=method,
            version=version,
            remote=remote,
            local_addr=local_addr,
            local_port=local_port,
            tls=bool(creq.tls),
            websocket_handshake=bool(creq.websocket_handshake),
            app_target=creq.app_target,
            server_name=server_name,
            target=target,
            path=path,
            query=query,
            headers=fields,
            preread_content=preread,
            content_length=creq.content_length,
        )


@dataclass
class UnitResponse:
    status: int
    headers: List[HttpHeaderField] = field(default_factory=list)
    content: bytes = b""
    content_length: Optional[int] = None

    def serialize(self) -> bytes:
        """Serializes response into an exact nxt_unit_response_t byte buffer."""
        resp_hdr_size = ctypes.sizeof(CNxtUnitResponse)
        fields_size = len(self.headers) * ctypes.sizeof(CNxtUnitField)
        payload_start = resp_hdr_size + fields_size

        buf = bytearray(payload_start)

        def put_str(s: str) -> Tuple[int, int]:
            b = s.encode("utf-8")
            off = len(buf)
            buf.extend(b)
            return len(b), off

        # Piggyback content
        if self.content:
            pcoff = len(buf)
            buf.extend(self.content)
            pclen = len(self.content)
        else:
            pcoff = 0
            pclen = 0

        cresp = CNxtUnitResponse()
        cresp.status = self.status
        cresp.fields_count = len(self.headers)
        cresp.piggyback_content_length = pclen
        cresp.content_length = self.content_length if self.content_length is not None else pclen
        cresp.piggyback_content_offset = (pcoff - CNxtUnitResponse.piggyback_content_offset.offset) if pcoff else 0

        buf[:resp_hdr_size] = bytes(cresp)

        # Append fields
        field_off = resp_hdr_size
        for idx, h in enumerate(self.headers):
            cf = CNxtUnitField()
            cf.hash = h.hash
            cf.skip = 1 if h.skip else 0
            cf.hopbyhop = 1 if h.hopbyhop else 0
            nlen, noff = put_str(h.name)
            vlen, voff = put_str(h.value)
            cf.name_length = nlen
            cf.value_length = vlen
            cf.name_offset = noff - (field_off + CNxtUnitField.name_offset.offset)
            cf.value_offset = voff - (field_off + CNxtUnitField.value_offset.offset)
            buf[field_off : field_off + ctypes.sizeof(CNxtUnitField)] = bytes(cf)
            field_off += ctypes.sizeof(CNxtUnitField)

        return bytes(buf)

    @classmethod
    def deserialize(cls, data: bytes) -> UnitResponse:
        """Deserializes a byte buffer containing CNxtUnitResponse into a UnitResponse."""
        resp_hdr_size = ctypes.sizeof(CNxtUnitResponse)
        if len(data) < resp_hdr_size:
            return cls(status=200, content=data)

        cresp = CNxtUnitResponse.from_buffer_copy(data[:resp_hdr_size])
        fields: List[HttpHeaderField] = []
        field_size = ctypes.sizeof(CNxtUnitField)
        offset = resp_hdr_size

        for _ in range(cresp.fields_count):
            if offset + field_size > len(data):
                break
            cf = CNxtUnitField.from_buffer_copy(data[offset : offset + field_size])
            name_abs = offset + CNxtUnitField.name_offset.offset + cf.name_offset
            val_abs = offset + CNxtUnitField.value_offset.offset + cf.value_offset
            name = data[name_abs : name_abs + cf.name_length].decode("utf-8", errors="replace") if name_abs + cf.name_length <= len(data) else ""
            val = data[val_abs : val_abs + cf.value_length].decode("utf-8", errors="replace") if val_abs + cf.value_length <= len(data) else ""
            fields.append(HttpHeaderField(
                name=name,
                value=val,
                hash=cf.hash,
                skip=bool(cf.skip),
                hopbyhop=bool(cf.hopbyhop),
            ))
            offset += field_size

        content = b""
        if cresp.piggyback_content_length > 0 and cresp.piggyback_content_offset:
            pc_abs = CNxtUnitResponse.piggyback_content_offset.offset + cresp.piggyback_content_offset
            if pc_abs + cresp.piggyback_content_length <= len(data):
                content = data[pc_abs : pc_abs + cresp.piggyback_content_length]
        elif offset < len(data):
            content = data[offset:]

        return cls(
            status=cresp.status,
            headers=fields,
            content=content,
            content_length=cresp.content_length,
        )


# --- Self-Validation against Linux x86_64 ABI ---

def validate_layouts() -> None:
    """Ensure ctypes struct sizes match the FreeUnit C compiler layouts."""
    assert ctypes.sizeof(CNxtPortMsg) == 16, f"CNxtPortMsg size mismatch: {ctypes.sizeof(CNxtPortMsg)} != 16"
    assert ctypes.sizeof(CNxtPortMmapMsg) == 12, f"CNxtPortMmapMsg size mismatch: {ctypes.sizeof(CNxtPortMmapMsg)} != 12"
    assert ctypes.sizeof(CNxtAppQueueItem) == 36, f"CNxtAppQueueItem size mismatch: {ctypes.sizeof(CNxtAppQueueItem)} != 36"
    assert ctypes.sizeof(CNxtUnitField) == 16, f"CNxtUnitField size mismatch: {ctypes.sizeof(CNxtUnitField)} != 16"
    assert ctypes.sizeof(CNxtUnitRequest) == 96, f"CNxtUnitRequest size mismatch: {ctypes.sizeof(CNxtUnitRequest)} != 96"
    assert ctypes.sizeof(CNxtUnitResponse) == 24, f"CNxtUnitResponse size mismatch: {ctypes.sizeof(CNxtUnitResponse)} != 24"
    assert ctypes.sizeof(CSignalfdSiginfo) == 128, f"CSignalfdSiginfo size mismatch: {ctypes.sizeof(CSignalfdSiginfo)} != 128"



validate_layouts()
