"""
DuckDB Server Python Client

A simple Python client for connecting to DuckDB Server.
"""

import socket
import struct
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import IntEnum
import threading


# Protocol constants
PROTOCOL_MAGIC = 0x4B435544  # "DUCK"
PROTOCOL_VERSION = 0x01
DEFAULT_PORT = 9999
HEADER_SIZE = 12


class MessageType(IntEnum):
    """Protocol message types"""
    HELLO = 0x01
    HELLO_RESPONSE = 0x02
    PING = 0x03
    PONG = 0x04
    CLOSE = 0x05
    
    QUERY = 0x10
    QUERY_RESULT = 0x11
    QUERY_ERROR = 0x12
    QUERY_PROGRESS = 0x13
    QUERY_CANCEL = 0x14
    
    PREPARE = 0x20
    PREPARE_RESPONSE = 0x21
    EXECUTE = 0x22
    DEALLOCATE = 0x23
    
    RESULT_HEADER = 0x40
    RESULT_BATCH = 0x41
    RESULT_END = 0x42


class ClientType(IntEnum):
    """Client type identifiers"""
    UNKNOWN = 0x00
    CPP = 0x01
    PYTHON = 0x02
    JAVA = 0x03
    GO = 0x04


@dataclass
class MessageHeader:
    """Protocol message header"""
    magic: int
    version: int
    msg_type: int
    flags: int
    reserved: int
    length: int
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'MessageHeader':
        magic, version, msg_type, flags, reserved, length = struct.unpack('<IBBBBI', data)
        return cls(magic, version, msg_type, flags, reserved, length)
    
    def to_bytes(self) -> bytes:
        return struct.pack('<IBBBBI', 
                          self.magic, self.version, self.msg_type,
                          self.flags, self.reserved, self.length)
    
    def is_valid(self) -> bool:
        return self.magic == PROTOCOL_MAGIC and self.version == PROTOCOL_VERSION


@dataclass
class Message:
    """Protocol message"""
    header: MessageHeader
    payload: bytes
    
    @classmethod
    def create(cls, msg_type: MessageType, payload: bytes = b'', flags: int = 0) -> 'Message':
        header = MessageHeader(
            magic=PROTOCOL_MAGIC,
            version=PROTOCOL_VERSION,
            msg_type=msg_type.value,
            flags=flags,
            reserved=0,
            length=len(payload)
        )
        return cls(header, payload)
    
    def to_bytes(self) -> bytes:
        return self.header.to_bytes() + self.payload


class DuckDBError(Exception):
    """DuckDB Server error"""
    def __init__(self, code: int, sql_state: str, message: str):
        self.code = code
        self.sql_state = sql_state
        super().__init__(message)


@dataclass
class QueryResult:
    """Query result container"""
    columns: List[str]
    types: List[str]
    rows: List[Tuple]
    row_count: int
    execution_time_us: int


class DuckDBClient:
    """
    DuckDB Server Client
    
    Example usage:
        client = DuckDBClient('localhost', 9999)
        client.connect()
        result = client.query("SELECT * FROM test")
        client.close()
    """
    
    def __init__(self, host: str = 'localhost', port: int = DEFAULT_PORT,
                 username: str = '', timeout: float = 30.0):
        self.host = host
        self.port = port
        self.username = username
        self.timeout = timeout
        
        self._socket: Optional[socket.socket] = None
        self._session_id: int = 0
        self._query_id: int = 0
        self._lock = threading.Lock()
        self._connected = False
    
    def connect(self) -> None:
        """Connect to the server"""
        if self._connected:
            return
        
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket.connect((self.host, self.port))
        
        # Send HELLO
        hello_payload = self._build_hello_payload()
        self._send_message(MessageType.HELLO, hello_payload)
        
        # Receive HELLO_RESPONSE
        msg = self._recv_message()
        
        if msg.header.msg_type == MessageType.QUERY_ERROR.value:
            error = self._parse_error(msg.payload)
            raise DuckDBError(error[0], error[1], error[2])
        
        if msg.header.msg_type != MessageType.HELLO_RESPONSE.value:
            raise DuckDBError(0, "00000", "Unexpected response to HELLO")
        
        # Parse response
        response = self._parse_hello_response(msg.payload)
        
        if response['status'] != 0:
            raise DuckDBError(response['status'], "00000", "Connection rejected")
        
        self._session_id = response['session_id']
        self._connected = True
        
        print(f"Connected to DuckDB Server (session_id={self._session_id})")
    
    def close(self) -> None:
        """Close the connection"""
        if not self._connected:
            return
        
        try:
            self._send_message(MessageType.CLOSE, b'')
        except:
            pass
        
        if self._socket:
            self._socket.close()
            self._socket = None
        
        self._connected = False
        self._session_id = 0
    
    def query(self, sql: str, timeout_ms: int = 0, max_rows: int = 0) -> QueryResult:
        """Execute a query and return results"""
        if not self._connected:
            raise DuckDBError(0, "00000", "Not connected")
        
        with self._lock:
            self._query_id += 1
            query_id = self._query_id
        
        # Build query payload
        payload = self._build_query_payload(query_id, sql, timeout_ms, max_rows)
        self._send_message(MessageType.QUERY, payload)
        
        # Receive results
        columns = []
        types = []
        rows = []
        row_count = 0
        execution_time_us = 0
        
        while True:
            msg = self._recv_message()
            
            if msg.header.msg_type == MessageType.QUERY_ERROR.value:
                error = self._parse_error(msg.payload)
                raise DuckDBError(error[0], error[1], error[2])
            
            elif msg.header.msg_type == MessageType.RESULT_HEADER.value:
                columns, types = self._parse_schema(msg.payload)
            
            elif msg.header.msg_type == MessageType.RESULT_BATCH.value:
                batch_rows = self._parse_batch(msg.payload, types)
                rows.extend(batch_rows)
            
            elif msg.header.msg_type == MessageType.RESULT_END.value:
                end_info = self._parse_result_end(msg.payload)
                row_count = end_info['total_rows']
                execution_time_us = end_info['execution_time_us']
                break
        
        return QueryResult(columns, types, rows, row_count, execution_time_us)
    
    def execute(self, sql: str) -> int:
        """Execute a statement and return affected row count"""
        result = self.query(sql)
        return result.row_count
    
    def ping(self) -> bool:
        """Send ping and wait for pong"""
        if not self._connected:
            return False
        
        try:
            self._send_message(MessageType.PING, b'')
            msg = self._recv_message()
            return msg.header.msg_type == MessageType.PONG.value
        except:
            return False
    
    def _send_message(self, msg_type: MessageType, payload: bytes) -> None:
        """Send a message to the server"""
        msg = Message.create(msg_type, payload)
        self._socket.sendall(msg.to_bytes())
    
    def _recv_message(self) -> Message:
        """Receive a message from the server"""
        # Read header
        header_data = self._recv_exact(HEADER_SIZE)
        header = MessageHeader.from_bytes(header_data)
        
        if not header.is_valid():
            raise DuckDBError(0, "00000", "Invalid message header")
        
        # Read payload
        payload = b''
        if header.length > 0:
            payload = self._recv_exact(header.length)
        
        return Message(header, payload)
    
    def _recv_exact(self, size: int) -> bytes:
        """Receive exact number of bytes"""
        data = b''
        while len(data) < size:
            chunk = self._socket.recv(size - len(data))
            if not chunk:
                raise DuckDBError(0, "00000", "Connection closed")
            data += chunk
        return data
    
    def _build_hello_payload(self) -> bytes:
        """Build HELLO message payload"""
        username_bytes = self.username.encode('utf-8')
        
        payload = struct.pack('<BBHI',
                             PROTOCOL_VERSION,  # protocol_version
                             ClientType.PYTHON.value,  # client_type
                             1,  # client_version
                             0)  # capabilities
        
        # Username
        payload += struct.pack('<I', len(username_bytes)) + username_bytes
        
        # Auth data (empty for now)
        payload += struct.pack('<I', 0)
        
        return payload
    
    def _parse_hello_response(self, data: bytes) -> Dict[str, Any]:
        """Parse HELLO_RESPONSE payload"""
        offset = 0
        
        status = data[offset]
        offset += 1
        
        session_id = struct.unpack_from('<Q', data, offset)[0]
        offset += 8
        
        protocol_version = data[offset]
        offset += 1
        
        server_capabilities = struct.unpack_from('<I', data, offset)[0]
        offset += 4
        
        info_len = struct.unpack_from('<I', data, offset)[0]
        offset += 4
        
        server_info = data[offset:offset+info_len].decode('utf-8')
        
        return {
            'status': status,
            'session_id': session_id,
            'protocol_version': protocol_version,
            'server_capabilities': server_capabilities,
            'server_info': server_info
        }
    
    def _build_query_payload(self, query_id: int, sql: str, 
                            timeout_ms: int, max_rows: int) -> bytes:
        """Build QUERY message payload"""
        sql_bytes = sql.encode('utf-8')
        
        payload = struct.pack('<QIII',
                             query_id,
                             0,  # flags
                             timeout_ms,
                             max_rows)
        
        payload += struct.pack('<I', len(sql_bytes)) + sql_bytes
        
        return payload
    
    def _parse_error(self, data: bytes) -> Tuple[int, str, str]:
        """Parse error payload"""
        offset = 0
        
        error_code = struct.unpack_from('<I', data, offset)[0]
        offset += 4
        
        sql_state = data[offset:offset+5].decode('ascii')
        offset += 5
        
        msg_len = struct.unpack_from('<I', data, offset)[0]
        offset += 4
        
        message = data[offset:offset+msg_len].decode('utf-8')
        
        return (error_code, sql_state, message)
    
    def _parse_schema(self, data: bytes) -> Tuple[List[str], List[str]]:
        """Parse schema from RESULT_HEADER"""
        offset = 0
        
        n_columns = struct.unpack_from('<Q', data, offset)[0]
        offset += 8
        
        columns = []
        types = []
        
        for _ in range(n_columns):
            # Name
            name_len = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            name = data[offset:offset+name_len].decode('utf-8')
            offset += name_len
            columns.append(name)
            
            # Format (type)
            format_len = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            fmt = data[offset:offset+format_len].decode('utf-8')
            offset += format_len
            types.append(fmt)
            
            # Nullable flag
            offset += 1
        
        return columns, types
    
    def _parse_batch(self, data: bytes, types: List[str]) -> List[Tuple]:
        """Parse data batch from RESULT_BATCH"""
        offset = 0

        # Read array metadata
        length = struct.unpack_from('<Q', data, offset)[0]
        offset += 8
        null_count = struct.unpack_from('<Q', data, offset)[0]
        offset += 8
        n_buffers = struct.unpack_from('<Q', data, offset)[0]
        offset += 8
        n_children = struct.unpack_from('<Q', data, offset)[0]
        offset += 8

        if length == 0:
            return []

        # Parse each column
        columns_data = []
        for col_idx in range(n_children):
            child_length = struct.unpack_from('<Q', data, offset)[0]
            offset += 8
            child_null_count = struct.unpack_from('<Q', data, offset)[0]
            offset += 8
            child_n_buffers = struct.unpack_from('<Q', data, offset)[0]
            offset += 8

            col_type = types[col_idx] if col_idx < len(types) else 'u'
            col_values = []

            # Parse buffers
            buffers = []
            for buf_idx in range(child_n_buffers):
                buf_size = struct.unpack_from('<Q', data, offset)[0]
                offset += 8
                if buf_size > 0:
                    buf_data = data[offset:offset+buf_size]
                    offset += buf_size
                    buffers.append(buf_data)
                else:
                    buffers.append(None)

            # Convert buffer data to values based on type
            col_values = self._decode_column(buffers, col_type, child_length, child_null_count)
            columns_data.append(col_values)

        # Transpose columns to rows
        rows = []
        for row_idx in range(length):
            row = tuple(col[row_idx] if row_idx < len(col) else None for col in columns_data)
            rows.append(row)

        return rows

    def _decode_column(self, buffers: List, col_type: str, length: int, null_count: int) -> List:
        """Decode column buffer data based on Arrow type format"""
        values = []

        # Skip validity bitmap buffer if present (index 0 when there are nulls)
        data_buf_idx = 1 if null_count > 0 and len(buffers) > 1 else 0

        if not buffers or data_buf_idx >= len(buffers) or buffers[data_buf_idx] is None:
            return [None] * length

        data_buf = buffers[data_buf_idx]

        try:
            if col_type in ('l', 'L'):  # INT64/UINT64
                fmt = '<q' if col_type == 'l' else '<Q'
                for i in range(length):
                    values.append(struct.unpack_from(fmt, data_buf, i * 8)[0])
            elif col_type in ('i', 'I'):  # INT32/UINT32
                fmt = '<i' if col_type == 'i' else '<I'
                for i in range(length):
                    values.append(struct.unpack_from(fmt, data_buf, i * 4)[0])
            elif col_type in ('s', 'S'):  # INT16/UINT16
                fmt = '<h' if col_type == 's' else '<H'
                for i in range(length):
                    values.append(struct.unpack_from(fmt, data_buf, i * 2)[0])
            elif col_type in ('c', 'C'):  # INT8/UINT8
                for i in range(length):
                    values.append(data_buf[i] if col_type == 'C' else struct.unpack_from('<b', data_buf, i)[0])
            elif col_type == 'g':  # DOUBLE
                for i in range(length):
                    values.append(struct.unpack_from('<d', data_buf, i * 8)[0])
            elif col_type == 'f':  # FLOAT
                for i in range(length):
                    values.append(struct.unpack_from('<f', data_buf, i * 4)[0])
            elif col_type == 'b':  # BOOL
                for i in range(length):
                    byte_idx = i // 8
                    bit_idx = i % 8
                    values.append(bool((data_buf[byte_idx] >> bit_idx) & 1))
            elif col_type in ('u', 'U', 'z', 'Z'):  # String/Binary types
                # For strings, we need offsets buffer (index 1) and data buffer (index 2)
                if len(buffers) >= 3 and buffers[1] and buffers[2]:
                    offsets_buf = buffers[1]
                    str_data_buf = buffers[2]
                    for i in range(length):
                        start = struct.unpack_from('<I', offsets_buf, i * 4)[0]
                        end = struct.unpack_from('<I', offsets_buf, (i + 1) * 4)[0]
                        if col_type in ('u', 'U'):  # UTF-8 strings
                            values.append(str_data_buf[start:end].decode('utf-8'))
                        else:  # Binary
                            values.append(bytes(str_data_buf[start:end]))
                else:
                    values = [None] * length
            else:
                # Unknown type - return None values
                values = [None] * length
        except Exception:
            values = [None] * length

        return values
    
    def _parse_result_end(self, data: bytes) -> Dict[str, int]:
        """Parse RESULT_END payload"""
        total_rows, execution_time_us, bytes_scanned = struct.unpack('<QQQ', data[:24])
        
        return {
            'total_rows': total_rows,
            'execution_time_us': execution_time_us,
            'bytes_scanned': bytes_scanned
        }
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# Convenience function
def connect(host: str = 'localhost', port: int = DEFAULT_PORT,
            username: str = '') -> DuckDBClient:
    """Create and connect a new client"""
    client = DuckDBClient(host, port, username)
    client.connect()
    return client


if __name__ == '__main__':
    # Simple test
    print("DuckDB Client Test")
    print("==================")
    
    try:
        with DuckDBClient('localhost', 9999) as client:
            print("Connected!")
            
            # Test ping
            if client.ping():
                print("Ping: OK")
            
            # Test query
            result = client.query("SELECT 1 + 1 AS result")
            print(f"Query result: {result}")
            
    except Exception as e:
        print(f"Error: {e}")
