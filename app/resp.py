from enum import Enum
from dataclasses import dataclass
from typing import List, Union, Any

class RESPType(bytes, Enum):
    SIMPLE_STRING = b"+"
    ERROR = b"-"
    INTEGER = b":"
    BULK_STRING = b"$"
    ARRAY = b"*"

@dataclass
class RESPValue():
    type: RESPType
    value: any

class RESPSimpleString:
    def __init__(self, value: str):
        self.value = value
    
    def encode(self) -> bytes:
        return f"+{self.value}\r\n".encode()

class RESPError:
    def __init__(self, message: str):
        self.message = message
    
    def encode(self) -> bytes:
        return f"-{self.message}\r\n".encode()

class RESPInteger:
    def __init__(self, value: int):
        self.value = value
    
    def encode(self) -> bytes:
        return f":{self.value}\r\n".encode()

class RESPBulkString:
    def __init__(self, value: str = None):
        self.value = value
    
    def encode(self) -> bytes:
        if self.value is None:
            return b"$-1\r\n"
        return f"${len(self.value)}\r\n{self.value}\r\n".encode()

class RESPArray:
    def __init__(self, items: List[Union[str, 'RESPBulkString', 'RESPInteger', Any]] = None):
        self.items = items or []
    
    def encode(self) -> bytes:
        if not self.items:
            return b"*0\r\n"
        
        response = f"*{len(self.items)}\r\n"
        for item in self.items:
            if isinstance(item, str):
                response += f"${len(item)}\r\n{item}\r\n"
            elif hasattr(item, 'encode'):
                response += item.encode().decode()
            else:
                str_item = str(item)
                response += f"${len(str_item)}\r\n{str_item}\r\n"
        
        return response.encode()

def ok() -> RESPSimpleString:
    return RESPSimpleString("OK")

def pong() -> RESPSimpleString:
    return RESPSimpleString("PONG")

def error(message: str) -> RESPError:
    return RESPError(f"ERR {message}")

def wrongtype_error() -> RESPError:
    return RESPError("WRONGTYPE Operation against a key holding the wrong kind of value")

def null_bulk_string() -> RESPBulkString:
    return RESPBulkString(None)

def parse_resp_with_offset(data: bytes, offset: int) -> tuple[RESPValue, int]:
    if offset >= len(data):
        raise ValueError("Offset exceeds data length")

    try:
        resp_type = RESPType(data[offset:offset + 1])
    except ValueError:
        raise ValueError(f"Unknown RESP type: {data[offset:offset+1]}")

    offset += 1  # skip the type byte

    if resp_type in {RESPType.SIMPLE_STRING, RESPType.ERROR}:
        line, offset = read_line(data, offset)
        value = line.decode()
        return RESPValue(resp_type, value), offset

    elif resp_type == RESPType.INTEGER:
        line, offset = read_line(data, offset)
        value = int(line)
        return RESPValue(resp_type, value), offset

    elif resp_type == RESPType.BULK_STRING:
        line, offset = read_line(data, offset)
        length = int(line)

        if length == -1:
            return RESPValue(resp_type, None), offset
        value = data[offset:offset + length]
        offset += length
        if data[offset:offset + 2] != b"\r\n":
            raise ValueError("Invalid CRLF after bulk string")
        offset += 2
        return RESPValue(resp_type, value.decode()), offset

    elif resp_type == RESPType.ARRAY:
        line, offset = read_line(data, offset)
        count = int(line)
        items = []

        for _ in range(count):
            item, offset = parse_resp_with_offset(data, offset)
            items.append(item)
        return RESPValue(resp_type, items), offset

    raise NotImplementedError(f"Type {resp_type} not implemented")

def read_line(data: bytes, start: int) -> tuple[bytes, int]:
    end = data.find(b"\r\n", start)
    if end == -1:
        raise ValueError("Missing lines")
    return data[start:end], end + 2

class IncompleteRESPError(Exception):
    """Raised when RESP data is incomplete"""
    pass