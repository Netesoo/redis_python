from enum import Enum
from dataclasses import dataclass


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