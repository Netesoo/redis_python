import socket  # noqa: F401
import threading
import json
import os
from enum import Enum
from dataclasses import dataclass


buff_size: int = 1024


class Database:
    def __init__(self, filename="data.json"):
        self.filename = filename
        self.store = self._load()

    def _load(self):
        if os.path.exists(self.filename):
            with open(self.filename, "r") as f:
                return json.load(f)
        return {}

    def _save(self):
        with open(self.filename, "w") as f:
            json.dump(self.store, f)

    def set(self, key, value):
        self.store[key] = value
        self._save()

    def get(self, key):
        return self.store.get(key)



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
        raise ValueError("Brakuje końca linii")
    return data[start:end], end + 2


def handle_parsed_value(resp_value: RESPValue, database: Database):
    if resp_value.type != RESPType.ARRAY:
        return b"-ERR expected array\r\n"

    items = resp_value.value
    if not items or items[0].type != RESPType.BULK_STRING:
        return b"-ERR invalid command format\r\n"

    command = items[0].value.upper()
    args = [item.value for item in items[1:]]
    print(f"{args}")
    return handle_command(command, args, database)


def handle_command(command, args, database):
    func = COMMANDS.get(command.upper())
    if not func:
        return b"-ERR unknown command\r\n"
    return func(args, database)


def cmd_echo(args, database):
    if len(args) != 1:
            return b"-ERR wrong number of arguments\r\n"
    msg = args[0]
    return f"${len(msg)}\r\n{msg}\r\n".encode()


def cmd_ping(args, database):
    msg = "PONG"
    return f"+PONG\r\n".encode()


def cmd_set(args, database):
    if len(args) != 2:
        return b"-ERR wrong number of arguments\r\n"
    database.set(args[0], args[1])
    return f"+OK\r\n".encode()


def cmd_get(args, database):
    if len(args) != 1:
        return b"-ERR wrong number of arguments\r\n"
    
    value = database.get(args[0])
    if value is None:
        return b"$-1\r\n"
    
    return f"${len(value)}\r\n{value}\r\n".encode()


COMMANDS = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
}


def handle_client(client: socket.socket, database: Database):
    buffer = b""
    while data := client.recv(buff_size):
        buffer += data
        offset = 0

        while offset < len(buffer):
            try:
                value, offset = parse_resp_with_offset(buffer, offset)
#                print(f"Got: {value.value}")

                client.sendall(handle_parsed_value(value, database))
            except IncompleteRESPError:
                break
        
        buffer = buffer[offset:]


def _gether(database: Database):
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        client_socket, client_addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, database)).start()


def main():
    database = Database()
    _gether(database)


if __name__ == "__main__":
    main()