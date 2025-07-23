from app.commands import handle_command
from app.resp import RESPType, RESPValue, parse_resp_with_offset


def handle_parsed_value(resp_value: RESPValue, database):
    if resp_value.type != RESPType.ARRAY:
        return b"-ERR expected array\r\n"

    items = resp_value.value
    if not items or items[0].type != RESPType.BULK_STRING:
        return b"-ERR invalid command format\r\n"

    command = items[0].value.upper()
    args = [item.value for item in items[1:]]
    return handle_command(command, args, database)


def handle_client(client, database):
    buffer = b""
    while data := client.recv(1024):
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