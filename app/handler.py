import socket
from app.commands import handle_command
from app.resp import RESPType, RESPValue, parse_resp_with_offset, error, RESPArray, RESPBulkString

def handle_parsed_value(resp_value: RESPValue, database, context):
    if resp_value.type != RESPType.ARRAY:
        return error("expected array").encode()

    items = resp_value.value
    if not items or items[0].type != RESPType.BULK_STRING:
        return error("invalid command format").encode()

    command = items[0].value.upper()
    args = [item.value for item in items[1:]]
    #return handle_command(command, args, database, context)

    if command == "QUIT":
        return handle_command(command, args, database, context), True
    return handle_command(command, args, database, context), False

def handle_client(client, database, config=None):
    buffer = b""
    context = {
        "in_transaction": False,
        "transaction_queue": [],
        "in_subscription": False,
        "subscribed_channels": set(),
        "config": config or {},
        "client_socket": client
    }

    while data := client.recv(1024):
        buffer += data
        offset = 0

        while offset < len(buffer):
            try:
                value, offset = parse_resp_with_offset(buffer, offset)
                response, should_close = handle_parsed_value(value, database, context)
                if response:
                    client.sendall(response)
                if should_close:
                    break
            except IncompleteRESPError:
                break
            except Exception as e:
                print(f"Error handling client: {e}")
                break
        
        buffer = buffer[offset:]
    
    if context.get("in_subscription"):
        for channel in context["subscribed_channels"]:
            database.unsubscribe(channel, client)
    client.close()

def perform_handshake(master_host, master_port, config):
    try:
        master_socket = socket.create_connection((master_host, master_port))

        ping_command = RESPArray([RESPBulkString("PING")])
        master_socket.sendall(ping_command.encode())
        
        response = master_socket.recv(1024)
        print(f"Master response to PING: {response}")

    except Exception as e:
        print(f"Handshake error: {e}")