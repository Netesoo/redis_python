import socket
from app.commands import handle_command, COMMANDS
from app.resp import RESPType, RESPValue, parse_resp_with_offset, error, RESPArray, RESPBulkString
from app.database import Database

def handle_parsed_value(resp_value: RESPValue, database, context):
    if resp_value.type != RESPType.ARRAY:
        return error("expected array").encode()

    items = resp_value.value
    if not items or items[0].type != RESPType.BULK_STRING:
        return error("invalid command format").encode()

    command = items[0].value.upper()
    args = [item.value for item in items[1:]]
    #return handle_command(command, args, database, context)
    
    response = handle_command(command, args, database, context)
    
    if command == "QUIT":
        return response, True

    if response is None:
        return b"", False

    return response, False

def handle_client(client, database, config=None):
    buffer = b""
    client_addr = client.getpeername()
    context = {
        "in_transaction": False,
        "transaction_queue": [],
        "in_subscription": False,
        "subscribed_channels": set(),
        "config": config or {},
        "client_socket": client,
        "client_addr": client_addr
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

def perform_handshake(master_host, master_port, config, database):
    try:
        master_socket = socket.create_connection((master_host, master_port))

        ping_command = RESPArray([RESPBulkString("PING")])
        master_socket.sendall(ping_command.encode())
        
        response = master_socket.recv(1024)
        print(f"Master response to PING: {response}")

        replconf_port = RESPArray([
            RESPBulkString("REPLCONF"),
            RESPBulkString("listening-port"),
            RESPBulkString(str(config.port))
        ])
        master_socket.sendall(replconf_port.encode())

        response = master_socket.recv(1024)
        print(f"Master response to REPLCONF listenig-port: {response}")

        replconf_capa = RESPArray([
            RESPBulkString("REPLCONF"),
            RESPBulkString("capa"),
            RESPBulkString("psync2")
        ])
        master_socket.sendall(replconf_capa.encode())

        response = master_socket.recv(1024)
        print(f"Master response to REPLCONF capa: {response}")

        psync_command = RESPArray([
            RESPBulkString("PSYNC"),
            RESPBulkString("?"),
            RESPBulkString("-1")
        ])
        master_socket.sendall(psync_command.encode())
        
        response = master_socket.recv(1024)
        print(f"Master response to PSYNC: {response}")

        listen_to_master(master_socket, config, database)

    except Exception as e:
        print(f"Handshake error: {e}")

def listen_to_master(master_socket, config, database):
    buffer = b""
    rdb_received = False
    replica_offset = 0

    while True:
        try:
            data = master_socket.recv(1024)
            if not data:
                print("Master connection closed")
                break
                
            buffer += data
            offset = 0
            
            if not rdb_received:
                offset = skip_rdb_dump(buffer)
                if offset == -1:
                    continue
                else:
                    print("RDB dump received and skipped")
                    rdb_received = True
                    buffer = buffer[offset:]
                    offset = 0
            
            while offset < len(buffer):
                try:
                    command_start_offset = offset
                    value, new_offset = parse_resp_with_offset(buffer, offset)

                    command_bytes = buffer[command_start_offset:new_offset]
                    command_length = len(command_bytes)

                    offset = new_offset
                    
                    if value.type == RESPType.ARRAY and value.value:
                        command = value.value[0].value.upper()
                        args = [item.value for item in value.value[1:]]
                        
                        print(f"Replica received command: {command} {args}")
                        
                        if not (command == "REPLCONF" and len(args) > 0 and args[0].upper() == "GETACK"):
                            replica_offset += command_length
                        else:
                            print(f"REPLCONF GETACK - not updating offset")

                        context = {
                            "config": config, 
                            "is_replica": True,
                            "replication_offset": replica_offset
                            }

                        response = handle_command(command, args, database, context)

                        if (command == "REPLCONF" and len(args) > 0 and args[0].upper() == "GETACK"):
                            if response:
                                # Sprawdź czy response to już bytes czy trzeba je zakodować
                                if isinstance(response, bytes):
                                    response_bytes = response
                                elif hasattr(response, 'encode'):
                                    response_bytes = response.encode()
                                else:
                                    print(f"Invalid response type for GETACK: {type(response)}")
                                    continue
                                
                                master_socket.sendall(response_bytes)
                                print(f"Replica sent ACK response to master: {response_bytes}")
                            else:
                                print(f"No response for GETACK")
                        
                        print(f"Replica executed: {command}, current offset: {replica_offset}")
                       
                except Exception as e:
                    print(f"Error parsing replica command: {e}")
                    break
            
            buffer = buffer[offset:]
            
        except Exception as e:
            print(f"Error listening to master: {e}")
            break
    
    master_socket.close()

def skip_rdb_dump(buffer):
    if len(buffer) < 1:
        return -1
    
    if buffer[0:1] != b'$':
        return -1
    
    crlf_pos = buffer.find(b'\r\n', 1)
    if crlf_pos == -1:
        return -1
    
    try:
        length_str = buffer[1:crlf_pos].decode()
        rdb_length = int(length_str)
        
        rdb_start = crlf_pos + 2
        rdb_end = rdb_start + rdb_length
        
        if len(buffer) < rdb_end:
            print(f"RDB dump incomplete: have {len(buffer)} bytes, need {rdb_end}")
            return -1
        
        print(f"RDB dump complete: {rdb_length} bytes")
        return rdb_end
        
    except ValueError as e:
        print(f"Error parsing RDB length: {e}")
        return -1