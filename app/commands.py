import fnmatch
import time 

from app.resp import (
    RESPSimpleString, RESPError, RESPInteger, RESPBulkString, RESPArray,
    ok, pong, error, wrongtype_error, null_bulk_string
)
from app.database import Stream

WRITE_COMMANDS = {
    "SET", "DEL", "INCR", "LPUSH", "RPUSH", "LPOP", "RPOP",
    "ZADD", "ZREM", "XADD"
}

def handle_command(command, args, database, context):
    if context.get("in_subscription") and command.upper() not in ("SUBSCRIBE", "UNSUBSCRIBE", "PING", "QUIT", "RESET"):
        return error(f"Can't execute '{command}': only SUBSCRIBE, UNSUBSCRIBE, PING, QUIT, and RESET allowed in subscription mode").encode()

    func = COMMANDS.get(command.upper())
    if not func:
        return error("unknown command").encode()
    
    if context.get("in_transaction") and command.upper() not in ("MULTI", "EXEC", "DISCARD"):
        context["transaction_queue"].append((command, args))
        return RESPSimpleString("QUEUED").encode()

    response = func(args, database, context)
    
    config = context.get("config", {})
    is_replica = context.get("is_replica", False)
    
    
    if not config.replicaof and not is_replica and command.upper() in WRITE_COMMANDS:
        print(f"[REPLICATION] This is master, replicating {command}")
        replicate_command(command, args, database)
    else:
        print(f"[REPLICATION] Not replicating: master={not config.replicaof}, replica_cmd={is_replica}")
    
    return response.encode() if hasattr(response, 'encode') else response

def replicate_command(command, args, database):
    replicas = database.get_connected_replicas()
    
    if not replicas:
        print("[REPLICATION] No replicas connected")
        return
    
    resp_array = RESPArray([RESPBulkString(command)] + [RESPBulkString(str(arg)) for arg in args])
    command_bytes = resp_array.encode()
    
    print(f"[REPLICATION] Command bytes to send: {command_bytes}")
    
    for replica in replicas[:]:  
        try:
            replica["socket"].sendall(command_bytes)
            print(f"[REPLICATION] Successfully sent {command} to replica at {replica['host']}:{replica['port']}")
        except Exception as e:
            print(f"[REPLICATION] Failed to send to replica {replica['host']}:{replica['port']}: {e}")
            database.remove_replica(replica["socket"])

def cmd_echo(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")
    return RESPBulkString(args[0])

def cmd_ping(args, database, context):
    if context.get("in_subscription"):
        return RESPArray([RESPBulkString("pong"), RESPBulkString("")])
    return pong()

def cmd_set(args, database, context):
    if len(args) not in (2, 4):
        return error("wrong number of arguments")

    key = args[0]
    value = args[1]

    px = None
    if len(args) == 4:
        if args[2].upper() != "PX":
            return error("only PX option supported")
        try:
            px = int(args[3])
        except ValueError:
            return error("PX value must be an integer")

    database.set(key, value, px=px)
    
    return ok()

def cmd_get(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")
    
    value = database.get(args[0])

    if value is None:
        return null_bulk_string()
    
    return RESPBulkString(value)

def cmd_rpush(args, database, context):
    if len(args) < 2:
        return error("wrong number of arguments")
    
    key = args[0]
    values = args[1:]

    try:
        new_len = database.rpush(key, *values)
        return RESPInteger(new_len)
    except TypeError:
        return wrongtype_error()

def cmd_lpush(args, database, context):
    if len(args) < 2:
        return error("wrong number of arguments")

    key = args[0]
    values = args[1:]

    try:
        new_len = database.lpush(key, *values)
        return RESPInteger(new_len)
    except TypeError:
        return wrongtype_error()

def cmd_lrange(args, database, context):
    if len(args) != 3:
        return error("wrong number of arguments")

    key = args[0]
    try:
        start = int(args[1])
        stop = int(args[2])
    except ValueError:
        return error("value is not an integer or out of range")

    try:
        result = database.lrange(key, start, stop)
        return RESPArray(result)
    except TypeError:
        return wrongtype_error()

def cmd_llen(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")

    key = args[0]

    try:
        result = database.llen(key)
        return RESPInteger(result)
    except TypeError:
        return wrongtype_error()

def cmd_lpop(args, database, context):
    if len(args) not in (1, 2):
        return error("wrong number of arguments")
    
    key = args[0]
    value = 1
    
    if len(args) == 2:
        try:
            value = int(args[1])
            if value < 0:
                return error("value cannot be negative")
        except ValueError:
            return error("value is not an integer")
    try:
        result = database.lpop(key, value)
        
        if not result:
            return null_bulk_string()
        
        if value == 1:
            return RESPBulkString(result[0])
        else:
            return RESPArray(result)
    except TypeError:
        return wrongtype_error()

def cmd_blpop(args, database, context):
    if len(args) != 2:
        return error("wrong number of arguments")

    key = args[0]
    try:
        timeout = float(args[1])
    except ValueError:
        return error("value is not a double")
    
    try:
        result = database.blpop(key, timeout)
    except TypeError:
        return wrongtype_error()

    if not result:
        return null_bulk_string()

    return RESPArray([result[0], result[1]])

def cmd_incr(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")

    key = args[0]

    try:
        result = database.incr(key)
    except TypeError:
        return wrongtype_error()

    if result == -1:
        return error("value is not an integer or out of range")
    return RESPInteger(result)

def cmd_multi(args, database, context):
    context["in_transaction"] = True
    context["transaction_queue"] = []
    return ok()

def cmd_exec(args, database, context):
    if not context.get("in_transaction"):
        return error("EXEC without MULTI")

    responses = []
    for cmd, cmd_args in context["transaction_queue"]:
        func = COMMANDS.get(cmd.upper())
        
        if not func:
            responses.append(error("unknown command"))
            continue
        
        try:
            resp = func(cmd_args, database, context)
        except Exception as e:
            resp = error(str(e))
        responses.append(resp)

    context["in_transaction"] = False
    context["transaction_queue"] = []
    return RESPArray(responses)

def cmd_discard(args, database, context):
    if not context.get("in_transaction"):
        return error("DISCARD without MULTI")

    context["in_transaction"] = False
    context["transaction_queue"] = []
    return ok()

def cmd_config(args, database, context):
    if len(args) < 1:
        return error("wrong number of arguments")

    subcommand = args[0].upper()

    if subcommand == "GET":
       if len(args) != 2:
           return error("wrong number of arguments")
       return cmd_config_get(args[1], database, context)
    else:
        return error(f"unknown CONFIG subcommand: {subcommand}")

def cmd_config_get(parameter, database, context):
    config = context.get("config", {})

    if parameter == "dir":
        return RESPArray(["dir", config.dir])
    elif parameter == "dbfilename":
        return RESPArray(["dbfilename", config.dbfilename])
    else:
        return RESPArray([])

def cmd_keys(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")
    
    pattern = args[0]
    print(f"KEYS pattern: '{pattern}' (type: {type(pattern)})")
    
    with database._condition:
        keys = []
        current_time = int(time.time() * 1000)
        
        for key, entry in database._store.items():
            print(f"Checking key: {key}, entry: {entry}")
            
            expires_at = entry.get("expires_at")
            if expires_at and current_time > expires_at:
                print(f"Key {key} is expired")
                continue
            
            print(f"Testing fnmatch.fnmatch('{key}', '{pattern}')")
            match_result = fnmatch.fnmatch(key, pattern)
            
            if match_result:
                print(f"Key {key} matches pattern {pattern}")
                keys.append(key)
        
        print(f"Returning keys: {keys}")
    
    return RESPArray(keys)

def cmd_subscribe(args, database, context):
    if len(args) < 1:
        return error("wrong number of arguments")

    context["in_subscription"] = True
    context["subscribed_channels"] = context.get("subscribed_channels", set())

    responses = []
    client_socket = context.get("client_socket")
    if not client_socket:
        return error("internal error: client socket not found")

    for channel in args:
        database.subscribe(channel, client_socket)
        context["subscribed_channels"].add(channel)
        responses.extend([
            RESPBulkString("subscribe"),
            RESPBulkString(channel),
            RESPInteger(len(context["subscribed_channels"]))
        ])

    return RESPArray(responses)

def cmd_publish(args, database, context):
    if len(args) != 2:
        return error("wrong number of arguments")

    channel = args[0]
    message = args[1]

    count = database.publish(channel, message)
    return RESPInteger(count)

def cmd_unsubscribe(args, database, context):
    if len(args) < 1:
        return error("wrong number of arguments")

    channel = args[0]
    context["subscribed_channels"] = context.get("subscribed_channels", set())
    
    client_socket = context.get("client_socket")
    if not client_socket:
        return error("internal error: client socket not found")

    database.unsubscribe(channel, client_socket)
    context["subscribed_channels"].remove(channel)
    return RESPArray([
        RESPBulkString("unsubscribe"),
        RESPBulkString(channel),
        RESPInteger(len(context["subscribed_channels"]))
    ])

def cmd_quit(args, database, context):
    if len(args) != 0:
        return error("wrong number of arguments")
    return ok()

def cmd_zadd(args, database, context):
    if len(args) < 3 or len(args) % 2 != 1:
        return error("wrong number of arguments")

    key = args[0]
    score_members = []

    try:
        for i in range(1, len(args), 2):
            score = float(args[i])
            member = args[i + 1]
            score_members.append((score, member))
    except ValueError:
        return error("score is not a valid float")

    try:
        result = database.zadd(key, *score_members)
        return RESPInteger(result)
    except TypeError:
        return wrongtype_error()

def cmd_zrank(args, database, context):
    if len(args) != 2:
        return error("wrong number of arguments")

    key = args[0]
    member = args[1]
    result = database.zrank(key, member)

    return null_bulk_string() if result == None else RESPInteger(result)

def cmd_zrange(args, database, context):
    if len(args) != 3:
        return error("wrong number of arguments")
    
    key = args[0]
    start = int(args[1])
    stop = int(args[2])
    result = database.zrange(key, start, stop)

    return RESPArray() if result == None else RESPArray(result)

def cmd_zcard(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")

    key = args[0]
    return RESPInteger(database.zcard(key))

def cmd_zscore(args, database, context):
    if len(args) != 2:
        return error("wrong number of arguments")

    key = args[0]
    member = args[1]
    result = database.zscore(key, member)

    return null_bulk_string if result == None else RESPBulkString(result) 

def cmd_zrem(args, database, context):
    if len(args) != 2:
        return error("wrong number of arguments")

    key = args[0]
    member = args[1]
    result = database.zrem(key, member)

    return RESPInteger(result)

def cmd_type(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")

    key = args[0]

    result = database._type(key)

    if type(result) == str:
        return RESPSimpleString("string")
    elif type(result) == list:
        return RESPSimpleString("list")
    elif type(result) == set:
        return RESPSimpleString("set")
    elif type(result) == Stream:
        return RESPSimpleString("stream")
    else:
        return RESPSimpleString("none")

def cmd_xadd(args, database, context):
    if len(args) < 3 or len(args) % 2 != 0:
        return error("wrong number of arguments")
    
    key = args[0]
    stream_id = args[1]
    fields_values = args[2:]
    
    try:
        result = database.xadd(key, stream_id, *fields_values)
        return RESPBulkString(result)
    except TypeError:
        return wrongtype_error()
    except ValueError as e:
        return error(str(e))
    
def cmd_xrange(args, database, context):
    if len(args) != 3:
        return error("wrong number of arguments")
    
    key = args[0]
    start = args[1]
    end = args[2]

    try:
        result = database.xrange(key, start, end)
        return RESPArray([
            RESPArray([RESPBulkString(entry_id), RESPArray(fields)
            ]) for entry_id, fields in result])
    except TypeError as e:
        print(f"TypeError in cmd_xrange: {e}")
        return wrongtype_error()
    except ValueError as e:
        print(f"ValueError in cmd_xrange: {e}")
        return error(str(e))

def cmd_xread(args, database, context):
    if len(args) < 3:
        return error("wrong number of arguments")
    
    count = None
    block = None
    i = 0
    
    while i < len(args):
        if args[i].upper() == "COUNT":
            if i + 1 >= len(args):
                return error("wrong number of arguments")
            try:
                count = int(args[i + 1])
                if count <= 0:
                    return error("COUNT must be > 0")
                i += 2
            except ValueError:
                return error("COUNT must be an integer")
        elif args[i].upper() == "BLOCK":
            if i + 1 >= len(args):
                return error("wrong number of arguments")
            try:
                block = int(args[i + 1])
                if block < 0:
                    return error("BLOCK timeout must be >= 0")
                i += 2
            except ValueError:
                return error("BLOCK timeout must be an integer")
        elif args[i].upper() == "STREAMS":
            i += 1
            break
        else:
            return error("syntax error")
    
    if i >= len(args):
        return error("wrong number of arguments")
    
    streams_part = args[i:]
    
    if len(streams_part) % 2 != 0:
        return error("Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.")
    
    num_streams = len(streams_part) // 2
    stream_keys = streams_part[:num_streams]
    stream_ids = streams_part[num_streams:]
    
    streams_and_ids = list(zip(stream_keys, stream_ids))
    
    try:
        result = database.xread(streams_and_ids, block)
        
        if not result:
            return null_bulk_string()
        
        response_streams = []
        for stream_key, entries in result:
            formatted_entries = []
            for entry_id, fields in entries:
                if count is not None and len(formatted_entries) >= count:
                    break
                formatted_entries.append(RESPArray([
                    RESPBulkString(entry_id),
                    RESPArray([RESPBulkString(field) for field in fields])
                ]))
            
            if formatted_entries:
                response_streams.append(RESPArray([
                    RESPBulkString(stream_key),
                    RESPArray(formatted_entries)
                ]))
        
        if not response_streams:
            return null_bulk_string()
            
        return RESPArray(response_streams)
        
    except TypeError as e:
        import traceback
        traceback.print_exc()
        return wrongtype_error()
    except ValueError as e:
        return error(str(e))

def cmd_info(args, database, context):
    if len(args) > 1:
        return error("wrong number of arguments")

    section = args[0].lower() if args else "default"

    if section in ("replication", "default"):
        config = context.get("config", {})

        is_replica = config.replicaof is not None

        if is_replica:
            master_host, master_port = config.replicaof.split(" ")
            info_lines = [
                "# Replication",
                "role:slave",
                f"master_host:{master_host}",
                f"master_port:{master_port}",
                "master_link_status:up",
                "slave_repl_offset:0",
                "slave_priority:100",
                "slave_read_only:1"
            ]
        else:
            connected_slaves = database.get_connected_replicas()

            info_lines = [
                "# Replication",
                "role:master",
                f"connected_slaves:{len(connected_slaves)}",
                "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                "master_replid2:0000000000000000000000000000000000000000",
                "master_repl_offset:0",
                "second_repl_offset:-1",
                "repl_backlog_active:0",
                "repl_backlog_size:1048576",
                "repl_backlog_first_byte_offset:0",
                "repl_backlog_histlen:0"
            ]

            for i, replica in enumerate(connected_slaves):
                info_lines.append(f"slave{i}:ip={replica["host"]},port={replica["port"]},state=online,offset=0,lag=0")

        info_text = "\r\n".join(info_lines) + "\r\n"
        return RESPBulkString(info_text)

    else:
        return RESPBulkString("")

def cmd_replconf(args, database, context):
    if len(args) < 2:
        return error("wrong number of arguments")

    subcommand = args[0].upper()

    if subcommand == "LISTENING-PORT":
        port = args[1]
        context["replica_port"] = port
        return ok()
    elif subcommand == "CAPA":
        capability = args[1].upper()
        if capability == "PSYNC2":
            context["replica_capabilities"] = ["psync2"]
            return ok()
        else:
            return error(f"unsupported capability: {capability}")
    else:
        return error(f"unknown REPLCONF subcommand: {subcommand}")

def cmd_psync(args, database, context):
    if len(args) != 2:
        return error("wrong number of arguments")
    
    replication_id = args[0]
    offset = args[1]
    
    print(f"[PSYNC] Received PSYNC {replication_id} {offset}")
    
    if replication_id == "?" and offset == "-1":
        master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        master_offset = 0
        
        client_socket = context.get("client_socket")
        if client_socket:
            try:
                client_addr = client_socket.getpeername()
                replica_host = client_addr[0]
                replica_port = context.get("replica_port", "unknown")
                database.add_replica(replica_host, replica_port, client_socket)
                print(f"[PSYNC] Added replica {replica_host}:{replica_port} to database")
            except Exception as e:
                print(f"[PSYNC] Error adding replica: {e}")
        
        fullresync_response = f"FULLRESYNC {master_replid} {master_offset}"
        print(f"[PSYNC] Sending: {fullresync_response}")
        
        if client_socket:
            try:
                client_socket.sendall(RESPSimpleString(fullresync_response).encode())
                
                empty_rdb = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
                rdb_bulk_string = f"${len(empty_rdb)}\r\n".encode() + empty_rdb
                client_socket.sendall(rdb_bulk_string)
                
                print(f"[PSYNC] Sent RDB dump ({len(empty_rdb)} bytes) to replica")
                
                return None
            except Exception as e:
                print(f"[PSYNC] Error sending RDB: {e}")
        
        return RESPSimpleString(fullresync_response)
    else:
        return error("unsupported PSYNC parameters")

def _match_pattern(key, pattern):
    return fnmatch.fnmatch(key, pattern)

COMMANDS = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
    "RPUSH": cmd_rpush,
    "LPUSH": cmd_lpush,
    "LRANGE": cmd_lrange,
    "LLEN": cmd_llen,
    "LPOP": cmd_lpop,
    "BLPOP": cmd_blpop,
    "INCR": cmd_incr,
    "MULTI": cmd_multi,
    "EXEC": cmd_exec,
    "DISCARD": cmd_discard,
    "CONFIG": cmd_config,
    "KEYS": cmd_keys,
    "SUBSCRIBE": cmd_subscribe,
    "PUBLISH": cmd_publish,
    "QUIT": cmd_quit,
    "UNSUBSCRIBE": cmd_unsubscribe,
    "ZADD": cmd_zadd,
    "ZRANK": cmd_zrank,
    "ZRANGE": cmd_zrange,
    "ZCARD": cmd_zcard,
    "ZSCORE": cmd_zscore,
    "ZREM": cmd_zrem,
    "TYPE": cmd_type,
    "XADD": cmd_xadd,
    "XRANGE": cmd_xrange,
    "XREAD": cmd_xread,
    "INFO": cmd_info,
    "REPLCONF": cmd_replconf,
    "PSYNC": cmd_psync,
}