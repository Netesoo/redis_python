import fnmatch
import time 

from app.resp import (
    RESPSimpleString, RESPError, RESPInteger, RESPBulkString, RESPArray,
    ok, pong, error, wrongtype_error, null_bulk_string
)


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
    return response.encode() if hasattr(response, 'encode') else response


def cmd_echo(args, database, context):
    if len(args) != 1:
        return error("wrong number of arguments")
    return RESPBulkString(args[0])


def cmd_ping(args, database, context):
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
            match_result = fnmatch.fnmatch(key, pattern)  # Note: key first, pattern second
            print(f"Match result: {match_result}")
            
            if match_result:
                print(f"Key {key} matches pattern {pattern}")
                keys.append(key)
        
        print(f"Returning keys: {keys}")
    
    return RESPArray(keys)


def _match_pattern(key, pattern):
    return fnmatch.fnmatch(key, pattern)


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
}