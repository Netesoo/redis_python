from app.resp import (
    RESPSimpleString, RESPError, RESPInteger, RESPBulkString, RESPArray,
    ok, pong, error, wrongtype_error, null_bulk_string
)


def handle_command(command, args, database):
    func = COMMANDS.get(command.upper())
    if not func:
        return error("unknown command").encode()
    
    response = func(args, database)
    return response.encode() if hasattr(response, 'encode') else response


def cmd_echo(args, database):
    if len(args) != 1:
        return error("wrong number of arguments")
    return RESPBulkString(args[0])


def cmd_ping(args, database):
    return pong()


def cmd_set(args, database):
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


def cmd_get(args, database):
    if len(args) != 1:
        return error("wrong number of arguments")
    
    value = database.get(args[0])
    if value is None:
        return null_bulk_string()
    
    return RESPBulkString(value)


def cmd_rpush(args, database):
    if len(args) < 2:
        return error("wrong number of arguments")
    
    key = args[0]
    values = args[1:]

    try:
        new_len = database.rpush(key, *values)
        return RESPInteger(new_len)
    except TypeError:
        return wrongtype_error()


def cmd_lpush(args, database):
    if len(args) < 2:
        return error("wrong number of arguments")

    key = args[0]
    values = args[1:]

    try:
        new_len = database.lpush(key, *values)
        return RESPInteger(new_len)
    except TypeError:
        return wrongtype_error()


def cmd_lrange(args, database):
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


def cmd_llen(args, database):
    if len(args) != 1:
        return error("wrong number of arguments")

    key = args[0]

    try:
        result = database.llen(key)
        return RESPInteger(result)
    except TypeError:
        return wrongtype_error()


def cmd_lpop(args, database):
    if len(args) < 1:
        return error("wrong number of arguments")
    elif len(args) == 2:
        try:
            val = int(args[1])
        except ValueError:
            return error("value is not an integer")
    else: 
        val = 1
    
    key = args[0]
    try:
        result = database.lpop(key, val)
        
        if not result:  # Pusta lista
            return null_bulk_string()
        
        if val == 1:
            return RESPBulkString(result[0])
        else:
            return RESPArray(result)

    except TypeError:
        return wrongtype_error()


def cmd_blpop(args, database):
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


def cmd_incr(args, database):
    if len(args) != 1:
        return error("wrong number of arguments")

    key = args[0]

    try:
        result = database.incr(key)
    except TypeError:
        return wrongtype_error()

    return RESPInteger(result)

    
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
}