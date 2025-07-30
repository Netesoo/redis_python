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
    if len(args) not in (2, 4):
        return b"-ERR wrong number of arguments\r\n"

    key = args[0]
    value = args[1]

    px = None
    if len(args) == 4:
        if args[2].upper() != "PX":
            return b"-ERR only PX option suported\r\n"
        try:
            px = int(args[3])
        except ValueError:
            return b"-ERR PX value must be an integer\r\n"

    database.set(key, value, px=px)
    return b"+OK\r\n"


def cmd_get(args, database):
    if len(args) != 1:
        return b"-ERR wrong number of arguments\r\n"
    
    value = database.get(args[0])
    if value is None:
        return b"$-1\r\n"
    
    return f"${len(value)}\r\n{value}\r\n".encode()


def cmd_rpush(args, database):
    if len(args) < 2:
        return b"-ERR wrong number of arguments\r\n"
    
    key = args[0]
    values = args[1:]

    try:
        new_len = database.rpush(key, *values)
        return f":{new_len}\r\n".encode()
    except TypeError:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def cmd_lpush(args, database):
    if len(args) < 2:
        return b"-ERR wrong number of arguments\r\n"

    key = args[0]
    values = args[1:]

    try:
        new_len = database.lpush(key, *values)
        return f":{new_len}\r\n".encode()
    except TypeError:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"



def cmd_lrange(args, database):
    if len(args) != 3:
        return b"-ERR wrong number of arguments\r\n"

    key = args[0]
    try:
        start = int(args[1])
        stop = int(args[2])
    except ValueError:
        return b"-ERR value is not an integer or out of range\r\n"

    try:
        result = database.lrange(key, start, stop)
        response = f"*{len(result)}\r\n"
        for item in result:
            response += f"${len(item)}\r\n{item}\r\n"

        if result == []:
            return f"*0\r\n".encode()

        return response.encode()
    except TypeError:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def cmd_llen(args, database):
    if len(args) != 1:
        return b"-ERR wrong number of arguments\r\n"

    key = args[0]

    try:
        result = database.llen(key)
        return f":{result}\r\n".encode()
    except TypeError:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def cmd_lpop(args, database):
    if len(args) < 1:
        return b"-ERR wrong number of arguments\r\n"

    key = args[0]

    try:
        result = database.lpop(key)
        if result == []:
            return f"$-1\r\n".encode()
        else:
            return f"${len(result[0])}\r\n{result[0]}\r\n".encode()
    except TypeError:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"



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
}