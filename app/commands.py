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

    current = database.get(key)
    
    if current is None:
        current = []
    elif not isinstance(current, list):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

    current.extend(values)
    database.set(key, current)
    
    return f":{len(current)}\r\n".encode()


COMMANDS = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
    "RPUSH": cmd_rpush,
}