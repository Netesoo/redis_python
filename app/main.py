import socket  # noqa: F401


def main():
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()
    

    while True:
        request: bytes = connection.recv(1024)
        data: str = request.decode()


        if "ping" in data.lower():
            connection.sendall("+PONG\r\n".encode())
    

if __name__ == "__main__":
    main()
