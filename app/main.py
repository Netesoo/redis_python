import socket  # noqa: F401
import threading

buff_size: int = 1024


def handle_client(client: socket.socket):
        while data := client.recv(buff_size):
            if data == b"":
                break

            client.sendall("+PONG\r\n".encode())


def _gether():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        client_socket, client_addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket,)).start()


def main():
    _gether()

if __name__ == "__main__":
    main()