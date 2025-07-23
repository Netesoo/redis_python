import socket  # noqa: F401
import threading
from app.database import Database
from app.handler import handle_client


def _gether(database: Database):
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        client_socket, client_addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, database)).start()


def main():
    database = Database()
    _gether(database)


if __name__ == "__main__":
    main()