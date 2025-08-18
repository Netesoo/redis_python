import socket
import threading
from app.database import Database
from app.handler import handle_client, perform_handshake
from app.args import parse_args

def _gether(database: Database, config: dict):
    server_socket = socket.create_server(("localhost", config.port), reuse_port=True)
    while True:
        client_socket, client_addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, database, config)).start()

def main():
    config = parse_args()
    database = Database()

    if config.replicaof:
        host, port = config.replicaof.split(" ")
        print(f"Starting as replica of {host}:{port}")
        threading.Thread(target=perform_handshake, args=(host, int(port), config)).start()
    else:
        print("Starting as master")
    
    if not config.replicaof and config.dir and config.dbfilename:
        rdb_path = f"{config.dir}/{config.dbfilename}"
        print(f"loading RDB from: {rdb_path}")
        database.load_rdb(rdb_path)

    _gether(database, config)

if __name__ == "__main__":
    main()