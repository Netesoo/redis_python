# Custom Redis Implementation (Python)
A high-performance, asynchronous-ready Redis server implementation from scratch. This project was built as part of the CodeCrafters "Build Your Own Redis" challenge to explore network protocols, data serialization, and distributed system fundamentals.

## 🚀 Implemented Features
RESP Protocol Parser: Full implementation of the REdis Serialization Protocol, handling Simple Strings, Errors, Integers, Bulk Strings, and Arrays.

Core Commands: Support for PING, ECHO, SET, GET (with TTL/expiry support), and INFO.

Replication: Master-Slave replication logic, including handshake sequences and command propagation.

RDB Persistence: Ability to parse RDB files to restore server state on startup.

Streams: Support for Redis Streams (XADD, XREAD) including blocking read operations.

## 🛠️ Technical Challenges & Learning
Protocol Accuracy: Implementing the RESP protocol required strict adherence to the specification to ensure compatibility with standard Redis clients.

Concurrency: Managed multiple client connections using Python's threading and socket modules, ensuring thread-safe data access.

Regression Analysis: (Tutaj możesz dodać to, o czym rozmawialiśmy) After a period of inactivity, I performed a manual audit of the BLPOP logic to align it with updated test specifications, demonstrating my ability to debug and refactor "legacy" code.

## ⚙️ How to Run
While this project is designed to be tested via the CodeCrafters CLI, you can run the server locally:

Prerequisites: Python 3.13+

Run the server:

Bash
./your_program.sh
Note: The server defaults to port 6379.

## 🧪 Testing
The project was rigorously tested using the CodeCrafters CLI integration suite, covering:

Protocol compliance.

Replication consistency.

Multi-client concurrency.
