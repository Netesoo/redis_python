from typing import Any
from abc import ABC, abstractmethod
from socket import socket

from .rdb.reader import RDBReader
from .rdb.writer import RDBWriter
from app.resp import (
    RESPSimpleString, RESPError, RESPInteger, RESPBulkString, RESPArray,
    ok, pong, error, wrongtype_error, null_bulk_string
)

import time
import threading

def current_millis():
    return int(time.time() * 1000)

class Database:
    def __init__(self):
        self._store = {}
        self._condition = threading.Condition()
        self._subscriptions = {}

    def set(self, key: str, value: Any, px: int = None):
        with self._condition:
            entry = {"value": value}
            if px:
                entry["expires_at"] = current_millis() + px
            self._store[key] = entry

    def get(self, key: str) -> Any | None:
        with self._condition:
            entry = self._store.get(key)
            if not entry:
                return None

            expires_at = entry.get("expires_at")
            if expires_at and current_millis() > expires_at:
                del self._store[key]
                return None

            return entry["value"]

    def delete(self, key: str):
        with self._condition:
            if key in self._store:
                del self._store[key]

    def save_rdb(self, filename: str):
        writer = RDBWriter(self)
        writer.write_rdb(filename)
        
    def load_rdb(self, filename: str):
        reader = RDBReader(self)
        reader.load_rdb(filename)
        print(f"Trying to load RDB from: {filename}")
        try:
            print(f"Successfully loaded RDB, store now has: {list(self._store.keys())}")
        except FileNotFoundError:
            print(f"RDB file {filename} not found, starting with empty database")
        except Exception as e:
            print(f"Error loading RDB: {e}")

    def rpush(self, key: str, *values: str) -> int:
        with self._condition:
            entry = self._store.get(key)
            if entry:
                if not isinstance(entry["value"], list):
                    raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
                entry["value"].extend(values)
            else:
                self._store[key] = {"value": list(values)}

            self._condition.notify()
            return len(self._store[key]["value"])

    def lpush(self, key: str, *values: str) -> int:
        with self._condition:
            entry = self._store.get(key)
            if entry:
                if not isinstance(entry["value"], list):
                    raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
                for value in values:
                    entry["value"].insert(0, value)
            else:
                self._store[key] = {"value": list(reversed(values))}
            
            self._condition.notify()
            return len(self._store[key]["value"])

    def lrange(self, key: str, start: int, stop: int) -> list:
        with self._condition:
            entry = self._store.get(key)

            if not entry:
                return []

            if not isinstance(entry["value"], list):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            lst = entry["value"]
            length = len(lst)

            if length == 0:
                return []

            if start >= length:
                return []

            if stop >= length:
                stop = length - 1

            if start < 0:
                start = length + start
            if stop < 0:
                stop = length + stop

            if start < 0:
                start = 0
            if stop >= length:
                stop = length - 1

            if start > stop:
                return []

            return lst[start:stop + 1]

    def llen(self, key: str) -> int:
        with self._condition:
            entry = self._store.get(key)
            if entry:
                if not isinstance(entry["value"], list):
                    raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            elif entry == None:
                return 0

            return len(entry["value"])

    def lpop(self, key: str, val=1) -> list:
        with self._condition:
            entry = self._store.get(key)

            if entry:
                if not isinstance(entry["value"], list):
                    raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            elif entry == None:
                return []

            lst = []
            for i in range(val):
                lst.append(entry["value"].pop(0))

            return lst

    def blpop(self, key: str, timeout: float) -> list:
        end_time = time.time() + timeout

        with self._condition:
            while True:
                entry = self._store.get(key)

                if entry:
                    if not isinstance(entry["value"], list):
                        raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

                    if entry["value"]:
                        value = entry["value"].pop(0)
                        return [key, value]

                remaining = end_time - time.time()
                if timeout > 0 and remaining <= 0:
                    return []

                self._condition.wait(timeout=remaining if timeout > 0 else None)

    def incr(self, key: str) -> int:
        with self._condition:
            entry = self._store.get(key)

            if entry == None:
                entry = {"value": 0}

            try:
                result = int(entry['value']) + 1
                entry = {"value": f"{result}"}
            except ValueError:
                return -1

            self._store[key] = entry
            return result

    def subscribe(self, channel: str, client: socket):
        with self._condition:
            if channel not in self._subscriptions:
                self._subscriptions[channel] = []
            if client not in self._subscriptions[channel]:
                self._subscriptions[channel].append(client)
                print(f"Client subscribed to channel: {channel}")
                
    def publish(self, channel: str, message: str) -> int:
        while self._condition:
            if channel not in self._subscriptions:
                return 0
            subscribers = self._subscriptions[channel]
            for client in subscribers[:]:
                try:
                    response = RESPArray([
                        RESPBulkString("message"),
                        RESPBulkString(channel),
                        RESPBulkString(message)
                    ])
                    client.sendall(response.encode())
                except Exception as e:
                    print(f"Error sending t client: {e}")
                    self._subscriptions[channel].remove(client)
            return len(subscribers)

    def unsubscribe(self, channel:str, client: socket):
        with self._condition:
            if channel in self._subscriptions:
                if client in self._subscriptions[channel]:
                    self._subscriptions[channel].remove(client)
                    print(f"Client unsubscribed from channel: {channel}")
                if not self._subscriptions[channel]:
                    del self._subscriptions[channel]
    
    def zadd(self, key: str, *score_members: tuple[float, str]) -> int:
        with self._condition:
            entry = self._store.get(key)
            if entry and not isinstance(entry["value"], SortedSet):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            if not entry:
                self._store[key] = {"value": SortedSet()}

            added_count = 0
            sorted_set = self._store[key]["value"]

            for score, member in score_members:
                added_count += sorted_set.add(member, score)

            return added_count

    def zrank(self, key: str, member: str) -> int | None:
        with self._condition:
            entry = self._store.get(key)
            if not entry:
                return None
            if not isinstance(entry["value"], SortedSet):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return entry["value"].get_rank(member)

    def zrange(self, key: str, start: int, stop: int) -> list | None:
        with self._condition:
            entry = self._store.get(key)

            if not entry:
                return None
            if not isinstance(entry["value"], SortedSet):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            lst = entry["value"]._sorted_list

            length = len(lst)

            if length == 0:
                return []

            if start >= length:
                return []

            if stop >= length:
                stop = length - 1

            if start < 0:
                start = length + start
            if stop < 0:
                stop = length + stop

            if start < 0:
                start = 0
            if stop >= length:
                stop = length - 1

            if start > stop:
                return []

            return lst[start:stop + 1]

    def zcard(self, key: str) -> int:
        with self._condition:
            entry = self._store.get(key)

            if not entry:
                return 0
            if not isinstance(entry["value"], SortedSet):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return len(entry["value"]._sorted_list)

    def zscore(self, key: str, member: str) -> str | None:
        with self._condition:
            entry = self._store.get(key)

            if not entry:
                return None
            if not isinstance(entry["value"], SortedSet):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        return str(entry["value"].get_score(member))

    def zrem(self, key: str, member:str) -> int:
        with self._condition:
            entry = self._store.get(key)

            if not entry:
                return 0
            if not isinstance(entry["value"], SortedSet):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

            return entry["value"].delete(member)

    def _type(self, key: str) -> Any | None:
        with self._condition:
            entry = self._store.get(key)
            
            if not entry:
                return None
            return entry["value"]

    def xadd(self, key: str, stream_id: str, *fields_values: str) -> str:
        with self._condition:
            entry = self._store.get(key)
            if entry and not isinstance(entry["value"], Stream):
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            
            if not entry:
                self._store[key] = {"value": Stream()}
        
            stream = self._store[key]["value"]
            return stream.add(stream_id, *fields_values)

           

class SortedSet:
    def __init__(self):
        self._members = {}
        self._sorted_list = []

    def add(self, member, score):
        added = member not in self._members  
        self._members[member] = float(score) 
        if added:
            self._sorted_list.append(member)
        else:
            self._sorted_list.remove(member)
            self._sorted_list.append(member)

        self._sorted_list.sort(key=lambda x: (self._members[x], x))
        return 1 if added else 0

    def get_score(self, member):
        return self._members.get(member)

    def get_rank(self, member):
        try:
            return self._sorted_list.index(member)
        except ValueError:
            return None

    def delete(self, member):
        if member in self._members:
            self._sorted_list.remove(member)
            return 1
        return 0

    def __len__(self):
        return len(self._sorted_list)

class Stream:
    def __init__(self):
        self._entries = []
    
    def add(self, stream_id: str, *fields_values: str) -> str:
        if len(fields_values) % 2 != 0:
            raise ValueError("wrong number of arguments for field-value pairs")
            
        if stream_id == "*":
            current_ms = current_millis()
            seq = 0
            if self._entries:
                last_id = self._entries[-1][0]
                last_ms, last_seq = map(int, last_id.split("-"))
                if last_ms == current_ms:
                    seq = last_seq + 1
                elif last_ms > current_ms:
                    raise ValueError("invalid ID: timestamp is in the past")
            new_id = f"{current_ms}-{seq}"
        else:
            try:
                ms, seq = map(int, stream_id.split("-"))
                if ms < 0 or seq < 0:
                    raise ValueError("The ID specified in xADD can not have negative timestamp or sequence")
                if ms == 0 and seq == 0:
                    raise ValueError("The ID specified in XADD must be greater than 0-0")
                if self._entries:
                    last_id = self._entries[-1][0]
                    last_ms, last_seq = map(int, last_id.split("-"))
                    if ms < last_ms or (ms == last_ms and seq <= last_seq):
                        raise ValueError("The ID specified in XADD is equal or smaller than the target stream top item")
                
                new_id = stream_id
            except ValueError as e:
                if str(e).startswith("The ID"):
                    raise
                raise ValueError("invalid ID format")

        entry_dict= {}
        for i in range(0, len(fields_values), 2):
            entry_dict[fields_values[i]] = fields_values[i + 1]

        self._entries.append((new_id, entry_dict))

        return new_id 
