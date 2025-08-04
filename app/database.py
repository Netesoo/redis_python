from typing import Any
from abc import ABC, abstractmethod
import time
import threading


def current_millis():
    return int(time.time() * 1000)


class Database:
    def __init__(self):
        self._store = {}
        self._condition = threading.Condition()


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

    
#    def multi(self)