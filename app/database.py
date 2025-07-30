import json
import time
import threading
import os


def current_millis():
    return int(time.time() * 1000)


class Database:
    def __init__(self, path="db.json"):
        self._store = {}
        self._lock = threading.Lock()
        self.path = path
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                return json.load(f)

    def _save(self):
        with open(self.path, "w") as f:
            json.dump(self._store, f)

    def set(self, key: str, value: str, px: int = None):
        with self._lock:
            entry = {"value": value}
            if px:
                entry["expires_at"] = current_millis() + px
            self._store[key] = entry
            self._save()

    def get(self, key: str) -> str | None:
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            
            expires_at = entry.get("expires_at")
            if expires_at and current_millis() > expires_at:
                del self._store[key]
                self._save()
                return None

            return entry["value"]

    def delete(self, key: str):
        with self._lock:
            if key in self._store:
                del self._store[key]
                self._save()