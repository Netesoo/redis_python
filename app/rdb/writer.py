import struct
import time

class RDBWriter:
    def __init__(self, database):
        self.database = database

    def write_rbd(self, filename: str):
        with open(filename, 'wb') as f:
            f.write(RBDConstats.MAGIC)
            f.write(RBDConstats.VERSION)

            f.write(bytes([RBDConstats.SELECTDB]))
            f.write(bytes([0]))

            with self.database._conditon:
                for key, entry in self.database._store.items():
                    self._write_key_value(f, key, entry)

            f.write(bytes([RBDConstats.EOF]))
            f.write(b'\x00' * 8)

    def _write_key_value(self, f, key, entry):
        value = entry["value"]
        expires_at = entry.get("expires_at")

        if expires_at:
            f.write(bytes([RDBConstants.EXPIRETIME_MS]))
            f.write(struct.pack('<Q', expires_at))

        if isinstance(value, str):
            f.write(bytes([RDBConstants.TYPE_STRING]))
            self._write_string(f, key)
            self._write_string(f, value)
        elif isinstance(value, list):
            f.write(bytes([RDBConstants.TYPE_LIST]))
            self._write_string(f, key)
            self._write_list(f, value)

    def _write_string(self, f, s):
        encoded = s.encode('utf-8')
        self._write_length(f, len(encoded))
        f.write(encoded)
    
    def _write_list(self, f, lst):
        self._write_length(f, len(lst))
        for item in lst:
            self._write_string(f, item)
    
    def _write_length(self, f, length):
        if length < 64:
            f.write(bytes([length]))
        else:
            f.write(bytes([0x40 | (length >> 8)]))
            f.write(bytes([length & 0xFF]))