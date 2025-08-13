import struct
import time

class RDBReader:
    def __init__(self, database):
        self.database = database
    
    def load_rdb(self, filename: str):
        try:
            with open(filename, 'rb') as f:
                print(f"Loading RDB file: {filename}")
                self._read_rdb(f)
                print(f"RDB loaded successfully. Keys in database: {list(self.database._store.keys())}")
        except FileNotFoundError:
            print(f"RDB file {filename} not found, starting with empty database")
        except Exception as e:
            print(f"Error loading RDB: {e}")
            import traceback
            traceback.print_exc()
    
    def _read_rdb(self, f):
        # Read and verify magic string
        magic = f.read(5)
        if magic != b"REDIS":
            raise ValueError(f"Invalid RDB file: expected REDIS, got {magic}")
        
        # Read version (4 bytes)
        version = f.read(4)
        print(f"RDB version: {version.decode()}")
        
        while True:
            byte = f.read(1)
            if not byte:
                break
                
            opcode = byte[0]
            print(f"Processing opcode: 0x{opcode:02x}")
            
            if opcode == 0xFF:  # EOF
                print("Found EOF, stopping")
                break
            elif opcode == 0xFE:  # SELECTDB
                db_number = self._read_length(f)
                print(f"Selected database: {db_number}")
            elif opcode == 0xFD:  # EXPIRETIME (seconds)
                expire_time = struct.unpack('<I', f.read(4))[0]
                self._read_key_value_pair(f, expire_time * 1000)  # Convert to milliseconds
            elif opcode == 0xFC:  # EXPIRETIME_MS
                expire_time = struct.unpack('<Q', f.read(8))[0]
                self._read_key_value_pair(f, expire_time)
            elif opcode == 0xFB:  # RESIZEDB
                # Hash table size info - just skip it
                hash_table_size = self._read_length(f)
                expire_hash_table_size = self._read_length(f)
                print(f"Hash table sizes: {hash_table_size}, {expire_hash_table_size}")
            elif opcode == 0xFA:  # AUX
                # Auxiliary fields (metadata) - skip them
                key = self._read_redis_string(f)
                value = self._read_redis_string(f)
                print(f"Auxiliary field: {key} = {value}")
            else:
                # This should be a value type
                self._read_key_value_pair(f, None, opcode)
    
    def _read_key_value_pair(self, f, expires_at=None, value_type=None):
        if value_type is None:
            value_type_byte = f.read(1)
            if not value_type_byte:
                return
            value_type = value_type_byte[0]
        
        print(f"Reading key-value pair, type: {value_type}")
        
        # Read key
        key = self._read_redis_string(f)
        print(f"Key: {key}")
        
        # Read value based on type
        if value_type == 0:  # String
            value = self._read_redis_string(f)
            print(f"Value (string): {value}")
        elif value_type == 1:  # List
            value = self._read_list(f)
            print(f"Value (list): {value}")
        else:
            print(f"Unsupported value type: {value_type}")
            return
        
        # Store in database
        if expires_at:
            current_ms = int(time.time() * 1000)
            if expires_at > current_ms:
                px = expires_at - current_ms
                self.database.set(key, value, px=px)
                print(f"Stored key '{key}' with expiration")
            else:
                print(f"Key '{key}' already expired, not storing")
        else:
            self.database.set(key, value)
            print(f"Stored key '{key}' without expiration")
    
    def _read_redis_string(self, f):
        """Read a Redis string which can be length-encoded or an integer"""
        first_byte_data = f.read(1)
        if not first_byte_data:
            raise ValueError("Unexpected end of file")
            
        first_byte = first_byte_data[0]
        encoding_type = (first_byte & 0xC0) >> 6
        
        if encoding_type == 3:  # 11: Special integer encoding
            special_type = first_byte & 0x3F
            if special_type == 0:  # 8-bit integer
                value = f.read(1)[0]
                return str(value)
            elif special_type == 1:  # 16-bit integer
                value = struct.unpack('<H', f.read(2))[0]
                return str(value)
            elif special_type == 2:  # 32-bit integer
                value = struct.unpack('<I', f.read(4))[0]
                return str(value)
            else:
                raise ValueError(f"Unknown special encoding: {special_type}")
        else:
            # Regular length-encoded string
            # Put the byte back and read length normally
            f.seek(f.tell() - 1)
            length = self._read_length(f)
            if length == 0:
                return ""
            data = f.read(length)
            return data.decode('utf-8')
    
    def _read_list(self, f):
        length = self._read_length(f)
        items = []
        for _ in range(length):
            items.append(self._read_redis_string(f))
        return items
    
    def _read_length(self, f):
        """Read just a length value (no special integer encoding)"""
        byte = f.read(1)
        if not byte:
            raise ValueError("Unexpected end of file")
        
        first_byte = byte[0]
        encoding_type = (first_byte & 0xC0) >> 6
        
        if encoding_type == 0:  # 00: next 6 bits represent length
            return first_byte & 0x3F
        elif encoding_type == 1:  # 01: next 14 bits represent length
            second_byte = f.read(1)[0]
            return ((first_byte & 0x3F) << 8) | second_byte
        elif encoding_type == 2:  # 10: next 32 bits represent length
            return struct.unpack('>I', f.read(4))[0]
        else:
            raise ValueError(f"Expected length encoding, got special encoding: {first_byte}")
