import datetime
import struct

from bplustree import BPlusTree
from bplustree import IntSerializer


class Schema:
    # custom index is a list of tuple (column_name, index_type)
    # index_type is always btree
    # the key column will be use for indexing in btree
    # index col may not unique
    def __init__(
        self, table_name: str, columns: list, key_col: str, custom_index, order
    ):
        if len(columns) == 0:
            raise ValueError("Schema must have at least one column")

        if key_col is None:  # by default, the first column is key
            key_col = columns[0].name
        self.key_col = key_col
        self.table_name = table_name
        self.columns = columns
        self.col_dict = {col.name: col for col in columns}
        self.record_length = sum([col.get_length() for col in columns])
        key_size = self.col_dict[key_col].get_length()
        self._tree = BPlusTree(
            "/tmp/" + table_name + ".db",
            key_size=key_size,
            value_size=self.record_length,
            order=order,
            serializer=IntSerializer(),
        )

    def __repr__(self):
        return "<Schema: table_name={} columns={}>".format(
            self.table_name, self.columns
        )

    def _get_not_null_columns(self):
        return [col for col in self.columns if not col.nullable]

    def _assign_default_value(self, data: dict):
        for col in self.columns:
            if col.name not in data or data[col.name] is None:
                data[col.name] = col.default

    def _validate_not_null_cols(self, data: dict):
        not_null_cols = self._get_not_null_columns()
        for col in not_null_cols:
            if col.name not in data or data[col.name] is None:
                raise ValueError("Column {} is not nullable".format(col.name))

    def _validate_data_is_valid(self, data: dict):
        # check type
        for col in self.columns:
            if col.name not in data:
                raise ValueError("Data is not complete")
            if col.type == "int":
                if not isinstance(data[col.name], int):
                    raise ValueError("Data type is not correct")
            elif col.type == "string":
                if not isinstance(data[col.name], str):
                    raise ValueError("Data type is not correct")
                if len(data[col.name]) > col.length:
                    raise ValueError("Data length is not correct")
            elif col.type == "boolean":
                if not isinstance(data[col.name], bool):
                    raise ValueError("Data type is not correct")
            elif col.type == "float":
                if not isinstance(data[col.name], float):
                    raise ValueError("Data type is not correct")
            elif col.type == "datetime":
                if not isinstance(data[col.name], datetime.datetime):
                    raise ValueError("Data type is not correct")
            else:
                raise ValueError("Data type is not correct")
    def serilize_record(self, data: dict) -> bytes:
        # data = {"id": 1, "name": "John", "is_active": True, "salary": 1000.0, "created_at": datetime.datetime.now()}
        # use column length to serilize the data
        # return b'1JohnTrue1000.0'
        bytes = b''
        for col in self.columns: 
            length = col.get_length() 
            if col.type == "int":
                bytes += data[col.name].to_bytes(length, "big")
            elif col.type == "string":
                bytes += data[col.name].encode("utf-8")
                bytes += b' ' * (length - len(data[col.name]))
            elif col.type == "boolean":
                bytes += int(data[col.name]).to_bytes(length, "big")
            elif col.type == "float":
                  bytes += struct.pack('d', data[col.name])
            elif col.type == "datetime":
                bytes += data[col.name].strftime("%Y-%m-%d %H:%M:%S").encode("utf-8")
            else:
                raise ValueError("Data type is not correct")
        return bytes
    def deserialize_record(self, bytes: bytes) -> dict:
        # byte value b'1JohnTrue1000.0'
        # reurn data = {"id": 1, "name": "John", "is_active": True, "salary": 1000.0, "created_at": datetime.datetime.now()}
        data = {}
        start = 0
        for col in self.columns:
            length = col.get_length()
            if col.type == "int":
                data[col.name] = int.from_bytes(bytes[start:start+length], "big")
            elif col.type == "string":
                data[col.name] = bytes[start:start+length].decode("utf-8").strip()
            elif col.type == "boolean":
                data[col.name] = bool.from_bytes(bytes[start:start+length], "big")
            elif col.type == "float":
                data[col.name] = struct.unpack('d', bytes[start:start+length])[0]
            elif col.type == "datetime":
                data[col.name] = datetime.datetime.strptime(bytes[start:start+length].decode("utf-8"), "%Y-%m-%d %H:%M:%S")
            else:
                raise ValueError("Data type is not correct")
            start += length
        return data
    
    def insert(self, data: dict):
        # data = {"id": 1, "name": "John", "is_active": True, "salary": 1000.0, "created_at": datetime.datetime.now()}
        # validating 
        self._assign_default_value(data)
        self._validate_not_null_cols(data)
        self._validate_data_is_valid(data)
        # serlaize the data
        byte_record = self.serilize_record(data)
        if len(byte_record) != self.record_length and len(byte_record)!= self._tree._tree_conf.value_size :
            raise ValueError("Something wrong with the serlization, Data length is not correct")
        # get the key index 
        key_value = data[self.key_col]
        if (key_value is None):
            raise ValueError("Key value is None")
        if not isinstance(key_value, int):
            raise ValueError("not supported yet for other key type which is not int")
        # insert into the tree
        self._tree.insert(key_value, byte_record)
        

    def get_record(self, key) -> dict:
        # find the key that match, may hvae scan the whole tree if hte column is not indexed
        
        record_bytes = self._tree.get_record(key)
        if record_bytes is None:
            return None
        record = self.deserialize_record(record_bytes)
        return record
        
    # any get function should be block with read access for entire during of transaction
    def get_records(self, operator, value) -> list: # target column, operator, value
        key_col = self.col_dict[self.key_col]  
        records = []
        if operator == "=":
            # just return one value 
            record = self._tree.get_record(value) # get single record 
            records.append(record)
        else: 
            records = self._tree.get_records(operator, value)
            
        for  i, record  in enumerate( records): 
            records[i] = self.deserialize_record(record)
        return records

    def update(self, key, data): # will be set of column and value that need to be updated but not hte index key 
        pass

    def delete(self, key):
        pass
    def close(self): 
        self._tree.close()


# for example: Schema("employee", [IntCol("id"), StrCol("name", 20), BoolCol("is_active"), FloatCol("salary"), DateTimeCol("created_at")])
