import datetime
import struct

from bplustree import BPlusTree
from bplustree import IntSerializer, StrSerializer
from . import utils
from bplustree import const


class Schema:
    """custom index is a list of col_name, if custom index is specified, then it will override
    the key in tree and define the key as string"""

    def __init__(
        self,
        table_name: str,
        columns: list,
        key_col: str,
        custom_index: list,
        order: int,
    ):
        if len(columns) == 0:
            raise ValueError("Schema must have at least one column")

        if key_col is None:
            raise ValueError("Key column can not be None")
        if custom_index is None:
            custom_index = []

        self.key_col = key_col
        self.table_name = table_name
        self.columns = columns
        self.col_dict = {col.name: col for col in columns}
        self.record_length = sum([col.get_length() for col in columns])

        self.key_serlizer = IntSerializer()

        if len(custom_index) > 0:
            self.key_serlizer = StrSerializer()
            self.key_size = sum(
                [self.col_dict[col_name].get_length() for col_name in custom_index]
            )
        else:
            key_type = self.col_dict[key_col].type
            self.key_size = self.col_dict[key_col].get_length()

            if key_type == "int":
                self.key_serlizer = IntSerializer()
            elif key_type == "string":
                self.key_serlizer = StrSerializer()
            else:
                raise ValueError(f"This Key type {key_type} is not supported yet")

        self._tree = BPlusTree(
            "/tmp/" + table_name + ".db",
            key_size=self.key_size,
            value_size=self.record_length,
            order=order,
            serializer=self.key_serlizer,
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
        bytes = b""
        for col in self.columns:
            bytes += col.serialize(data[col.name])
        return bytes

    def deserialize_record(self, bytes: bytes) -> dict:
        data = {}
        start = 0
        for col in self.columns:
            length = col.get_length()
            data[col.name] = col.deserialize(bytes[start : start + length])
            start += length
        return data

    def _serlize_validate_data(self, data: dict):
        self._assign_default_value(data)
        self._validate_not_null_cols(data)
        self._validate_data_is_valid(data)

        byte_record = self.serilize_record(data)
        if (
            len(byte_record) != self.record_length
            and len(byte_record) != self._tree._tree_conf.value_size
        ):
            raise ValueError(
                "Something wrong with the serlization, Data length is not correct"
            )
        # get the key index
        key_value = data[self.key_col]
        if key_value is None:
            raise ValueError("Key value is None")
        if not isinstance(key_value, int):
            raise ValueError("not supported yet for other key type which is not int")
        return key_value, byte_record

    def batch_insert(self, data_list: list[dict]):
        """Insert many records at once"""
        for i, data in enumerate(data_list):
            key_value, byte_record = self._serlize_validate_data(data)
            data_list[i] = (key_value, byte_record)
        self._tree.batch_insert(data_list)

    def insert(self, data: dict):
        key_value, byte_record = self._serlize_validate_data(data)
        self._tree.insert(self._trans_val(key_value), byte_record)

    def _trans_val(self, key):
        if isinstance(self.key_serlizer, StrSerializer):
            key = str(key)
        elif isinstance(self.key_serlizer, IntSerializer):
            key = int(key)
        else:
            raise ValueError(
                "not supported yet for other key type which is not int or string"
            )
        return key

    def get_record(self, key) -> dict:
        """Find the key that match, may hvae scan the whole tree if hte column is not indexed"""
        key = self._trans_val(key)
        record_bytes = self._tree.get_record(key)
        if record_bytes is None:
            return None
        record = self.deserialize_record(record_bytes)
        return record

    # any get function should be block with read access for entire during of transaction
    def get_records(self, operator: str, value) -> list:
        value = self._trans_val(value)

        utils.check_ops(operator)

        records = []
        if operator == "=":
            # just return one value
            record = self._tree.get_record(value)  # get single record
            records.append(record)
        else:
            records = self._tree.get_records(operator, value)

        for i, record in enumerate(records):
            records[i] = self.deserialize_record(record)
        return records

    def get_records_range(self, value1, op1, value2, op2) -> list:
        value1 = self._trans_val(value1)
        value2 = self._trans_val(value2)
        if value1 is None or value2 is None:
            raise ValueError("value1 and value2 can not be None")
        if value1 > value2:
            raise ValueError(
                "value1 should be less than value2: " + str(value1) + " " + str(value2)
            )
        if op1 not in [">", ">=", "="]:
            raise ValueError("op1 is not supported " + op1)
        if op2 not in ["<", "<=", "="]:
            raise ValueError("op2 is not supported " + op2)

        records = self._tree.get_records_range(value1, op1, value2, op2)
        for i, record in enumerate(records):
            records[i] = self.deserialize_record(record)
        return records

    def update(
        self, key, data
    ):  # will be set of column and value that need to be updated but not hte index key
        pass

    def delete(self, key):
        pass

    def close(self):
        self._tree.close()


# for example: Schema("employee", [IntCol("id"), StrCol("name", 20), BoolCol("is_active"), FloatCol("salary"), DateTimeCol("created_at")])
# TODO: CUSTOM INDEX
