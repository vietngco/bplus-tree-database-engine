import struct
import datetime
from typing import Any
from bplustree import const


class Column:
    def __init__(
        self,
        name: str,
        type: int,
        length: int,
        default=None,
        nullable=True,
        unique=False,
    ):
        self.name = name
        self.type = type  # int, string, boolean, float, datetime
        self.length = length  # in bytes
        self.default = default
        self.nullable = nullable
        self.unique = unique
        # todo: add constraints

    def __repr__(self):
        return "<Column: name={} type={} length={}>".format(
            self.name, self.type, self.length
        )

    def get_length(self):
        return self.length

    def serialize(self, obj: Any) -> bytes:
        return NotImplemented

    def deserialize(self, bytes: bytes) -> Any:
        return NotImplemented

    def validate(self, value: Any) -> None:
        return NotImplemented


class IntCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(
            name, const.INT_TYPE, 8, default, nullable, unique
        )  # byte length of int is 8

    def serialize(self, obj: int) -> bytes:
        return obj.to_bytes(8, const.ENDIAN)

    def deserialize(self, bytes: bytes) -> int:
        return int.from_bytes(bytes, const.ENDIAN)

    def validate(self, value: Any) -> None:
        if not isinstance(value, int):
            raise ValueError("Value must be an integer")


class StrCol(Column):
    def __init__(self, name, length=255, default=None, nullable=True, unique=False):
        # max byte: 255
        self._max_length = 255
        if length > 255:
            raise ValueError("Max length of string is 255")
        super().__init__(
            name,
            const.STR_TYPE,
            length,
            default=default,
            nullable=nullable,
            unique=unique,
        )

    def serialize(self, obj: str) -> bytes:
        data = b""
        data += obj.encode("utf-8")
        assert self.length - len(data) >= 0
        data += b"\x00" * (self.length - len(data))  # Pad with null bytes
        return data

    def deserialize(self, bytes: bytes) -> str:
        string = bytes.decode("utf-8")
        return string.strip("\x00")

    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ValueError("Value must be a string")


class BoolCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(
            name, const.BOOL_TYPE, 1, default=default, nullable=nullable, unique=unique
        )

    def serialize(self, obj: bool) -> bytes:
        return int(obj).to_bytes(1, const.ENDIAN)

    def deserialize(self, bytes: bytes) -> bool:
        return bool(int.from_bytes(bytes, const.ENDIAN))

    def validate(self, value: Any) -> None:
        if not isinstance(value, bool):
            raise ValueError("Value must be a boolean")


class FloatCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(
            name, const.FLOAT_TYPE, 8, default, nullable, unique
        )  # byte length of float is 8

    def serialize(self, obj: float) -> bytes:
        return struct.pack("d", obj)

    def deserialize(self, bytes: bytes) -> float:
        return struct.unpack("d", bytes)[0]

    def validate(self, value: Any) -> None:
        if not isinstance(value, float):
            raise ValueError("Value must be a float")


class DateTimeCol(Column):
    def __init__(self, name, nullable=True, unique=False):
        super().__init__(
            name, const.DATETIME_TYPE, 19, datetime.datetime.now(), nullable, unique
        )

    def serialize(self, obj: datetime) -> bytes:
        return obj.strftime("%Y-%m-%d %H:%M:%S").encode("utf-8")

    def deserialize(self, bytes: bytes) -> datetime:
        return datetime.datetime.strptime(bytes.decode("utf-8"), "%Y-%m-%d %H:%M:%S")

    def validate(self, value: Any) -> None:
        if not isinstance(value, datetime.datetime):
            raise ValueError("Value must be a datetime")


class CompositeKey:
    def __init__(self, columns: list[Column], values: list = None):
        """The columns and values in both list should be in the same order"""
        key_length = sum([c.length for c in columns])

        self.length = key_length
        self.columns = columns

        if not values:
            self.values = [None for _ in columns]
            assert len(self.values) == len(self.columns)
        else:
            self.values = values
            assert len(self.values) == len(self.columns)
            for i, col  in enumerate(self.columns): 
                col.validate(self.values[i])

        

    def __eq__(self, __obj: object) -> bool:
        if not isinstance(__obj, CompositeKey):
            return NotImplemented
        for i, value in enumerate(self.values):
            if value != __obj.values[i]:
                return False
        return True

    def __ne__(self, __obj: object) -> bool:
        return self.__eq__(__obj) == False

    def __lt__(self, __obj: object) -> bool:
        if not isinstance(__obj, CompositeKey):
            return NotImplemented
        for i, value in enumerate(self.values):
            if value < __obj.values[i]:
                return True
            elif value > __obj.values[i]:
                return False

    def __le__(self, __obj: object) -> bool:
        equal = self.__eq__(__obj)
        if equal:
            return True
        less = self.__lt__(__obj)
        if less:
            return True
        return False

    def __gt__(self, __obj: object) -> bool:
        if not isinstance(__obj, CompositeKey):
            return NotImplemented
        for i, value in enumerate(self.values):
            if value > __obj.values[i]:
                return True
            elif value < __obj.values[i]:
                return False

    def __ge__(self, __obj: object) -> bool:
        equal = self.__eq__(__obj)
        if equal:
            return True
        greater = self.__gt__(__obj)
        if greater:
            return True
        return False

    def __repr__(self):
        return f"CompositeKey({', '.join(repr(col) for col in self.columns)})"

    def serialize(self) -> bytes:  # aka dump func
        data = b""
        for i, value in enumerate(self.values):
            data += self.columns[i].serialize(value)
        return data

    def deserialize(self, bytes: bytes) -> "CompositeKey":  # aka load func
        assert len(self.values) == len(self.columns)
        start = 0
        for i, col in enumerate(self.columns):
            end = start + col.length
            self.values[i] = col.deserialize(bytes[start:end])
            start = end

        # return the key itself
        return self

    def __hash__(self) -> int:
        return hash(tuple(self.values))
