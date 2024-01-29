import struct
import datetime
from bplustree import const


class Column:
    def __init__(self, name, type, length, default=None, nullable=True, unique=False):
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

    # def __eq__(self, other):


class IntCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(
            name, "int", 8, default, nullable, unique
        )  # byte length of int is 8

    def serialize(self, obj: int) -> bytes:
        return obj.to_bytes(8, const.ENDIAN)

    def deserialize(self, bytes: bytes) -> int:
        return int.from_bytes(bytes, const.ENDIAN)


class StrCol(Column):
    def __init__(self, name, length=255, default=None, nullable=True, unique=False):
        # max byte: 255
        self._max_length = 255
        if length > 255:
            raise ValueError("Max length of string is 255")
        super().__init__(
            name, "string", length, default=default, nullable=nullable, unique=unique
        )

    def serialize(self, obj: str) -> bytes:
        by = b""
        by += obj.encode("utf-8")
        by += b" " * (self.length - len(obj))
        return by

    def deserialize(self, bytes: bytes) -> str:
        return bytes.decode("utf-8").strip()


class BoolCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(
            name, "boolean", 1, default=default, nullable=nullable, unique=unique
        )

    def serialize(self, obj: bool) -> bytes:
        return int(obj).to_bytes(1, const.ENDIAN)

    def deserialize(self, bytes: bytes) -> bool:
        return bool(int.from_bytes(bytes, const.ENDIAN))


class FloatCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(
            name, "float", 8, default, nullable, unique
        )  # byte length of float is 8

    def serialize(self, obj: float) -> bytes:
        return struct.pack("d", obj)

    def deserialize(self, bytes: bytes) -> float:
        return struct.unpack("d", bytes)[0]


class DateTimeCol(Column):
    def __init__(self, name, nullable=True, unique=False):
        super().__init__(
            name, "datetime", 19, datetime.datetime.now(), nullable, unique
        )

    def serialize(self, obj: datetime) -> bytes:
        return obj.strftime("%Y-%m-%d %H:%M:%S").encode("utf-8")

    def deserialize(self, bytes: bytes) -> datetime:
        return datetime.datetime.strptime(bytes.decode("utf-8"), "%Y-%m-%d %H:%M:%S")
