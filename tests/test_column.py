from datetime import datetime

from bplustree.column import CompositeKey, IntCol, StrCol, FloatCol, DateTimeCol
from bplustree import const


def test_int_col():
    col = IntCol("id", unique=True, nullable=False)
    assert col.name == "id"
    assert col.type == const.INT_TYPE
    assert col.length == 8
    assert col.unique
    assert not col.nullable
    assert repr(col) == "<Column: name=id type=1 length=8>"


def test_int_col_serialize():
    col = IntCol("id", unique=True, nullable=False)
    assert col.serialize(42) == b"*\x00\x00\x00\x00\x00\x00\x00"


def test_int_col_deserialize():
    col = IntCol("id", unique=True, nullable=False)
    assert col.deserialize(b"*\x00\x00\x00\x00\x00\x00\x00") == 42


def test_str_col():
    col = StrCol("name", length=20)
    assert col.name == "name"
    assert col.type == const.STR_TYPE
    assert col.length == 20
    assert not col.unique
    assert col.nullable
    assert repr(col) == "<Column: name=name type=2 length=20>"


def test_str_col_serialize():
    col = StrCol("name", length=3)
    assert col.serialize("foo") == b"foo"
    col = StrCol("name", length=4)
    assert col.serialize("foo") == b"foo\x00"


def test_str_col_deserialize():
    col = StrCol("name", length=3)
    assert col.deserialize(b"foo") == "foo"
    col = StrCol("name", length=4)
    assert col.deserialize(b"foo\x00") == "foo"


def test_float_col():
    col = FloatCol("salary")
    assert col.name == "salary"
    assert col.type == const.FLOAT_TYPE
    assert col.length == 8
    assert repr(col) == "<Column: name=salary type=3 length=8>"


def test_float_col_serialize():
    col = FloatCol("salary")
    assert col.serialize(42.0) == b"\x00\x00\x00\x00\x00\x00E@"


def test_float_col_deserialize():
    col = FloatCol("salary")
    assert col.deserialize(b"\x00\x00\x00\x00\x00\x00E@") == 42.0


def test_datetime_col():
    col = DateTimeCol("created_at")
    assert col.name == "created_at"
    assert col.type == const.DATETIME_TYPE
    assert col.length == 19
    assert repr(col) == "<Column: name=created_at type=4 length=19>"


def test_datetime_col_serialize():
    col = DateTimeCol("created_at")
    dt = datetime(2018, 1, 6, 21, 42, 2)
    assert col.serialize(dt) == b"2018-01-06 21:42:02"


def test_datetime_col_deserialize():
    col = DateTimeCol("created_at")
    assert col.deserialize(b"2018-01-06 21:42:02") == datetime(2018, 1, 6, 21, 42, 2)


def test_compkey():
    int_col = IntCol("id", unique=True, nullable=False)
    str_col = StrCol("name", length=20)
    ck = CompositeKey(columns=[int_col, str_col], values=[42, "foo"])
    ck2 = CompositeKey(columns=[int_col, str_col], values=[43, "foo"])
    # test smaller
    assert ck < ck2
    assert ck <= ck2

    # test equal
    assert ck == ck
    assert ck != ck2

    # test greater
    ck = CompositeKey(columns=[int_col, str_col], values=[43, "foo"])
    ck2 = CompositeKey(columns=[int_col, str_col], values=[43, "foox"])
    # test smaller
    assert ck2 > ck
    assert ck2 >= ck
