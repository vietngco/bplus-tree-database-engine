import pytest
from datetime import datetime

from .conftest import schema_name
from bplustree.schema import Schema
from bplustree.column import IntCol, StrCol, BoolCol, FloatCol, DateTimeCol


def get_cols() -> list:
    id_col = IntCol("id", unique=True, nullable=False)  # 8
    name_col = StrCol("name", 20)  # 28
    is_active_col = BoolCol("is_active", nullable=False)  # 29
    salary_col = FloatCol("salary", nullable=False)  # 37
    created_at_col = DateTimeCol("created_at", nullable=False)  # 45
    note = StrCol(
        "note", 20, nullable=True, default="", unique=False
    )  # 65 # total byte will 65

    columns = [id_col, name_col, is_active_col, salary_col, created_at_col, note]
    return columns
def get_data_to_insert(a,b): 
    data = []
    if a >= b: 
        raise ValueError("a should be less than b")
    for i in range(a, b):
        data.append(
            {
                "id": i,
                "name": "John Doe" + str(i),
                "is_active": True,
                "salary": 1000.0 + i,
                "created_at": datetime.now(),
                "note": "test" + str(i),
            }
        )
    return data

@pytest.fixture
def s():
    s = Schema(
        table_name=schema_name,
        columns=get_cols(),
        key_col="id",
        custom_index=[],
        order=5,
    )
    yield s
    s.close()


# @mock.patch('bplustree.tree.BPlusTree.close')
# def test_closing_context_manager(mock_close):
#     with BPlusTree(filename, page_size=512, value_size=128) as b:
#         pass
#     mock_close.assert_called_once_with()
def test_insert_data(s):
    for i in range(10, 20):
        s.insert(
            {
                "id": i,
                "name": "John Doe" + str(i),
                "is_active": True,
                "salary": 1000.0 + i,
                "created_at": datetime.now(),
                "note": "test" + str(i),
            }
        )
    assert len(s._tree) == 10


def test_get_data(s):
    s.batch_insert(get_data_to_insert(10, 20))
    assert len(s._tree) == 10
    record = s.get_record(10)
    assert record["id"] == 10
    assert record["name"] == "John Doe10"
    assert record["is_active"] == True
    assert record["salary"] == 1010.0
    assert record["note"] == "test10"


def test_get_range(s):
    s.batch_insert(get_data_to_insert(0, 10))
    assert len(s._tree) == 10
    records = s.get_records(">", 3)
    assert len(records) == 6
    for record in records:
        assert record["id"] > 3

def test_get_range_prev_page(s):
    s.batch_insert(get_data_to_insert(0, 100))
    assert len(s._tree) == 100
    records = s.get_records("<", 3)
    assert len(records) == 3
    for record in records:
        assert record["id"] < 3