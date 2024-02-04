import json
import datetime
import uuid
from bplustree.schema import Schema
from bplustree.column import IntCol, StrCol, BoolCol, FloatCol, DateTimeCol


def get_cols() -> list:
    id_col = IntCol("id", unique=True, nullable=False)  # 8
    name_col = StrCol("name", 20)  # 28
    is_active_col = BoolCol("is_active", nullable=False)  # 29
    salary_col = FloatCol("salary", nullable=False)  # 37
    created_at_col = DateTimeCol("created_at", nullable=False)  # 45
    department_col = StrCol("department", 20, nullable=True, default="", unique=False)
    note = StrCol(
        "note", 20, nullable=True, default="", unique=False
    )  # 65 # total byte will 65

    columns = [id_col, name_col, department_col, is_active_col, salary_col, created_at_col, note]
    return columns

columns = get_cols()
employee = Schema(
    table_name="employee6", columns=columns, custom_index=[ "department", "name", "id"], order=5
)


def insert_data():
    departments = ["HR", "IT", "Finance", "Marketing", "Sales"]
    for i in range(0, 40):
        department_val = departments[i % 5]
        employee.insert(
            {
                "name": "John Doe" + str(i),
                "department": department_val,  # "HR", "IT", "Finance", "Marketing", "Sales
                "id": i,
                "is_active": True,
                "salary": 1000.0 + i,
                "created_at": datetime.datetime.now(),
                "note": "test" + str(i),
            }
        )


def primary_check():
    for key, value in employee._tree.items():
        print("record", key, value)
        # print class of key 
        # print("type of key", type(key))
        pass
    print("total lenght of the tree", len(employee._tree))
    comp_key = employee.create_comp_key({
        "department": "HR",
        "name": "John Doe0",
        "id": 0
    })
    employee_json = employee.get_record(comp_key)
    print( employee_json)


def check_small_larger():

    records = employee.get_records(">", 13)  # should return more than 10 reocords
    return records


def check_range():
    records = employee.get_records_range(3, ">", 13, "<=")
    return records


def print_record(record):
    print("id", record["id"])
    print("name", record["name"])
    print("is_active", record["is_active"])
    print("salary", record["salary"])
    print("created_at", record["created_at"])
    print("department", record["department"])
    print("note", record["note"])
    print("=====================================")


# insert_data()
single_record = primary_check()
# records1 = check_small_larger()
# print("check_small_larger: len of records ", len(records1))

# records2 = check_range()
# print("check_range: len of records ", len(records2))


# close
employee.close()
# query 1:
# [{
#   "col": "id",
#   "operator": ">",
#   "value": 2
# },
# {
#   "col": "id",
#   "operator": ">",
#   "value": 2
# }
# {
#   "col": "id",
#   "operator": "between",
#   "value1": 2,
#   "value2": 4
# }
# ]
# another form of query: col: operation  col: operation
# such as id:>2 name:=2
# 1 < id < 3 or 5 < id < 10
# for simplify we just support the AND statement for now
