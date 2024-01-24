import json 
import datetime
import uuid
from bplustree.schema import Schema
from bplustree.column import IntCol, StrCol, BoolCol, FloatCol, DateTimeCol

def get_cols() -> list:
    id_col = IntCol("id", unique=True, nullable=False) # 8 
    name_col = StrCol("name", 20) # 28 
    is_active_col = BoolCol("is_active", nullable=False ) # 29 
    salary_col = FloatCol("salary", nullable=False) # 37 
    created_at_col = DateTimeCol("created_at", nullable=False) # 45
    note = StrCol("note", 20, nullable=True, default="", unique=False) # 65 # total byte will 65 

    columns = [id_col, name_col, is_active_col, salary_col, created_at_col, note]
    return columns


columns = get_cols()
employee = Schema(
    table_name="employee2", columns=columns, key_col="id", custom_index=[], order=5
)
def insert_data():
    for i in range(10, 20): 
        employee.insert(
            {
                "id": i,
                "name": "John Doe" + str(i),
                "is_active": True,
                "salary": 1000.0 + i,
                "created_at": datetime.datetime.now(),
                "note" : "test" + str(i)
            }
        )
def primary_check():
    for key, value in employee._tree.items(): 
        # print("record", key, value)
        pass 
    print ("total lenght of the tree", len(employee._tree))
    employee_json = employee.get_record(1)
    return employee_json

def check_get_range(): 
    records  = employee.get_records(">", 3) # should return more than 10 reocords 
    return records

def print_record(record):
    print("id", record["id"])
    print("name", record["name"])
    print("is_active", record["is_active"])
    print("salary", record["salary"])
    print("created_at", record["created_at"])
    print("note", record["note"])
    print("=====================================")
# insert_data()
single_record = primary_check()
records = check_get_range()
print("len of recoreds", len(records))
for record in records:
    print_record(record)

# close 
employee.close()