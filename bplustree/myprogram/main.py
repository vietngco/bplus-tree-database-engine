import json 
import datetime
import uuid
from bplustree.schema import Schema
from bplustree.column import IntCol, StrCol, BoolCol, FloatCol, DateTimeCol






# test the schema
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
    table_name="employee4", columns=columns, key_col="id", custom_index=[], order=6
)
# for i in range(100): 
#     employee.insert(
#         {
#             "id": i,
#             "name": "John Doe" + str(i),
#             "is_active": True,
#             "salary": 1000.0 + i,
#             "created_at": datetime.datetime.now(),
#             "note" : "test" + str(i)
#         }
#     )

for key, value in employee.tree.items(): 
    # print("record", key, value)
    pass 
employee_json = employee.get(1)
# print("employee name", employee_json['name'])
for key, value  in employee_json.items():
    # print(key, ":",value)
    pass 

# close the db 
# records  = employee.get_by_key(">", 3)
# print(records)
employee.tree.close()



# header: node_type + used_page_length + page_reference_byte 
# body : 