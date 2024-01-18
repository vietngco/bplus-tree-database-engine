import datetime
class Column: 
    def __init__(self, name, type, length, default=None, nullable=True, unique=False):
        self.name = name
        self.type = type # int, string, boolean, float, datetime
        self.length = length # in bytes 
        self.default = default
        self.nullable = nullable
        self.unique = unique
        # todo: add constraints  

    def __repr__(self):
        return '<Column: name={} type={} length={}>'.format(
            self.name, self.type, self.length
        )
    def get_length(self):
        return self.length
    # def __eq__(self, other):

class IntCol(Column):  
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(name, "int", 8, default, nullable, unique) # byte length of int is 8

class StrCol(Column):
    def __init__(self, name, length, default=None, nullable=True, unique=False):
        # max byte: 255
        if length > 255:
            raise ValueError("Max length of string is 255")
        super().__init__(name, "string", length, default=default, nullable=nullable, unique=unique)

class BoolCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(name, "boolean", 1, default=default, nullable=nullable, unique=unique)

class FloatCol(Column):
    def __init__(self, name, default=None, nullable=True, unique=False):
        super().__init__(name, "float", 8, default, nullable, unique) # byte length of float is 8

class DateTimeCol(Column):
    def __init__(self, name, nullable=True, unique=False):
        super().__init__(name, "datetime", 19, datetime.datetime.now(), nullable, unique) 