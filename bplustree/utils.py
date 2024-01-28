import itertools
from typing import Iterable


def pairwise(iterable: Iterable):
    """Iterate over elements two by two.

    s -> (s0,s1), (s1,s2), (s2, s3), ...
    """
    a, b = itertools.tee(iterable)
    next(b, None) # always move one step ahead of a
    return zip(a, b)


def iter_slice(iterable: bytes, n: int):
    """Yield slices of size n and says if each slice is the last one.

    s -> (b'123', False), (b'45', True)
    """
    start = 0
    stop = start + n
    final_offset = len(iterable)

    while True:
        if start >= final_offset:
            break

        rv = iterable[start:stop]
        start = stop
        stop = start + n
        yield rv, start >= final_offset


def get_ops( key, input_key, op) -> bool:
        if op == "<": 
            return key < input_key
        elif op == ">": 
            return key > input_key
        elif op == "<=":
            return key <= input_key
        elif op == ">=":
            return key >= input_key 
        else:
            raise ValueError("Not supported operator =")
        
def check_ops(op):
    if op == "=":
        return True
    elif op == "<": 
        return True
    elif op == ">": 
        return True
    elif op == "<=":
        return True
    elif op == ">=":
        return True 
    else:
        raise ValueError("Not supported operator " + op)