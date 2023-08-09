#!/usr/bin/env python3

#### THIS FILE IS COPIED FROM pymcsr PROJECT. ONLY partition_list IS USED HERE I THINK. PROBABLY LOL.
class ParseError(ValueError):
    pass

def query_help(s):
    return {
        "function": "Function calls must have no spaces between the start of the function name, and"
        "the opening parenthesis. Example: player(HamazonAU), not player (HamazonAU)."
    }[s]

def peek(s: str):
    return s[0] if s else ''

def consume(s: str):
    return (s[0], s[1:])

def consume_until(s: str, val: str, discard_delim=False):
    a, b, c = s.partition(val)
    return (a, ("" if discard_delim else b) + c)

def consume_to_hard(s: str, val: str, miss: str|None=None):
    a, b, c = s.partition(val)
    if not b:
        err = (miss or f"'{val}'").format(a=a)
        raise ParseError(f"Could not find {err}")
    return a, c

def consume_str(s: str):
    s = s.lstrip()
    if not s:
        return (None, '')
    if s[0] == "'":
        return consume_until(s[1:], "'", True)
    return consume_until(s, " ", True)

def consume_word(s: str) -> tuple[str, str]:
    import re
    s = s.lstrip()
    res = re.split(r'([^\w])', s, 1)
    if len(res) == 1:
        return (res[0], '')
    return res[0], ''.join(res[1:])

def consume_function(s: str):
    c, s = consume_to_hard(s, '(', "opening parenthesis for function {a}")
    if ' ' in c:
        raise ParseError(f'Invalid function call: {c}. {query_help("function")}')
    v, s = consume_to_hard(s, ')', f"closing parenthesis for function {c}")

    return c.lower(), v, s.lstrip()

def parse_function_args_simple(s: str):
    return [x.strip().lower() for x in s.split(',')]

def partition_list(l, p):
    yes, no = list(), list()
    for v in l:
        if p(v):
            yes.append(v)
        else:
            no.append(v)
    return yes, no

def extract_aint_or(l: list[str], default: int):
    ints, nots = partition_list(l, lambda v: v.isdigit())
    if not ints:
        return default, nots
    return int(ints[0]), nots

if __name__ == "__main__":
    import sys
    s = sys.argv[1]
    print(f'Working with test string: {s}')
    c, s = consume(s)
    print(f'Consume one character: {c} ({s})')
    c, s = consume_str(s)
    print(f'Consume one string: {c} ({s})')
    c, s = consume_until(s, "q")
    print(f'Consume until q while not discarding delimiter: {c} ({s})')
    c, s = consume_word(s)
    print(f'Consume until non-word: {c} ({s})')
