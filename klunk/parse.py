# IDK why parse_utils isn't in this directory, but this is now, lol.
def parse_boolean(b: str|bool) -> bool:
    if type(b) == bool:
        return b
    if 'true'.startswith(b):
        return True
    if 'false'.startswith(b):
        return False
    raise ValueError(f'{b} is not convertible to a boolean.')
