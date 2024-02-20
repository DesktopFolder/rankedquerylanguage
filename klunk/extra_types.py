class Milliseconds(int):
    pass


class Seconds(int):
    pass


class UUID(str):
    pass


def is_numeric(T):
    return issubclass(T, int) or issubclass(T, float)
