from typing import Any


class Expression:
    def __init__(self, loc, command=None, arguments: list[Any] | None = None):
        self.command = command
        self.arguments = arguments or list()
        self.loc = loc

    def __bool__(self):
        return self.command is not None

    def __eq__(self, other) -> bool:
        return other.command == self.command and other.arguments == self.arguments

    def __repr__(self) -> str:
        loc = "" if self.loc is None else f",c:{self.loc}"
        return f"Expression<{self.command},{self.arguments}{loc}>"
