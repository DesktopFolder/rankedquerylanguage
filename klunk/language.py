#!/usr/bin/env python3
from typing import Any, Callable, Iterable, Tuple
from .component import Component
from .expression import Expression

""" klunk - Documentation

- Expressions must start with a pipe (|).
- Leading pipes are implied. (with prep_top_level_expression)
- Default index is implied.

Example query:

| index xyz | filter winner(desktopfolder) | filter completed | fastest n(10) | matchdisplay
expression(
| -> PIPE
index -> STRING (for now)
xyz -> STRING
winner -> FUNCTION(STRING)
, -> LIST
)
"""

class Token:
    def __init__(self, name, data, c=None):
        self.name = name
        self.data = data
        self.c = c

    def instance(self, data, c=None):
        return Token(self.name, data, c=c)

    def __getitem__(self, k):
        assert self.data is not None
        return self.data[k]

    def __eq__(self, other) -> bool:
        return other.name == self.name

    def __repr__(self) -> str:
        loc = "" if self.c is None else f", c:{self.c}"
        return f'Token<{self.name}, {self.data}{loc}>'

class BasicToken(Token):
    def __init__(self, name):
        super().__init__(name, None)

    def instance(self, c=None):
        return super().instance(None, c=c)

    def __eq__(self, other) -> bool:
        return other.name == self.name

    def __repr__(self) -> str:
        loc = "" if self.c is None else f", c:{self.c}"
        return f'Token<{self.name}{loc}>'

class Tokens:
    PIPE = BasicToken("|")
    FUNCTION = Token("FUNCTION", ())
    STRING = Token("STRING", ())
    LIST = Token("LIST", ())
    PARAMETERS = Token("PARAMETERS", list())

def STRING(s = "", c=None):
    return Tokens.STRING.instance(s, c)

def PIPE(c=None):
    return Tokens.PIPE.instance(c=c)

def FUNCTION(s = "", d="", c=None):
    return Tokens.FUNCTION.instance((s, d), c=c)

def LIST(c=None):
    return Tokens.LIST.instance(list(), c=c)

def prep_top_level_expression(expr: str):
    if not expr.startswith('|'):
        expr = '|' + expr

    return expr

class ParseError(ValueError):
    pass

class CompileError(ValueError):
    pass

# A bunch of random parse helpers.
def peek(s: str):
    return s[0] if s else ''

def split_before(c: str, p: Callable) -> Tuple[str, str]:
    i = 0
    for i, v in enumerate(c):
        if p(v):
            break
    else:
        # Nothing fulfills the predicate.
        # Return before = full string, after = empty
        return c, ''
    return c[:i], c[i:]

def flip(a, b):
    return b, a

def consume_string(s: str):
    return split_before(s, lambda c: c in "|() ")

def consume_quoted(s: str):
    res = str()
    initial = s
    c1 = s[0]
    s = s[1:]
    while s:
        a, b = split_before(s, lambda c: c == c1)
        if a and a[-1] == '\\' and b:
            # ok I could parse this properly but like
            # why
            # so we'll just do this and if it's an issue..? eh
            res += a[0:-1] + b[0]
            s = b[1:]
            continue
        else:
            res += a
            s = b
            break
    if not s:
        raise ParseError(f'Could not find end to string: {initial}')
    return res, s[1:]


# Technically this is a lexer, because it adds extra data.
class Tokenizer(Component):
    MAX_TOKENS = 1000

    def __init__(self):
        super().__init__("Tokenizer")

    def tokenize(self, s: str):
        # Expressions are lists. So, if type(tok) == list, it's an expression.
        tokens = list()

        initial_sz = len(s)

        if peek(s) == '+':
            # Consume debug arguments.
            args = s[1:].split('|', 1)
            if len(args) == 1:
                s = ''
            else:
                s = '|' + args[1]
            args = [x for x in args[0].split(' ') if x]
            self.handle_parameters(args)
            # I was going to add metadata here so that we could determine the position of the inserted |.
            # Then I realized there's a guaranteed | anyways (or nothing...). So whatever.
            tokens.append(Tokens.PARAMETERS.instance(args, 0))

        self.time("Tokenization")
        
        while True:
            if len(tokens) > Tokenizer.MAX_TOKENS:
                raise ParseError(f'Exceeded max tokens ({Tokenizer.MAX_TOKENS}) '
                                 f'The current s is: {s}. The last token is: {tokens[-1]}')

            # Consume whitespace.
            s = s.lstrip()

            # Consume empty strings.
            if not s:
                break

            loc = initial_sz - len(s)

            # Consume pipes.
            if s[0] == '|':
                s = s[1:]
                tokens.append(Tokens.PIPE.instance(loc))
                continue

            if s[0].isalpha():
                res, s = consume_string(s)
                if peek(s) == '(':
                    # It's a function.
                    # Functions allow recursion. So, we must recurse.
                    # Let's not bother for now.
                    # This obviously does not work but frankly it doesn't matter.
                    data, _, s = s[1:].partition(')')
                    tokens.append(Tokens.FUNCTION.instance((res, data), loc))
                else:
                    tokens.append(Tokens.STRING.instance(res, loc))
                continue

            # Temporary - support integers as strings.
            # Temporary for sure though :)))
            if s[0].isdigit():
                res, s = consume_string(s)
                tokens.append(Tokens.STRING.instance(res, loc))
                continue

            if s[0] in {"'", '"'}:
                res, s = consume_quoted(s)
                tokens.append(Tokens.STRING.instance(res, loc))
                continue

            raise ParseError(f"Unexpected character while tokenizing: '{s[0]}' (Query remainder: '{s}')")

        self.log_time('Tokenization')

        return tokens

class Compiler(Component):
    """
    Compiler takes the lexer output and compiles it into an easily callable pipeline.
    something something graphics programming
    """
    def __init__(self):
        super().__init__("Compiler") 

    def validate(self, expr: Expression):
        if not expr:
            # We could discard these, but I want to be strict earlier.
            raise CompileError(f"Expression starting at character {expr.loc} was empty.")

    def compile(self, tokens: list[Token], source=None) -> tuple[list[Expression], list[str]]:
        # In general, our compilation process is pretty simple.
        # The compiler is mostly intended for debugging.
        # And compiling things, but you know, that's obvious, right?
        # Obviously, this is an interpreted language, so this is analogous to javac.
        # But somehow worse :)
        
        # but better ofc i mean it's java so
        pipeline: list[Expression] = list()
        parameters: list[str] = list()

        if not tokens:
            raise CompileError("Empty token list.")

        if tokens[0] == Tokens.PARAMETERS:
            # Cool.
            tok = tokens.pop(0)
            parameters.extend(tok.data)
            self.handle_parameters(parameters)
            self.log(f"Added parameters: {tok.data}")

        self.time("Compilation")

        if not tokens:
            # This is technically legal, I guess.
            # For debugging purposes only.
            self.log("Skipped compilation as only parameters were provided.")
            return list(), parameters

        if source is not None:
            self.log(f"Began compiling tokens from: {source}")

        # Let's add a leading pipe.
        if tokens[0] != PIPE():
            tokens.insert(0, PIPE(c=0))
            self.log(f"Inserted a pipe for expression.")

        while tokens:
            tok = tokens.pop(0)

            # If it's a pipe, start a new expression.
            if tok == PIPE():
                self.llog(tok.c, "Found new expression.")
                if pipeline:
                    # validate the PREVIOUS expression
                    self.validate(pipeline[-1])
                pipeline.append(Expression(tok.c))
                continue

            # Otherwise, we want that last expression.
            expr = pipeline[-1]

            if tok == STRING():
                if expr:
                    expr.arguments.append(tok.data)
                    self.llog(tok.c, f'Added string argument to {expr.command}: {tok.data}')
                else:
                    expr.command = tok.data
                    self.llog(tok.c, f'Started new expression with command {tok.data}')
                continue

            if tok == FUNCTION():
                if not expr:
                    # TODO - is this actually super dumb?
                    # e.g. | fastest(10) makes a lot of sense.
                    # but forcing nice naming (| fastest n(10)) doesn't seem terrible, idk. whatever.
                    raise CompileError(f'Found leading function in expression ({tok.data}). Expressions must start with a command, which does not have parentheses (e.g. "| filter winner(DesktopFolder)")')
                # TODO - must recurse here. for now we will not... lol.
                # This is part of where we're really just passing things along for the time being.
                # I will rework this later. But it's mostly drop in fixes/extra code.
                # I don't think this is shooting myself in the foot, now that the overall approach is better.
                # The function execution code will just be really simple for the time being.
                expr.arguments.append(tok.data)
                self.llog(tok.c, f'Added function: {tok.data}')
                continue

            raise CompileError(f"Unsupported token: {tok}")

        self.log_time("Compilation")
        return pipeline, parameters
