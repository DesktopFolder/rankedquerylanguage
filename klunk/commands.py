from typing import Callable
from .dataset import Dataset


class ExecutableExpression:
    def __init__(self, executor, *args, **kwargs):
        self.executor = executor
        self.args = args
        self.kwargs = kwargs

    def __call__(self, d: Dataset):
        return self.executor(d, *self.args, **self.kwargs)


class Executor:
    def __init__(self, func, greedy=True, print_dataset=True):
        self.func = func
        self.greedy = greedy
        self.print_dataset = print_dataset

        self.help = func.__doc__

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def prime(self, *args, **kwargs):
        return ExecutableExpression(self, *args, **kwargs)


class Counter:
    def __init__(self, l=None) -> None:
        from collections import Counter

        self.d = Counter()
        if l is not None:
            for x in l:
                self.add(x)

    def add(self, k):
        self.d[k] += 1

    def get(self):
        return list(self.d.items())


TESTING_ONLY = False

basic_commands = dict()
logger = None


def Command(f: Callable):
    global basic_commands
    realname = f.__name__[len("_command_") :]
    basic_commands[realname] = Executor(f) # add other stuff later lol
    return Executor(f)


def TestCommand(f: Callable):
    if TESTING_ONLY:
        return Command(f)
    return f


@TestCommand
def _command_wait(_, dura: str):
    # Debug only.
    import time

    time.sleep(float(dura))


@Command
def _command_testlog(_, msg: str):
    """
    `testlog` - Super duper for testing. Why can you even use this?
    """
    if logger is None:
        raise RuntimeError(f"Could not log {msg} as logger was None.")
    logger.log(msg)


@Command
def _command_count_uniques(d: Dataset, val: str | None = None):
    """
    `count_uniques` - Essentially, takes all the objects in the current list, and counts unique occurrences of them.
    Only works if the objects are hashable. If you have a use case to add that to something, let me know
    """
    if val is None:
        return Counter(d.l).get()
    raise RuntimeError("count_uniques does not support arguments yet.")


@Command
def _command_randomselect(d: Dataset, val: str):
    """
    `randomselect(n)` - Returns a random sample of `n` size of the current dataset.
    """
    import random

    return random.sample(d.l, k=int(val))


@Command
def _command_dropn(d: Dataset, val: str):
    """
    `dropn` - drop the first n values.
    """
    return d.l[int(val) :]


@Command
def _command_flatten(d: Dataset):
    """
    `flatten` - flattens lists of lists.
    """
    return [x for y in d.l for x in y]

@Command
def _command_enumerate(d: Dataset, base='1'):
    """
    `enumerate(base = 1)` - takes a list of items with `dynamic` capabilities,
    and assigns `i` to dynamic[default], where `i` is current position + base.
    You can do `extract dynamic` to get the data back.
    """
    for i, v in enumerate(d.l):
        v.dynamic = v.dynamic or dict()
        v.dynamic["default"] = i 
