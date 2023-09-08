from typing import Callable
from .dataset import Dataset

class Counter:
    def __init__(self, l = None) -> None:
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
    realname = f.__name__[len("_command_"):]
    basic_commands[realname] = f
    return f

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
    if logger is None:
        raise RuntimeError(f'Could not log {msg} as logger was None.')
    logger.log(msg)

@Command
def _command_count_uniques(d: Dataset, val: str|None = None):
    if val is None:
        return Counter(d.l).get()
    raise RuntimeError('count_uniques does not support arguments yet.')

@Command
def _command_randomselect(d: Dataset, val: str):
    import random
    return random.sample(d.l, k=int(val))
