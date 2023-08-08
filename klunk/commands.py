from typing import Callable

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
