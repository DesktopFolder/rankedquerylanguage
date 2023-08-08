from .language import *
from .runtime import Runtime
from .dataset import load_defaults

class Query(Component):
    def __init__(self, query: str, debug = False, timing = False):
        super().__init__("SandboxedQuery")
        self.debug = debug
        self._timing = timing
        self.query = query
        self.compiler = Compiler()
        self.tokenizer = Tokenizer()
        self.runtime = None

        self.tokens = None
        self.program = None
        self.parameters = None

        self.result = None

    def run(self):
        self.log(f"Running with query: {self.query}")
        self.time("Full query", always=True) # must ALWAYS do this, as we parse later

        # Do everything step by step. That way, if we throw, we have max info.
        self.tokens = self.tokenizer.tokenize(self.query)
        self.program, self.parameters = self.compiler.compile(self.tokens)

        self.handle_parameters(self.parameters)

        from os.path import isfile
        if isfile('location.txt'):
            self.log("Using location.txt to load matches.")
            loc = open('location.txt').read().strip()
        else:
            self.log("Loading sample matches.")
            loc = "klunk/samples/"

        # Now construct the runtime, for which we need to load samples, etc.
        datasets = load_defaults(loc, quiet = not self.debug)
        commands = dict()
        self.runtime = Runtime(datasets, commands)

        # Finally, get the result of program execution.
        self.result = self.runtime.execute(self.program, self.parameters)
        self._result = self.runtime._result

        self.log_time("Full query")

        return self.result
