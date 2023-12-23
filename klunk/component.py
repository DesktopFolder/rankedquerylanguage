class Component:
    def __init__(self, name: str):
        self._log = list()
        self.debug = False
        self.name = name
        self.is_test = False

        self._timing = False
        self._time: dict[str, float] = {}

        self._tracebacks = False

        self._do_upload = False

        self._result = None

    def handle_parameters(self, parameters):
        for param in parameters:
            if param == "debug":
                self.debug = True
                self.log(f"{self.name} entered debug mode.")
            # This basically allows us to have tests for printing things.
            elif param == "test":
                self.is_test = True
                self.log(f"{self.name} entered testing mode.")
            # Ah, timing.
            elif param == "timing":
                self._timing = True
                self.log(f"{self.name} entered timing mode.")
            # Tracebacks! Is that like a tattoo?
            elif param in ["tb", "tracebacks"]:
                self._tracebacks = True
                self.log(f"{self.name} enabled tracebacks.")
            # Files! Discord thing.
            elif param in ["asfile"]:
                self._do_upload = True
                self.log(f"{self.name} enabled file uploads.")

    def time(self, s: str, always=False):
        if self._timing or always:
            from time import time

            self._time[s] = time()

    def time_diff(self, s: str):
        if not self._timing:
            return None
        from time import time

        end = time()
        v = self._time.get(s)
        if v is None:
            raise RuntimeError(f"Did not have time for {s}")
        del self._time[s]
        return round((end - v) * 1000, 3)

    def log_time(self, s: str):
        if self._timing:
            td = self.time_diff(s)

            # Assertions are only for this kind of situation
            assert td is not None

            m = "m"
            if td > 1000:
                td = round(td / 1000, 2)
                m = ""
            self.log(f"{s} took {td}{m}s")

    def log(self, *args):
        self._log.append(" ".join([str(x) for x in args]))
        if self.debug and not self.is_test:
            print(f"{self.name}:", *args)

    def llog(self, c, *args):
        self.log(f"[c:{c}]", *args)

    def alwayslog(self, *args):
        v = self.debug
        self.debug = True
        self.log(*args)
        self.debug = v

    def add_result(self, *args):
        if self._result is None:
            self.log(f"Began recording a result.")
            self._result = list()
        self._result.append(" ".join([str(x) for x in args]))
        self.log("Added to result:", *args)

    def dump_log(self):
        return "\n".join(self._log)
