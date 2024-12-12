from functools import total_ordering

def noop(*args, **kwargs):
    return None


class Logger:
    def __init__(self):
        self.log = ""

    def write(self, *args, **kwargs):
        self.log += " ".join(args)
        self.log += "\n"


def average(l):
    # rewrite this later
    if len(l) == 0:
        return -1  # lol
    return sum(l) / len(l)

def median(l):
    sz = len(l)
    if sz == 0:
        return -1
    if sz % 2 == 0:
        # even number. e.g. 4. we want [1, 2] in that case.
        half = sz // 2
        return average([l[half-1], l[half]])
    return l[sz // 2]
        
def short_time_fmt(x: int | float, is_s=False):
    if x == -1:
        return "Invalid Time"
    if isinstance(x, float):
        x = int(x)
    s = x // (1000 if not is_s else 1)
    m = s // 60
    h = m // 60
    s = s % 60
    return f"{m:02}:{s:02}"


def time_fmt(x: int | float, is_s=False):
    if x == -1:
        return "Invalid Time"
    if isinstance(x, float):
        x = int(x)
    s = x // (1000 if not is_s else 1)
    m = s // 60
    h = m // 60
    s = s % 60
    m = m % 60
    return f"{h}:{m:02}:{s:02}"


@total_ordering
class Percentage:
    def __init__(self, small: float|int|str = 0, large: float|int = 0):
        """
        Accept Percentage() so that type-testing works.
        Should never actually be used this way. I hope.

        Accept Percentage('n') so that comparison works. Kind of.
        """

        if isinstance(small, str):
            # This kind of works...
            self.pctg = float(small)
            self.num = self.pctg
            self.denom = 100.0
            return

        self.num = small
        self.denom = large
        if large != 0:
            self.pctg = round(100 * small / large, 2)
        else:
            self.pctg = None

    def __eq__(self, o) -> bool:
        if o is None:
            return self.pctg is None
        if not isinstance(o, Percentage):
            raise RuntimeError(f'Compared Percentage to {type(o)}?')
        return self.num == o.num and self.denom == o.denom

    def __lt__(self, o) -> bool:
        if self.pctg is None:
            if o.pctg is None:
                return False # Equal
            return True # Less than
        if o.pctg is None:
            return False # Greater than
        return self.pctg < o.pctg

    def __str__(self):
        if self.pctg is None:
            return "<No Data>"
        return f"{self.pctg}% ({self.num} / {self.denom})"

    def __repr__(self):
        return self.__str__()


def percentage_str(small, large) -> Percentage:
    #pctg = round(100 * small / large, 2)
    #return f"{pctg}% ({small} / {large})"
    return Percentage(small, large)


def test_guild():
    import discord

    return discord.Object(id=1133544816563716250)  # replace with your guild id
