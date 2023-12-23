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


def percentage_str(small, large):
    pctg = round(100 * small / large, 2)
    return f"{pctg}% ({small} / {large})"


def test_guild():
    import discord

    return discord.Object(id=1133544816563716250)  # replace with your guild id
