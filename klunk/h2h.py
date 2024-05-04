from .dataset import Dataset
from .match import QueryMatch

class H2H:
    def __init__(self, m1, m2):
        self.m1 = m1
        self.m2 = m2
        self.w1 = 0
        self.w2 = 0
        self.draw = 0

    def inc(self, k: str | None):
        if k == self.m1:
            self.w1 += 1
        elif k == self.m2:
            self.w2 += 1
        elif k is None:
            self.draw += 1
        else:
            raise KeyError(f'{self.m1}, {self.m2}, {k}')

    def get(self, k: str):
        if k == self.m1:
            return self.w1
        elif k == self.m2:
            return self.w2
        else:
            raise KeyError(f'{self.m1}, {self.m2}, {k}')

def generate(d: Dataset, pl: list[str], nickmap: dict[str, str]):
    uuids = set(pl)
    
    e = d.example()
    if e is None:
        raise RuntimeError("Dataset was empty?")
    if type(e) != QueryMatch:
        raise RuntimeError(f"h2h must be given a list of matches (got: {type(e)}).")

    def h2hmatch(m: QueryMatch):
        if m.is_decay or m.type != 2:
            return False
        assert len(m.members) == 2
        for mem in m.members:
            if mem.uuid not in uuids:
                return False
        return True

    nu = [m for m in d.l if h2hmatch(m)]

    res: dict[tuple[str, str], H2H] = dict()

    for m in nu:
        k = tuple(sorted([mem.uuid for mem in m.members]))
        if k not in res:
            res[k] = H2H(m.members[0].uuid, m.members[1].uuid)
        res[k].inc(m.winner)

    l = list()
    for (p1, p2), h in res.items():
        p1w = h.get(p1)
        p2w = h.get(p2)
        p1s = nickmap[p1]
        p2s = nickmap[p2]
        draw = h.draw
        l.append(f'{p1s} vs {p2s}: {p1w} - {draw} - {p2w}')
    return l
