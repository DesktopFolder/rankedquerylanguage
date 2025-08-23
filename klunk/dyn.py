from klunk.match import QueryMatch
import json

try:
    with open("dyn.json") as file:
        __dyn = json.load(file)
except Exception:
    __dyn = {}

class PlayerRanking:
    def __init__(self):
        self.longest = 0, None, None
        self.current = 0, None
    def inc(self, st: str, mid: int):
        if self.current[1] != st:
            self.current = 1, st
        else:
            self.current = self.current[0] + 1, self.current[1]
        if self.current[0] > self.longest[0]:
            self.longest = self.current[0], self.current[1], mid
    def value(self):
        return self.longest[0]

# Seedtype streaks
__sts = {"__global": PlayerRanking()}
stsnum = 0
def seedtype_streaks(m: QueryMatch):
    global __sts
    global stsnum
    if not m.is_realranked() or m.seed_type is None:
        return
    stsnum += 1
    for p in m.members:
        if p.uuid not in __sts:
            __sts[p.uuid] = PlayerRanking()
        __sts[p.uuid].inc(m.seed_type, m.id)
    __sts["__global"].inc(m.seed_type, m.id)
    

def seedtype_streaks_finish():
    print("Attempting to finish seed types query...")
    print("Ran over", stsnum, "matches")

    fin = {

    }
    glob = __sts.pop("__global")
    cm = sorted(__sts.items(), reverse=True, key=lambda di: di[1].value())

    def fmt(uuid: str, pr: PlayerRanking):
        num, t, last = pr.longest
        return f"{uuid} with {num} matches in a row on {t} seed type (last match: {last})"

    fin["global"] = fmt("global", glob)
    fin["others"] = [fmt(uuid, pr) for uuid, pr in cm[0:5]]

    return fin

queries = {
    "sts": seedtype_streaks,
}
finishes = {
    "sts": seedtype_streaks_finish,
}
todos = [f for q, f in queries.items() if q not in __dyn]
finishes = [(q, f) for q, f in finishes.items() if q not in __dyn]

cmpt = 0

def dynamic_query(m: QueryMatch):
    global cmpt
    cmpt += 1
    for x in todos:
        x(m)

def finish_query():
    global __dyn
    print("Finishing all queries, ran over", cmpt, "matches")
    for name, f in finishes:
        __dyn[name] = f()
        print(json.dumps(__dyn[name], indent=2))
    with open("dyn.json", "w") as file:
        json.dump(__dyn, file)
