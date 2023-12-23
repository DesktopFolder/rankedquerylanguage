CURRENT_SEASON = 2


def fFF(m) -> bool:  # Filters for forfeits
    MIN_DURATION = 1000 * 60 * 4
    if isinstance(m, Match):
        return m.is_ff or m.duration < MIN_DURATION
    if not ("forfeit" in m and type(m["forfeit"]) is bool):
        raise RuntimeError(m)
    return m["forfeit"] == True or m.get("final_time", 0) < MIN_DURATION


def fNOFF(m) -> bool:  # Filters for non-forfeits
    return not fFF(m)


def fSEASON(m, season) -> bool:
    return season is None or m.season == season


def ufSEASON(season):
    return lambda m: fSEASON(m, season)


def fREALMATCH(m) -> bool:
    if isinstance(m, Match):
        return not m.is_decay
    return not m.get("is_decay", False)


def fCOMPLETED(m) -> bool:
    # not a forfeit, not cheated, not a draw, not a decay
    return not fFF(m) and not m["winner"] == None and fREALMATCH(m)


def fRANKED(m) -> bool:
    return m.type == 2


def fDEFAULTS(m):
    return fSEASON(m, CURRENT_SEASON) and fREALMATCH(m) and fRANKED(m)


def fNONE(m):
    return True


def ufALLOF(*args):
    def fALLOF(m):
        return all([a(m) for a in args])

    return fALLOF


def uuid_sort(l):
    return sorted(l, key=lambda mem: mem["uuid"])


def filtered(u, *args):
    return [x for x in u if all([f(x) for f in args])]
