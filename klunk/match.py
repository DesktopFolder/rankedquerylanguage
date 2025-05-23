from typing import Any, Callable, Optional

from .parse import parse_boolean
from .extra_types import *

ABNORMAL_MATCH_MS = 6 * 60 * 1000
IMPOSSIBLE_MATCH_MS = 4 * 60 * 1000

# Just this season's top few completions' players.
NON_ABNORMAL_PLAYERS = {
        'lowk3y_', 'dandannyboy', '7rowl', 'v_strid', 'NoFearr1337', 'Oxidiot', 'Waluyoshi',
}

def is_abnormal(m) -> bool:
    """
    is_abnormal(match)
    Decides whether a match is ABNORMAL or LEGITIMATE.
    See code for the process...
    """

    # If the match is not completed, we have no applicable checks.
    # So, incomplete matches are always (!) legitimate, currently.
    if not m.rql_completed():
        return False

    # If the winner is NOT a valid member, then this is glitched.
    if m.winner not in [x.uuid for x in m.members]:
        return True

    # If the duration is WAY TOO SHORT, *always* exit out.
    if m.duration <= IMPOSSIBLE_MATCH_MS:
        return True

    # If the duration is TOO SHORT, exit out. 
    if m.duration <= ABNORMAL_MATCH_MS:
        # Also check for known good players.
        try:
            # This is in a try-catch block because IT TURNS OUT there are corrupted matches
            # where the winner of the match is just some random uuid (???) not a member.
            if m.get_member(m.winner).user not in NON_ABNORMAL_PLAYERS:
                # They're a cheater because their username is not lowk3y_
                return True
        except:
            return True

    # Match duration is good, winner is a valid player, etc. We're fine.
    return False

def type_str(t: int):
    return {1: "Casual", 2: "Ranked", 3: "Private", 4: "Event"}[t]


def type_int(t: str):
    return {"casual": 1, "ranked": 2, "private": 3, "event": 4}[t.lower()]


def _extract(t, k, *args):
    if not k.startswith("_"):
        if hasattr(t, k):
            return getattr(t, k)
        if hasattr(t, "rql_" + k):
            return getattr(t, "rql_" + k)(*args)
    return ExtractFailure


class MatchMember:
    __slots__ = (
        "uuid",  # uuid: str (UUID... obviously)
        "user",  # nickname: str
        "badge",  # badge: int
        "old_elo",  # elo_rate: int (*)
        "old_rank",  # elo_rank: int (*)
        "elo",  # score_changes..[elo_before]: int
        "change",  # score_changes..[change]: int
        "elo_after",  # elo + change: int
    )

    def __init__(self, match_member: dict[str, Any]):
        self.uuid: str = UUID(match_member["uuid"])
        self.user: str = match_member["nickname"]
        self.badge: int = match_member.get('badge', match_member.get('roleType'))
        self.old_elo: int = match_member["elo_rate"]
        self.old_rank: int = match_member["elo_rank"]
        self.elo = None
        self.change = None
        self.elo_after = None

    def extract(self, attribute):
        return _extract(self, attribute)

    def __str__(self):
        return f"{self.user}"

    def __getitem__(self, attribute):
        return self.extract(attribute)


class ExtractFailure:
    pass


class Timeline:
    __slots__ = (
        "time",  # time: Milliseconds
        "id",  # timeline: str
        "uuid",  # uuid: UUID
    )

    def __init__(self, timeline):
        self.time: Milliseconds = Milliseconds(timeline["time"])
        self.id: str = timeline["timeline"]
        self.uuid: UUID = UUID(timeline["uuid"])

    @staticmethod
    def from_items(time, id, uuid):
        return Timeline({"time": time, "timeline": id, "uuid": uuid})

    @staticmethod
    def custom(uuid, name, time):
        return Timeline.from_items(uuid=uuid, id=f'rql.{name}', time=time)

    def __repr__(self):
        return f"Timeline({self.time}, {self.id}, {self.uuid})"

    def extract(self, t: str):
        ex = _extract(self, t)
        if ex is ExtractFailure:
            raise RuntimeError(f"Could not extract {t} from Timeline.")
        return ex


def _lextract(tl, k):
    if not tl:
        return None
    v0 = _extract(tl[0], k)
    if v0 is ExtractFailure:
        return None
    return [_extract(tlv, k) for tlv in tl]


class UUIDList(list):
    def get(self, uuid: UUID) -> MatchMember | None:
        for mem in self:
            if mem.uuid == uuid:
                return mem
        return None

    def extract(self, key: str):
        # Extract is our magic method.
        # We do this abstraction to allow for future changes.
        return _lextract(self, key)

    def basic_repr(self):
        return "".join(sorted([m.uuid for m in self]))

    def __str__(self):
        return f'UUIDList ({len(self)} elements): {", ".join([str((m.uuid, m.user)) for m in self])}'

    def __eq__(self, other):
        # We care if the list is 'relevantly same' for now
        return self.basic_repr() == other.basic_repr()

    def __hash__(self):
        return hash(self.basic_repr())


class TimelineList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Generate a special lookup dictionary.
        self.lookup: dict[str, list[Timeline]] = dict()
        for tl in self:
            assert isinstance(tl, Timeline)
            if tl.id not in self.lookup:
                self.lookup[tl.id] = list()
            self.lookup[tl.id].append(tl)

    def extract(self, key: str):
        return _lextract(self, key)

    def earlist_time(self, tl_id) -> Milliseconds | None:
        if tl_id in self.lookup:
            return self.lookup[tl_id][0].time
        else:
            return None

    def all(self, tl_id) -> list[Timeline]:
        return self.lookup.get(tl_id, list())


def SortBy(l, q, r=False):
    if not l:
        return list()
    if not hasattr(l[0], q) or q.startswith("_"):
        raise RuntimeError(f"{q} is not valid to sort by")  # TODO - query.Error, how to get class name idk
    return sorted(l, key=lambda v: getattr(v, q), reverse=r)


class QueryMatch:
    __slots__ = (  # Mostly direct match keys.
        "id",  # match_id: int
        "seed_type",  # seed_type: str
        "type",  # match_type: int
        "winner",  # winner: str (UUID)
        "members",  # members: UUIDList
        "duration",  # final_time: Milliseconds
        # 'score_changes', # dict
        "is_ff",  # forfeit: bool
        "season",  # match_season: int ([0, ..))
        "category", # category: str (Always 'ANY'?)
        "date",  # match_date: Seconds
        "is_decay",  # is_decay: bool
        # 'completes', # list[dict[UUID, Milliseconds]]
        "timelines",  # timelines: dict
        # Debug and meta keys
        "scored",  # debug; has score_changes
        "has_elos",  # debug; has score_changes and it's good
        "was_fixed",  # debug; timelines used to fix duration
        "is_abnormal",
        "dynamic", # meta - store arbitrary data.
        "tag",
        "spectated",
        "bastion",
    )

    @staticmethod
    def filter_value(a, b, uuidmap):
        from .translations import MatchVariablesDict as tl

        a = tl.get(a, a)
        if a == "type":
            return type_int(b)
        if a == "winner" or a == "members":
            return uuidmap.smart_uuid(b)
        if a == "is_ff":
            return parse_boolean(b)
        return b

    def is_filtered(self, a, b, func):
        from .translations import MatchVariablesDict as tl

        a = tl.get(a, a)
        if a not in self.__slots__:
            raise ValueError(f"{a} is not a valid match filter.")

        # id: basic
        # seed type: basic
        # match type: basic (assuming filter_value was properly supplied)
        # winner: uuid

        return func(self.extract(a))

    def __init__(self, m):
        # Important early things

        # This is mapped by the fetcher. Actual key: "decayed"
        self.is_decay = m["is_decay"]

        # Basic ones (mostly copied from JSON)
        self.id: int = m["match_id"]  # FILTER: Basic
        self.seed_type: Optional[str] = None if self.is_decay else m["seed_type"]  # FILTER: Basic
        # NEW THINGS HAVE DIFFERENT NAMING CONVENENTIONS.
        # OLD THINGS DON'T. THEY ARE MAPPED BY THE BOT.
        # YIKES.
        self.bastion: str = m.get("bastionType", None)
        self.type: int = m["match_type"]  # FILTER: Translation
        self.winner: UUID = UUID(m["winner"]) if m["winner"] is not None else (UUID("__draw") if not m["is_decay"] else UUID("__decay"))
        self.members = UUIDList([MatchMember(mem) for mem in m["members"]])
        self.duration: Milliseconds = Milliseconds(m["final_time"])
        self.is_ff = m["forfeit"]
        self.season = m["match_season"]
        self.date: Seconds = Seconds(m["match_date"])
        self.category: str = m.get("category", "UNKNOWN")
        assert self.is_decay is not None
        # TIMELINE LIST IS SORTED BY DEFAULT. THIS IS A GOOD THING.
        self.timelines = TimelineList(sorted([Timeline(tl) for tl in (m["timelines"] or list())], key=lambda tl: tl.time))
        self.dynamic: None | dict = None
        self.tag: str | None = m.get("tag")
        
        self.spectated: bool = len(m.get("spectators", [])) > 0

        self.scored = m["score_changes"] is not None and len(m["score_changes"]) > 0
        self.was_fixed = False
        if "timelines" in m and m["timelines"]:
            s = max([t["time"] for t in m["timelines"]])
            if s > self.duration:
                self.was_fixed = True
                self.duration = s
            if self.rql_completed():
                self.timelines.append(Timeline.custom(uuid=self.winner, name='completed', time=self.duration))
                for member in self.get_other_members(self.winner):
                    self.timelines.append(Timeline.custom(uuid=member.uuid, name='lost', time=self.duration))

        self.is_abnormal = False
        # Checking for abnormal matches.
        # Mainly, there are non-ff matches that have 0 duration.
        if is_abnormal(self):
            # No cheated matches in S2 but there are in S3, so.
            self.is_abnormal = True

        # Feature checks
        self.has_elos = self.scored

        if self.scored:
            for p in m["score_changes"]:
                if "change" not in p:
                    self.has_elos = False
                    break
                s = self.members.get(p["uuid"])
                assert s is not None
                s.elo = p["score"]
                if p["score"] == -1:
                    s.elo_after = None
                else:
                    s.elo_after = p["score"] + p["change"]
                s.change = p["change"]

    def rql_split_time(self, split: str, uuid: str):
        l = [t for t in self.timelines.all(split) if t.uuid == uuid]
        if not l:
            return None
        return min(l, key=lambda t: t.time).time

    def rql_is_draw(self):
        return self.winner == "__draw"

    def rql_loser(self):
        return self.get_other_member(self.winner).uuid

    def rql_winner(self):
        return self.get_member(self.winner).uuid

    def rql_completed(self):
        # NOTE - Draws are now marked as ff and weren't before.
        return not self.rql_is_draw() and not self.is_ff

    def rql_is_completed(self):
        return not self.rql_is_draw() and not self.is_ff

    def rql_uuids(self) -> list[UUID]:
        return [m.uuid for m in self.members]

    def rql_is_playoffs(self):
        return self.tag is not None and 'playoff' in self.tag

    def rql_dynamic(self, key="default"):
        if self.dynamic is None:
            raise RuntimeError(f'Cannot extract dynamic data... it has not been set.')
        return self.dynamic[key]

    def extract(self, t: str, *args):
        ex = _extract(self, t, *args)
        if ex is ExtractFailure:
            return self.members.extract(t, *args) or self.timelines.extract(t, *args) or ExtractFailure
        return ex

    def get_member(self, uuid):
        # Note to self - this does UUID comparisons properly.
        # If this is crashing, you're probably checking corrupted matches.
        for m in self.members: # UUIDList
            if m.uuid == uuid:
                return m
        raise ValueError(f"Could not find uuid {uuid} in match ID {self.id}")

    def get_other_member(self, uuid):
        # assert uuid in [x.uuid for x in self.members]
        for m in self.members:
            if m.uuid != uuid:
                return m
        raise ValueError(f"Could not find other uuid for {uuid} in match ID {self.id}")

    def get_other_members(self, uuid) -> list[MatchMember]:
        return [m for m in self.members if m.uuid != uuid]

    def valid_elos(self):
        return [
            m["elo_before"] for m in self.members if "elo_before" in m and m["elo_before"] is not None and m["elo_before"] >= 0
        ]

    def current_elos(self):
        return [m["elo_rate"] for m in self.members]

    def lowest_elo(self):
        es = self.valid_elos()
        if es:
            return min(es)
        return None

    # def is_draw(self):
    #    return self.winner is None

    def victor_elo(self):
        assert self.has_elos
        return self.get_member(self.winner)["elo_before"]

    def loser_elo(self):
        assert self.has_elos
        return self.get_other_member(self.winner)["elo_before"]

    def type_str(self):
        return type_str(self.type)

    def __repr__(self):
        return f"<Match(id={self.id},type={self.type},date={self.date},duration={self.duration})>"

    def __str__(self):
        from .utils import time_fmt

        try:
            memberstr = " vs ".join([v.user for v in self.members])
        except Exception as e:
            raise RuntimeError(self.members) from e
        return f"Match #{self.id}: {memberstr} ({time_fmt(self.duration)})"

    def rql_pretty(self):
        if self.is_decay:
            return f"Match #{self.id}: {self.members[0].user} decayed"
        win = self.get_member(self.winner).user if not self.rql_is_draw() else "Drawn Match"
        return f"{self} Winner: {win}"

    def __contains__(self, key):
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    def __getitem__(self, key):
        if key == "match_id":
            return self.id
        if key == "match_season":
            return self.season
        if key == "match_type":
            return self.type
        if key == "forfeit":
            return self.is_ff
        if key == "is_decay":
            return self.is_decay
        if key == "winner":
            return self.winner
        raise KeyError(f"QueryMatch.__getitem__ requires support for key {key}")

    def is_realranked(self):
        return not self.is_decay and self.type == 2

    def is_ranked(self):
        return self.type == 2

    def valid_timelines(self) -> bool:
        return self.timelines is not None and len(self.timelines) > 0

    def get_split(self, split: str):
        return self.timelines.get(split, None)

    def has_once(self, split: str, player=None):
        # Guarantee p is either none or uuid
        p = None if player is None else self.smart_player(player)["uuid"]
        s = self.get_split(split)
        if s is not None:
            for o in s:
                if o[0] == p:
                    return True
        return False

    def earliest(self, split: str, player: UUID | None = None):
        """
        Returns either None (no match / timeline data) or (timeline, time)
        """
        if not self.valid_timelines():
            return None

        if split in self.timelines.lookup:
            sd: list[Timeline] = self.timelines.lookup[split]
            # List must be > 0 - otherwise it shouldn't be in the lookup.
            assert len(sd) > 0

            # player param must be a UUID. We don't do silly lookups here. Do that externally.
            valid = [tl for tl in sd if player is None or tl.uuid == player]

            # Same as before. Pretty useless asserts but leaving for now.
            assert len(valid) > 0 or player is not None

            # Split didn't exist.
            if not valid:
                return None

            # Get the earliest.
            sorted_valid = sorted(valid, key=lambda tlo: tlo.time)
            return sorted_valid[0]

        return None


def iter(u, filt: Callable, *args):
    for m in u:
        if filt(m) and all([f(m) for f in args]):
            yield Match(m) if not isinstance(m, Match) else m


def from_json_string(s: str):
    import json

    return QueryMatch(json.loads(s))
