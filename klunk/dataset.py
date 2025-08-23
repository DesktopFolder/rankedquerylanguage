from klunk.extra_types import UUID, Milliseconds, Seconds
from klunk.utils import time_fmt
from .match import MatchMember, QueryMatch, Timeline, TimelineList, from_json_string
from .filters import *
from typing import Any
from .players import Player

# From Sichi! :) Thanks
PLAYOFFS_SEASON_1 = [371960, 372000, 372046, 372084, 372180, 372221, 372262, 372339, 372371, 372402, 372462, 372497, 372529, 372595, 372634, 372672, 372737, 372796, 372840, 372912, 372961, 388232, 388265, 388302, 388361, 388401, 388443, 388480, 388513, 388574, 388590, 388616, 388639, 388701, 388725, 388749, 388774, 388801, 390660, 390690, 390710, 390804, 390829, 390863, 390887, 390923, 391015, 391060, 391103, 391138, 391210, 391246, 391274, 391317, 391348, 391376, 391403]

PLAYOFFS_SEASON_2 = [538713, 538738, 538777, 538799, 538832, 538887, 538917, 538969, 538995, 539095, 539132, 539222, 539248, 539332, 539355, 539386, 539440, 539457, 539495, 551034, 551066, 551093, 551132, 551159, 551208, 551234, 551274, 551344, 551375, 551410, 551434, 551451, 551508, 551527, 551553, 551587, 551603, 553157, 553183, 553250, 553374, 553432, 553463, 553516, 553593, 553632, 553669, 553719, 553741, 553849, 553871, 553903, 553924, 553956, 553972, 553997]

PLAYOFFS_SEASON_3 = [709562, 709596, 709684, 709733, 709803, 709865, 709936, 709982, 710021, 710080, 710137, 710177, 710241, 710309, 710343, 710388, 710427, 710478, 710504, 710522, 722052, 722082, 722147, 722212, 722234, 722271, 722291, 722308, 722350, 722381, 722432, 722456, 722488, 722541, 722567, 722610, 724452, 724481, 724520, 724621, 724667, 724721, 724781, 724828, 724896, 724932, 724999, 725045, 725097, 725129, 725167]

PLAYOFFS = PLAYOFFS_SEASON_1 + PLAYOFFS_SEASON_2 + PLAYOFFS_SEASON_3

_datasets_ = None
__discord = False

__database = None
def use_sql():
    global __database
    if __database is not None:
        raise RuntimeError(f"Could not select database with ID pg: is already set to {__database}!")
    __database = "pg"
def is_sql():
    return __database == "pg"

SUPPORTED_ITERABLES = set([list, dict, set, tuple, TimelineList])
CURRENT_SEASON = None

def first_not_none(l):
    for x in l:
        if x is not None:
            return x
    return None

class PikaConnection:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.recv: list[bytes] = list()
        self.broken = False

    def callback(self, ch, method, __1__, body):
        print(f"received body with len {len(body)} (first bit: {body[0:24]})")
        self.recv.append(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        try:
            import pika

            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue="rql-ipc")
            self.channel.basic_consume(on_message_callback=lambda *args: self.callback(*args), queue="rql-ipc")

            # clear out the queue.
            self.connection.process_data_events(0)
            self.recv.clear()
        except Exception as e:
            print(e)
            self.broken = True

    def attempt_process_pika(self, itr=0):
        if self.broken:
            print("ignored attempt to process pika, broken")
            return
        try:
            assert self.connection is not None
            self.connection.process_data_events(0)
            self.connection.sleep(0.1)
        except:
            if itr > 4:
                self.broken = True
                raise RuntimeError("Auto updating has broken :sob: please retry your query.")
            import pika

            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue="rql-ipc")
            self.channel.basic_consume(on_message_callback=lambda *args: self.callback(*args), queue="rql-ipc")
            self.attempt_process_pika(itr + 1)

    def update_datasets(self):
        global _datasets_
        if self.connection is None:
            print('did not update datasets: no connection')
            return
        self.attempt_process_pika()

        recv = self.recv
        self.recv = list()
        ilen = len(recv)
        if not recv:
            return
        print(f"Updating with {ilen} matches.")
        # Check for validity first.
        recv = [m.decode() for m in recv]
        recv = [m for m in [m.split("|", maxsplit=1) for m in recv] if len(m) == 2 and m[0].isnumeric()]
        alen = len(recv)
        if alen != ilen:
            print(f"Removed {ilen - alen} bad messages (test/etc?)")

        if not recv:
            return

        # Actually update with these matches.
        res: list[QueryMatch] = list()
        for n, l in recv:
            stripped = l.strip()
            if stripped != "{}":
                try:
                    res.append(from_json_string(stripped))
                except Exception as e:
                    print(f"Char 0: {stripped[0]}")
                    raise RuntimeError(f'Bad JSON document: "{stripped}"') from e
            else:
                pass

        if not res:
            return

        assert _datasets_ is not None
        if res[0].season != res[-1].season or res[0].season != _datasets_["default"].l[0].season:
            raise RuntimeError(f"The current season has changed. Please tell DesktopFolder to reboot the bot :)")

        # assume default is unchanged.
        res = sorted(res, key=lambda m: m.id)
        _datasets_["default"].update(AsDefaultDatalist(res, res[0].season))
        _datasets_["all"].update(res)
        _datasets_["most"].update(AsMostDatalist(res))
        _datasets_["matchanalysis"].update(to_idx("ranked.nodecay.noabnormal", res))
        uuids, users = GetUserMappings(res)
        _datasets_["__uuids"].update_overwrite_dict(uuids)
        _datasets_["__users"].update_overwrite_dict(users)


_mq_ = PikaConnection()


def default_groups(dirname):
    from os import listdir

    return [
        x[0] for x in sorted([(x, int(x.split("-")[0])) for x in listdir(dirname) if x.endswith(".txt")], key=lambda v: v[1])
    ]


def get_skip_rule(groups):
    if len(groups) < 20:
        return lambda _: False
    def skip_rule(data):
        ignore_seasons = [(0, []), (1, PLAYOFFS_SEASON_1), (2, PLAYOFFS_SEASON_2), (3, PLAYOFFS_SEASON_3), (4, []), (5, [])]
        for s, passthrough in ignore_seasons:
            if data.season == s:
                if data.id in passthrough:
                    return False
                return True
        return False
    return skip_rule


def load_raw_matches(dirname, quiet=False) -> list[QueryMatch]:
    """
    This function does all of the match loading for the bot. Loads from
    our kind-of-cursed database system/datastore.
    Now only loads [s4, s5...]
    i.e. we are now ignoring s3 or previous matches, EXCEPT playoffs games.
    """
    assert not dirname or dirname.endswith("/")

    # [100000-120000.txt, ...]
    groups = default_groups(dirname)
    skip_rule = get_skip_rule(groups)

    # list[QueryMatch]
    res: list[QueryMatch] = list()

    # Debug - number of matches we ignore (= {})
    ignored = 0

    for g in groups:
        with open(f"{dirname}{g}") as file:
            # one line = one match
            for l in file:
                stripped = l.strip()
                if stripped != "{}":
                    try:
                        data = from_json_string(stripped)
                        if skip_rule(data):
                            continue
                        res.append(data)
                    except Exception as e:
                        print(f"Char 0: {stripped[0]}")
                        raise RuntimeError(f'Bad JSON document: "{stripped}"') from e
                else:
                    ignored += 1
    if not quiet:
        print(f"Loaded {len(res)} matches. Ignored {ignored} bad matches.")
    if not all(res[i]["match_id"] < res[i + 1]["match_id"] for i in range(0, len(res) - 1)):
        raise RuntimeError("Matches are out of order!")
    if not quiet:
        print(f'Latest match id: {res[-1]["match_id"]}')
    return res


def to_idx_key(s: str):
    return tuple(sorted([x.strip() for x in s.split(".") if x.strip()]))


def to_idx(s: str, l: list[QueryMatch], cs=None) -> list[QueryMatch]:
    key = to_idx_key(s)

    if "ranked" in key:
        l = [m for m in l if fRANKED(m)]
    if "nodecay" in key:
        l = [m for m in l if not m.is_decay]
    if "noabnormal" in key:
        l = [m for m in l if not m.is_abnormal]
    if "current" in key:
        l = [m for m in l if m.season == cs]

    return l


def mid_idx(mids: list[int], l: list[QueryMatch]) -> list[QueryMatch]:
    mids = sorted(mids)
    
    def pred(q: QueryMatch):
        if not mids:
            return False
        if q.id == mids[0]:
            mids.pop(0)
            return True
        return False
    
    l = [m for m in l if pred(m)]

    if mids:
        print(f'Warning! Match ids not found while constructing index: {mids}')

    return l


def format_str(o: object):
    if o is None:
        return "<None>"
    if type(o) == tuple:
        return " ".join([format_str(v) for v in o])
    if type(o) == Timeline:
        return format_str(tuple([o.uuid, o.id, o.time]))
    if type(o) == str:
        return o
    if type(o) == UUID:
        if _datasets_ is not None:
            try:
                if __discord:
                    return _datasets_["__uuids"].l[o].replace("_", "\\_")
                return _datasets_["__uuids"].l[o]
            except KeyError:
                raise KeyError(f"{o} is not a valid username.")
    if type(o) == Milliseconds:
        return time_fmt(o)
    if type(o) == Seconds and __discord:
        return f"<t:{o}:R>"
    if isinstance(o, list) and len(o) < 5:
        return str([format_str(so) for so in o])
    return str(o)
    # raise RuntimeError(f'Could not convert {type(o)} to formatted result.')


class Dataset:
    def __init__(self, name: str, l, root=False):
        self.l = l
        self.name = name
        self.has_unranked: bool = True
        if root and isinstance(self.l, list):
            if self.l and isinstance(self.l[0], QueryMatch):
                # if all matches are ranked
                if all([m.is_ranked() for m in l]):
                    self.has_unranked = False

    def __len__(self):
        if type(self.l) in SUPPORTED_ITERABLES:
            return len(self.l)
        return 1

    def has_iterable(self):
        return type(self.l) in SUPPORTED_ITERABLES

    def iter(self):
        if self.has_iterable():
            return iter(self.l)
        return [self.l]

    def clone(self, l):
        d = Dataset(self.name, l)
        d.has_unranked = self.has_unranked
        return d

    def update(self, other: list[QueryMatch]):
        last_mid = self.l[-1].id
        # ensure we don't get duplicate matches
        ilen = len(other)
        other = [m for m in other if m.id > last_mid]
        print(f"removed {ilen - len(other)} matches from update (dupes)")
        self.l.extend(other)

    def update_overwrite_dict(self, other: dict[str, str]):
        for k, v in other.items():
            self.l[k] = v

    def info(self):
        if type(self.l) in [list, dict]:
            return f"Dataset {self.name}, currently with {len(self.l)} objects."
        return f"Dataset containing {format_str(self.l)}"

    def detailed_info(self):
        return f"Dataset {self.name}. Contains {len(self)} items. Type of first item: {type(self.example())}"

    def summarize(self, formatter=None):
        def cleaned(s):
            if formatter is not None and 'clean' in formatter:
                return formatter["clean"](s)
            return s
        val = self.l
        if type(val) == dict:
            val = list(val.items())
        if isinstance(val, list):
            length = len(val)
            if length == 1:
                return cleaned(format_str(val[0]))
            res = cleaned("\n".join([f"{i+1}. {format_str(v)}" for i, v in enumerate(val[0:10])]))
            if length > 10:
                res += f"\n... ({length - 10} values trimmed)"
            return res
        if type(val) == str:
            return cleaned(val)
        if type(val) in [tuple, MatchMember, QueryMatch]:
            return format_str(val)
        raise RuntimeError(f'Could not summarize {type(val)}')

    def example(self):
        if type(self.l) in SUPPORTED_ITERABLES:
            try:
                if type(self.l) == dict:
                    return first_not_none(list(self.l.values()))
                return first_not_none(self.l)
            except:
                return None
        return self.l


class UUIDDataset:
    def __init__(self, users_to_uuids: Dataset, uuids_to_users: Dataset) -> None:
        self.users_to_uuids = users_to_uuids.l
        self.uuids_to_users = uuids_to_users.l

    def convert_uuid(self, uuid: UUID) -> str:
        # Convert a UUID into a nickname string.
        return self.uuids_to_users[uuid]

    def convert_user(self, user: str) -> UUID:
        # Convert a username into a UUID string.
        return self.users_to_uuids[user.lower()]


def AsDefaultDatalist(l: list[QueryMatch], current_season: int):
    return to_idx("ranked.current.noabnormal", l, current_season)


def AsMostDatalist(l: list[QueryMatch]):
    return to_idx("ranked.noabnormal", l)


def GetUserMappings(l: list[QueryMatch]):
    uuids = {}
    users = {}
    for m in l:
        for p in m.members:
            assert type(p) == MatchMember
            users[p.user.lower()] = p.uuid
            uuids[p.uuid] = p.user
    uuids["__draw"] = "Drawn Match"
    uuids["__decay"] = "Decayed Match"
    users["drawn match"] = "__draw"
    users["decayed match"] = "__decay"
    return uuids, users


def cleanup_matches(l: list[QueryMatch]):
    for i, m in enumerate(l):
        # Fix tagged playoffs matches
        if m.type == 3 and m.spectated and m.tag is not None:
            # Private match. Maybe tag propagation required.
            # Grab last hour / next hour and check.
            ONE_HOUR = 60 * 60

            # Back scan.
            loc = i
            while loc > 0:
                loc -= 1
                pm = l[loc]
                if (m.date - pm.date) > ONE_HOUR:
                    break
                if pm.type != 3 or not pm.spectated or pm.tag is not None:
                    continue
                if pm.members == m.members:
                    pm.tag = m.tag

            # Forward scan.
            loc = i
            while loc < len(l) - 1:
                loc += 1
                pm = l[loc]
                if (pm.date - m.date) > ONE_HOUR:
                    break
                if pm.type != 3 or not pm.spectated or pm.tag is not None:
                    continue
                if pm.members == m.members:
                    pm.tag = m.tag
    return l


def load_defaults(p: str, quiet=False, set_discord=False, no_mq=False):
    global __discord
    if set_discord:
        __discord = True
    global _datasets_
    if p.startswith("pg:"):
        from .pg import ingestor
        ingestor.load_raw_matches(p.split(':', maxsplit=1)[1])
        return
    if _datasets_ is None:
        if not no_mq:
            print("Starting RabbitMQ consumer.")
            _mq_.start_consuming()
            print("Finished loading RabbitMQ consumer.")
        l = cleanup_matches(load_raw_matches(p, quiet))
        uuids, users = GetUserMappings(l)
        _datasets_ = {
            "default": Dataset("Default", AsDefaultDatalist(l, l[-1].season), root=True),
            "all": Dataset("All", l, root=True),
            "most": Dataset("Most", AsMostDatalist(l), root=True),
            "matchanalysis": Dataset("Match Analysis", to_idx("ranked.nodecay.noabnormal",l), root=True),
            "playoffs1": Dataset("Ranked Playoffs 1", mid_idx(PLAYOFFS_SEASON_1, l), root=True),
            "playoffs2": Dataset("Ranked Playoffs 2", mid_idx(PLAYOFFS_SEASON_2, l), root=True),
            "playoffs3": Dataset("Ranked Playoffs 3", mid_idx(PLAYOFFS_SEASON_3, l), root=True),
            "playoffs": Dataset("Ranked Playoffs", mid_idx(PLAYOFFS, l), root=True),
            "__uuids": Dataset("UUIDs", uuids, root=True),
            "__users": Dataset("Users", users, root=True),
        }
        global CURRENT_SEASON
        CURRENT_SEASON = l[-1].season

    if not no_mq:
        # first, pull new matches from rmq
        _mq_.update_datasets()

    return _datasets_
