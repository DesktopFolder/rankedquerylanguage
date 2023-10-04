from klunk.extra_types import UUID, Milliseconds
from klunk.utils import time_fmt
from .match import MatchMember, QueryMatch, from_json_string
from .filters import *
from typing import Any
from .players import Player

_datasets_ = None
__discord = False

class PikaConnection:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.recv: list[bytes] = list()

    def callback(self, _, __, __1__, body):
        print(f'received body with len {len(body)} (first bit: {body[0:24]})')
        self.recv.append(body)

    def start_consuming(self):
        import pika
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='rql-ipc')
        self.channel.basic_consume(on_message_callback=lambda *args: self.callback(*args), queue='rql-ipc')
        
        # clear out the queue.
        self.connection.process_data_events(0)
        self.recv.clear()

    def update_datasets(self):
        global _datasets_
        assert self.connection is not None
        self.connection.process_data_events(0)
        recv = self.recv
        self.recv = list()
        ilen = len(recv)
        print(f'Updating with {ilen} matches.')
        # Check for validity first.
        recv = [m.decode() for m in recv]
        recv = [m for m in [m.split('|', maxsplit=1) for m in recv] if len(m) == 2 and m[0].isnumeric()]
        alen = len(recv)
        if alen != ilen:
            print(f'Removed {ilen - alen} bad messages (test/etc?)')

        # Actually update with these matches.
        res: list[QueryMatch] = list()
        for n, l in recv:
            stripped = l.strip()
            if stripped != '{}':
                try:
                    res.append(from_json_string(stripped))
                except Exception as e:
                    print(f'Char 0: {stripped[0]}')
                    raise RuntimeError(f'Bad JSON document: "{stripped}"') from e
            else:
                pass

        assert _datasets_ is not None
        if res[0].season != res[-1].season or res[0].season != _datasets_['default'].l[0].season:
            raise RuntimeError(f'The current season has changed. Please reboot the bot :)')

        # assume default is unchanged.
        res = sorted(res, key=lambda m: m.id)
        _datasets_['default'].update(AsDefaultDatalist(res, res[0].season))
        _datasets_['all'].update(res)
        _datasets_['most'].update(AsMostDatalist(res))
        uuids, users = GetUserMappings(res)
        _datasets_['__uuids'].update_overwrite_dict(uuids)
        _datasets_['__users'].update_overwrite_dict(users)


_mq_ = PikaConnection()

def default_groups(dirname):
    from os import listdir
    return [x[0] for x in sorted([(x, int(x.split('-')[0])) for x in listdir(dirname) if x.endswith('.txt')], key=lambda v:v[1])]

def load_raw_matches(dirname, quiet = False) -> list[QueryMatch]:
    assert not dirname or dirname.endswith('/')

    # [100000-120000.txt, ...]
    groups = default_groups(dirname)

    # list[QueryMatch]
    res: list[QueryMatch] = list()

    # Debug - number of matches we ignore (= {})
    ignored = 0

    for g in groups:
        with open(f'{dirname}{g}') as file:
            # one line = one match
            for l in file:
                stripped = l.strip()
                if stripped != '{}':
                    try:
                        res.append(from_json_string(stripped))
                    except Exception as e:
                        print(f'Char 0: {stripped[0]}')
                        raise RuntimeError(f'Bad JSON document: "{stripped}"') from e
                else:
                    ignored += 1
    if not quiet:
        print(f'Loaded {len(res)} matches. Ignored {ignored} bad matches.')
    if not all(res[i]['match_id'] < res[i + 1]['match_id'] for i in range(0, len(res) - 1)):
        raise RuntimeError("Matches are out of order!")
    if not quiet:
        print(f'Latest match id: {res[-1]["match_id"]}')
    return res

def to_idx_key(s: str):
    return tuple(sorted([x.strip() for x in s.split('.') if x.strip()]))

def to_idx(s: str, l: list[QueryMatch], cs=None) -> list[QueryMatch]:
    key = to_idx_key(s)

    if 'ranked' in key:
        l = [m for m in l if fRANKED(m)]
    if 'nodecay' in key:
        l = [m for m in l if not m.is_decay]
    if 'noabnormal' in key:
        l = [m for m in l if not m.is_abnormal]
    if 'current' in key:
        l = [m for m in l if m.season == cs]

    return l

def format_str(o: object):
    if o is None:
        return '<None>'
    if type(o) == QueryMatch:
        return str(o)
    if type(o) == tuple:
        return ' '.join([format_str(v) for v in o])
    if type(o) == str:
        return o
    if type(o) == Player:
        return str(o)
    if type(o) == UUID:
        if _datasets_ is not None:
            try:
                if __discord:
                    return _datasets_['__uuids'].l[o].replace('_', '\\_')
                return _datasets_['__uuids'].l[o]
            except KeyError:
                raise KeyError(f'{o} is not a valid username.')
    if type(o) == Milliseconds:
        return time_fmt(o)
    return str(o)
    # raise RuntimeError(f'Could not convert {type(o)} to formatted result.')

class Dataset:
    def __init__(self, name: str, l):
        self.l = l
        self.name = name

    def clone(self, l):
        return Dataset(self.name, l)

    def update(self, other: list[QueryMatch]):
        last_mid = self.l[-1]
        # ensure we don't get duplicate matches
        ilen = len(other)
        other = [m for m in other if m.id > last_mid]
        print(f'removed {ilen - len(other)} matches from update (dupes)')
        self.l.extend(other)

    def update_overwrite_dict(self, other: dict[str, str]):
        for k, v in other.items():
            self.l[k] = v

    def info(self):
        if type(self.l) in [list, dict]:
            return f'Dataset {self.name}, currently with {len(self.l)} objects.'
        return f'Dataset containing {format_str(self.l)}'

    def summarize(self):
        val = self.l
        if type(val) == dict:
            val = list(val.items())
        if type(val) == list:
            length = len(val)
            if length == 1:
                return format_str(val[0])
            res = '\n'.join([f'{i+1}. {format_str(v)}' for i, v in enumerate(val[0:10])])
            if length > 10:
                res += f'\n... ({length - 10} values trimmed)'
            return res
        if type(val) == str:
            return val

    def example(self):
        if type(self.l) in [list, set]:
            try:
                return self.l[0]
            except:
                return None
        return self.l

def AsDefaultDatalist(l: list[QueryMatch], current_season: int):
    return to_idx('ranked.current.nodecay', l, current_season)

def AsMostDatalist(l: list[QueryMatch]):
    return to_idx('ranked.nodecay.noabnormal', l)

def GetUserMappings(l: list[QueryMatch]):
    uuids = {}
    users = {}
    for m in l:
        for p in m.members:
            assert type(p) == MatchMember
            users[p.user.lower()] = p.uuid
            uuids[p.uuid] = p.user
    uuids['__draw'] = 'Drawn Match'
    users['drawn match'] = '__draw'
    return uuids, users

def load_defaults(p: str, quiet = False, set_discord = False):
    global __discord
    if set_discord:
        __discord = True
    global _datasets_
    if _datasets_ is None:
        print('Starting RabbitMQ consumer.')
        _mq_.start_consuming()
        print('Finished loading RabbitMQ consumer.')
        l = load_raw_matches(p, quiet)
        uuids, users = GetUserMappings(l)
        _datasets_ = {
            "default": Dataset("Default", AsDefaultDatalist(l, l[-1].season)),
            "all": Dataset("All", l),
            "most": Dataset("Most", AsMostDatalist(l)),
            "__uuids": Dataset("UUIDs", uuids),
            "__users": Dataset("Users", users),
        }

    # first, pull new matches from rmq
    _mq_.update_datasets()

    return _datasets_
