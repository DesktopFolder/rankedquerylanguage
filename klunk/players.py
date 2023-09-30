from typing import Callable
from .extra_types import Milliseconds, UUID, Seconds
from .utils import percentage_str, time_fmt
from . import match


def stime_fmt(*args, **kwargs):
    return time_fmt(*args, **kwargs).lstrip('0:')


def match_int_dict():
    return {1: 0, 2: 0, 3: 0, 4: 0}

class ExtractFailure:
    pass

def _extract(t, k):
    # UGH THIS SHOULD BE FACTORED OUT
    # STOP COPY PASTING THINGS AHHHHH
    if not k.startswith('_'):
        if hasattr(t, k):
            return getattr(t, k)
        if hasattr(t, 'rql_' + k):
            return getattr(t, 'rql_' + k)()
    return ExtractFailure

def _lextract(tl, k):
    if not tl:
        return None
    v0 = _extract(tl[0], k)
    if v0 is ExtractFailure:
        return None
    return [_extract(tlv, k) for tlv in tl]


class Player:
    def __init__(self, nick: str, uuid: UUID, latest, elo):
        # Sanity check because I added this argument, so...
        # Remove later for performance reasons.
        self.nick = nick
        self.uuid = uuid
        self.latest = latest
        self.elo = elo # NONE UNLESS CALIBRATED
        if self.elo == -1:
            self.elo = None
        self.history = dict()
        self.history_missing = 0

        self.played_per = match_int_dict()
        self.time_per = match_int_dict()
        self.decayed = 0

        # FF STATS :)
        self.ff_losses = match_int_dict()
        self.ff_wins = match_int_dict()
        self.wins = match_int_dict()
        self.losses = match_int_dict()

        # Ranked-only Stats (for now)
        self.match_completions = 0
        self.time_completions = 0

        self.pb: Milliseconds | None = None

        self.dynamic = dict()

    def __str__(self):
        return f'{self.nick} ({self.elo} elo)'

    def __repr__(self):
        return f'<Player({self.nick}, {self.elo}>'

    def extract(self, k):
        return _extract(self, k)

    def completions(self, mode=2):
        return self.wins[mode] - self.ff_wins[mode]

    def completion_winpct(self, mode=2):
        return 1 - (self.ff_wins[mode] / self.wins[mode])

    def completion_allpct(self, mode=2):
        # total wins - ff_wins = completion #
        return (self.wins[mode] - self.ff_wins[mode]) / self.played_per[mode]

    def n_completion_winpct(self, mode=2):
        return percentage_str(self.completions(mode), self.wins[mode])

    def n_completion_allpct(self, mode=2):
        return percentage_str(self.completions(mode), self.played_per[2])

    def total_time(self):
        return sum(self.time_per.values())

    def rql_total_time(self):
        return Milliseconds(sum(self.time_per.values()))

    def total_games(self):
        return sum(self.played_per.values())

    def rql_total_games(self):
        return sum(self.played_per.values())

    def avg_completion(self, mode=2):
        assert mode == 2
        return self.time_completions / self.match_completions

    def rql_average_completion(self):
        if self.match_completions == 0:
            return None
        return Milliseconds(self.time_completions // self.match_completions)

    def display(self):
        print(
            f'{self.nick}: {self.played_per[2]} ranked games, {self.ff_losses[2]} ranked forfeits, {self.wins} wins ({self.ff_wins[2]} due to forfeits), {self.losses[2]} total losses.')

    def tournament_summary(self):
        # really nice formatting
        print(f'{self.nick}:\n'
              f'{self.elo} ({int(round(100*self.wins[2] / self.played_per[2], 0))}%)\n'
              f'{stime_fmt(self.avg_completion())} / {stime_fmt(self.pb or 0)}')

    def tournament_summary_classic(self):
        print(f'{self.nick} ({self.elo} final elo):',
              f'{time_fmt(self.avg_completion())} average completion. (PB: {time_fmt(self.pb or 0)})',
              f'Winrate: {percentage_str(self.wins[2], self.played_per[2])}')


class PlayerManager:
    def __init__(self, l: list[match.QueryMatch], inject=set()):
        self.players: dict[str, Player] = {}
        self.games_added = 0
        self.ranked_added = 0

        for m in l:
            for member in m.members:
                assert type(member) == match.MatchMember
                uuid = member.uuid
                assert uuid is not None
                if uuid not in self.players:
                    self.players[uuid] = Player(
                        member.user, UUID(uuid), m.date, member.elo_after)

                p = self.players[uuid]

                if p.latest < m.date:
                    p.nick = member.user
                    # YOUR ELO IS NONE UNLESS YOU CALIBRATE
                    p.elo = member.elo_after
                    if p.elo == -1:
                        p.elo = None
                    p.latest = m.date

                if m.has_elos:
                    p.history[m.date] = member.elo_after
                else:
                    p.history_missing += 1

                if not m.is_decay:
                    p.played_per[m.type] += 1
                    p.time_per[m.type] += m.duration
                    if uuid == m.winner:
                        if m.is_ff:
                            p.ff_wins[m.type] += 1
                        else: # CHANGED from ELIF type = 2
                            # Ranked, no FF, winner, not decay.
                            p.match_completions += 1
                            p.time_completions += m.duration
                            p.pb = min(p.pb, m.duration) if p.pb is not None else m.duration
                        p.wins[m.type] += 1
                    else:
                        if m.is_ff:
                            p.ff_losses[m.type] += 1
                        p.losses[m.type] += 1
                else:
                    p.decayed += 1

                if 'nether_entries' in inject:
                    if 'nether_entries' not in p.dynamic:
                        p.dynamic['nether_entries'] = list()
                    assert type(m) == match.QueryMatch
                    e = m.earliest('story.enter_the_nether', UUID(uuid))
                    if e is not None:
                        p.dynamic['nether_entries'].append(e.time)

            if not m.is_decay:
                self.games_added += 1
                if m.type == 2:
                    assert(len(m.members) == 2)
                    self.ranked_added += 1

    def filtered(self,
                 min_games=match_int_dict(),
                 win_games=match_int_dict(),
                 f: Callable = lambda _: True):
        def mg(p, md):
            return all([p.played_per[t] >= md[t] for t in range(1, 5) if t in md])

        def wg(p, wd):
            return all([p.wins[t] >= wd[t] for t in range(1, 5) if t in wd])
        return [p for p in self.players.values() if mg(p, min_games) and wg(p, win_games) and f(p)]

    def leaderboard(self):
        return sorted(self.players.values(), key=lambda o: o.elo, reverse=True)

    def lookup(self, nick: str):
        nick = nick.lower()
        for user in self.players.values():
            if user.nick.lower() == nick:
                return user
        raise KeyError(f'{nick} not in players')


class MatchPlayer:
    def __init__(self, uuid):
        self.uuid = uuid
        self.elo_before = None
        self.elo_after = None
        self.tls = list()

    @staticmethod
    def from_match(m: match.QueryMatch):
        def uuid_iter(m):
            for p in m.members:
                yield p['uuid']

        mps = {uuid: MatchPlayer(uuid) for uuid in uuid_iter(m)}

        if m.valid_timelines():
            for tlid, uidtimes in m.timelines:
                for uid, time in uidtimes:
                    assert uid in mps
                    mps[uid].tls.append((tlid, time))

        if m.has_elos:
            for p in m.members:
                mps[p['uuid']].elo_before = p['elo_before']
                mps[p['uuid']].elo_after = p['elo_after']

        return list(mps.values())

def top_n(l: list[Player], n=10, display=None, key=None, reverse=True, compress_extra=None, is_pct=True):
    assert len(l) >= n
    if key is not None:
        if compress_extra:
            l = sorted(l, key=key, reverse=reverse)[0:n]
            print(f'{l[0]} - {(display or key)(l[0])}')
            # #2: Couriway (71%),
            if is_pct:
                print(', '.join(
                    [f'#{i + 2}: {p.nick} ({int(round(key(p)*100, 0))}%)' for i, p in enumerate(l[1:])]))
            else:
                print(
                    ', '.join([f'#{i + 2}: {p.nick} ({key(p)})' for i, p in enumerate(l[1:])]))
            return
        for p in sorted(l, key=key, reverse=reverse)[0:n]:
            print(f'{p} - {(display or key)(p)}')
        return
    display = display or (lambda p: f'decayed {p.decayed} times')
    for p in l[0:n]:
        print(f'{p} - {display(p)}')
