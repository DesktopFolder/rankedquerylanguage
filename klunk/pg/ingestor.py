# From Sichi! :) Thanks
PLAYOFFS_SEASON_1 = [371960, 372000, 372046, 372084, 372180, 372221, 372262, 372339, 372371, 372402, 372462, 372497, 372529, 372595, 372634, 372672, 372737, 372796, 372840, 372912, 372961, 388232, 388265, 388302, 388361, 388401, 388443, 388480, 388513, 388574, 388590, 388616, 388639, 388701, 388725, 388749, 388774, 388801, 390660, 390690, 390710, 390804, 390829, 390863, 390887, 390923, 391015, 391060, 391103, 391138, 391210, 391246, 391274, 391317, 391348, 391376, 391403]

PLAYOFFS_SEASON_2 = [538713, 538738, 538777, 538799, 538832, 538887, 538917, 538969, 538995, 539095, 539132, 539222, 539248, 539332, 539355, 539386, 539440, 539457, 539495, 551034, 551066, 551093, 551132, 551159, 551208, 551234, 551274, 551344, 551375, 551410, 551434, 551451, 551508, 551527, 551553, 551587, 551603, 553157, 553183, 553250, 553374, 553432, 553463, 553516, 553593, 553632, 553669, 553719, 553741, 553849, 553871, 553903, 553924, 553956, 553972, 553997]

PLAYOFFS_SEASON_3 = [709562, 709596, 709684, 709733, 709803, 709865, 709936, 709982, 710021, 710080, 710137, 710177, 710241, 710309, 710343, 710388, 710427, 710478, 710504, 710522, 722052, 722082, 722147, 722212, 722234, 722271, 722291, 722308, 722350, 722381, 722432, 722456, 722488, 722541, 722567, 722610, 724452, 724481, 724520, 724621, 724667, 724721, 724781, 724828, 724896, 724932, 724999, 725045, 725097, 725129, 725167]
PLAYOFFS = PLAYOFFS_SEASON_1 + PLAYOFFS_SEASON_2 + PLAYOFFS_SEASON_3
def default_groups(dirname):
    """ copied from dataset.py - avoid making changes """
    from os import listdir

    return [
        x[0] for x in sorted([(x, int(x.split("-")[0])) for x in listdir(dirname) if x.endswith(".txt")], key=lambda v: v[1])
    ]


def get_skip_rule(groups):
    """ copied from dataset.py - avoid making changes """
    if len(groups) < 20:
        return lambda _: False
    def skip_rule(data):
        # ignore_seasons = [(0, []), (1, PLAYOFFS_SEASON_1), (2, PLAYOFFS_SEASON_2), (3, PLAYOFFS_SEASON_3), (4, []), (5, [])]
        ignore_seasons = []
        for s, passthrough in ignore_seasons:
            if data.season == s:
                if data.id in passthrough:
                    return False
                return True
        return False
    return skip_rule

def from_json_string(s: str):
    import json

    return json.loads(s)

def to_db(m: dict, cur):
    """
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
    """
    import psycopg
    assert isinstance(cur, psycopg.Cursor)
    is_decay = m["is_decay"]

    id: int = m["match_id"]  # FILTER: Basic
    seed_type: str | None = None if is_decay else m["seed_type"]  # FILTER: Basic
    bastion: str = m.get("bastionType", None)
    type: int = m["match_type"]  # FILTER: Translation
    winner: str = m["winner"] if m["winner"] is not None else ("__draw" if not is_decay else "__decay")
    # members = UUIDList([MatchMember(mem) for mem in m["members"]])
    duration: int = m["final_time"]
    is_ff: bool = m["forfeit"]
    season: int = m["match_season"]
    date: int = m["match_date"]
    category: str = m.get("category", "UNKNOWN")
    assert is_decay is not None
    # timelines = TimelineList(sorted([Timeline(tl) for tl in (m["timelines"] or list())], key=lambda tl: tl.time))
    tag: str | None = m.get("tag")
    scored: bool = m["score_changes"] is not None and len(m["score_changes"]) > 0
    was_fixed = False

    # Fix 
    if "timelines" in m and m["timelines"]:
        s = max([t["time"] for t in m["timelines"]])
        if s > duration:
            was_fixed = True
            duration = s
        if self.rql_completed():
            self.timelines.append(Timeline.custom(uuid=self.winner, name='completed', time=self.duration))
            for member in self.get_other_members(self.winner):
                self.timelines.append(Timeline.custom(uuid=member.uuid, name='lost', time=self.duration))
    
    spectated: bool = len(m.get("spectators", [])) > 0
    cur.execute("INSERT INTO matches (id, seed_type, type, members, winner, loser, duration, is_ff, season, category, date, is_decay, is_abnormal, tag, spectated, bastion) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (id, seed_type, type, members, winner, loser, duration, is_ff, season, category, date, is_decay, is_abnormal, tag, spectated, bastion))



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

def load_raw_matches(dirname, quiet=False):
    import psycopg
    """
    This function does all of the match loading for the bot. Loads from
    our kind-of-cursed database system/datastore.
    Now only loads [s4, s5...]
    i.e. we are now ignoring s3 or previous matches, EXCEPT playoffs games.
    """
    assert not dirname or dirname.endswith("/")
    print('pg loading raw matches from', dirname)

    # [100000-120000.txt, ...]
    groups = default_groups(dirname)
    skip_rule = get_skip_rule(groups)

    # Debug - number of matches we ignore (= {})
    ignored = 0



    with psycopg.connect("dbname=rqldb port=7134 host=localhost password=example user=postgres") as conn:
        with conn.cursor() as cur:
            """
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
            """
            # Notes:
            # Integer largest number is 2bil
            cur.execute("""
                DROP TABLE matches;
                DROP TABLE players;
                DROP TABLE playerlists;
                DROP TABLE categories;
                DROP TYPE SEEDTYPE;
                DROP TYPE BASTION;
            """)
            cur.execute("""
                CREATE TYPE SEEDTYPE AS ENUM ('ruined_portal', 'village', 'buried_treasure', 'decay', 'unknown', 'shipwreck', 'desert_temple')
                """)
            cur.execute("""
                CREATE TYPE BASTION AS ENUM ('treasure', 'housing', 'stables', 'bridge', 'unknown')
                """)
            """
            Plan:
            First player (fake) is decay player, winner of all decay matches.
            """
            cur.execute("""
                CREATE TABLE players (
                    id serial PRIMARY KEY,
                    uuid char(32)
            )""")
            cur.execute("""
                CREATE TABLE playerlists (
                    id serial PRIMARY KEY,
                    players integer[]
            )""")
            cur.execute("""
                CREATE TABLE categories (
                    id serial PRIMARY KEY,
                    value varchar
            )""")
            cur.execute("""
                CREATE TABLE matches (
                    id integer PRIMARY KEY,
                    seed_type SEEDTYPE NOT NULL,
                    type smallint NOT NULL,
                    members serial references playerlists(id),
                    winner serial references players(id),
                    loser serial references players(id),
                    duration integer NOT NULL,
                    is_ff boolean,
                    season smallint,
                    category serial references categories(id),
                    date bigint,
                    is_decay boolean,
                    -- timelines
                    is_abnormal boolean,
                    tag varchar,
                    spectated boolean,
                    bastion BASTION
                )""")


            for g in groups:
                with open(f"{dirname}{g}") as file:
                    # one line = one match
                    for l in file:
                        stripped = l.strip()
                        if stripped != "{}":
                            try:
                                data = from_json_string(stripped)
                                to_db(data, cur)
                            except Exception as e:
                                print(f"Char 0: {stripped[0]}")
                                raise RuntimeError(f'Bad JSON document: "{stripped}"') from e
                        else:
                            ignored += 1
    """
    if not quiet:
        print(f"Loaded {len(res)} matches. Ignored {ignored} bad matches.")
    if not all(res[i]["match_id"] < res[i + 1]["match_id"] for i in range(0, len(res) - 1)):
        raise RuntimeError("Matches are out of order!")
    if not quiet:
        print(f'Latest match id: {res[-1]["match_id"]}')
    """
