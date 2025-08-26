import discord
from discord import app_commands
from io import BytesIO
from klunk import sandbox
from klunk.language import ParseError
from klunk.match import NON_ABNORMAL_PLAYERS

ONE_TIME = True

WARNS_ETC = set()


class QueryEngine:
    def __init__(self) -> None:
        self.formatter: None | dict = None

    def clean(self, s: str) -> str:
        if self.formatter is not None:
            return self.formatter["clean"](s)
        return s

    def run(self, query: str, debug: bool = False, timing: bool = False, is_bot: bool = False, no_mq: bool = False) -> dict:
        """Run a query within a sandbox.
        
        debug - passed on to sandbox.Query
        timing - passed on to sandbox.Query
        is_bot - Seems to prepend notes to our output?
        """
        sb = sandbox.Query(query, debug, timing, self.formatter, no_mq=no_mq)

        """
        {"file": FILE_DATA, "literal": PRINTED_DATA}
        """


        def format_result(s: str):
            if is_bot and sb.runtime and sb.runtime.notes:
                fmt_notes = "\n".join([f"Note: {self.clean(note)}" for note in sb.runtime.notes])
                return f"{fmt_notes}\n{s}"
            return s


        def build_literal(lits: list[str]|None, file=None):
            d = dict()
            if lits is not None:
                d = {"literal": "\n".join([format_result(str(s)) for s in lits])}
            if file is not None:
                d["file"] = file
            return d


        def build_result(result):
            file = None
            additional = sb._result
            upload = sb._do_upload
            if upload:
                if result is None:
                    additional = additional or list()
                    additional.append("Error: No dataset was actionable for upload.")
                elif not isinstance(result.l, list):
                    additional = additional or list()
                    additional.append(f"Error: {type(result.l)} (resulting dataset type) cannot be uploaded.")
                else:
                    file = result.l

            # result: resulting dataset
            # additional: misc warnings etc
            # file: resulting dataset except it's a file
            if all([x is None for x in [result, file, additional]]):
                # e.g. `validate` -> never does add_result (so additional is None)
                # -> returns no dataset, no file
                return build_literal(None, None)

            # OK. We want lits=[additional, result.summarize()]
            if file and not additional:
                return build_literal(None, file)

            additional = additional or list()

            if not file and result is not None:
                additional.append(result.summarize(formatter=self.formatter))

            return build_literal(additional, file)


        try:
            return build_result(sb.run())
        # ALL ERROR HANDLING BELOW HERE.
        except ParseError as e:
            extra = "\nIt looks like your query failed to parse. "
            extra += "Try `/query help` to get some examples."
            if sb._tracebacks:
                import traceback

                return {"literal": format_result(f"{traceback.format_exc().rstrip()}{extra}")}
            else:
                return {"literal": format_result(f"Error: {e}{extra}")}
        except Exception as e:
            if sb._tracebacks:
                import traceback

                return {"literal": format_result(f"{traceback.format_exc().rstrip()}")}
            else:
                return {"literal": format_result(f"Error: {e}")}


ENGINE = QueryEngine()


class MyClient(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        # A CommandTree is a special type that holds all the application command
        # state required to make it work. This is a separate class because it
        # allows all the extra state to be opt-in.
        # Whenever you want to work with application commands, your tree is used
        # to store and work with them.
        # Note: When using commands.Bot instead of discord.Client, the bot will
        # maintain its own tree instead.
        self.tree = app_commands.CommandTree(self)

    # In this basic example, we just synchronize the app commands to one guild.
    # Instead of specifying a guild to every command, we copy over our global commands instead.
    # By doing so, we don't have to wait up to an hour until they are shown to the end-user.
    async def setup_hook(self):
        # This copies the global commands over to your guild.
        # self.tree.copy_global_to(guild=MY_GUILD) # don't bother with this anymore
        await self.tree.sync()  # this syncs to other guilds


intents = discord.Intents.default()
client = MyClient(intents=intents)


DiscordFormatter = {
    "tick": lambda s: f"`{s}`",
    "ticks": lambda s: f"```\n{s}```",
    "bold": lambda s: f"**{s}**",
    "italics": lambda s: f"_{s}_",
    "underline": lambda s: f"__{s}__",
    "strike": lambda s: f"~{s}~",
    "doc": lambda s: "```\n" + " ".join([x.strip() for x in s.split("\n") if x.strip() is not None]) + "\n```",
    "username": lambda s: s.replace("_", "\\_"),
    "clean": lambda s: s.replace("_", "\\_"),
}


@client.event
async def on_ready():
    if client.user is None:
        print(f"Logged in as None?")
    else:
        print(f"Logged in as {client.user} (ID: {client.user.id})")


def get_warns(uid: int):
    global WARNS_ETC
    if uid not in WARNS_ETC:
        WARNS_ETC.add(uid)
        return '\n*All seasons have been removed except for the current season due to extreme memory issues. Modifications to use a SQL backend and support all seasons again are in progress, no ETA. Sorry!*\n'
        # return '\n**Important note:** Ranked matches from seasons 0, 1, 2, and 3 are NO LONGER available in the discord bot dataset. (Excepting playoff matches, which are still loaded.) I still have this data and will start loading it again if/when I rewrite the bot to not consume 15gb of data...\n**Other updates:** The "category" key/value pair is now available in match data.\n'
    return ''


async def run_discord_query(interaction: discord.Interaction, query: str, notes=None):
    print("Running Discord query:", query)
    await interaction.response.defer(ephemeral=False, thinking=True)
    resp = ENGINE.run(query, False, False, True)
    print("Bot finished running query:", query)
    warns = get_warns(interaction.user.id)
    notes = f"{warns}" if notes is None else "{warns}\nNote: ".join(notes)
    literal = "" if "literal" not in resp else f"\n{resp['literal']}"
    try:
        if "file" in resp:
            # Attempt string conversion. LOL this might be a bad idea.
            s = str()
            one_gb = 2 * 1024 * 1024 * 1024
            from klunk import dataset

            for l in resp["file"]:
                s += dataset.format_str(l)
                s += "\n"
                if len(s) > one_gb:
                    await interaction.followup.send(f"From query: `{query}`: Failed to upload, too large (>2gb){notes}{literal}")
                    return
            s = s.encode("utf-8")
            await interaction.followup.send(f"From query: `{query}`{notes}{literal}", file=discord.File(BytesIO(s), "result.txt"))
        else:
            if len(literal) > 2000:
                await interaction.followup.send(
                    f"Your query has a result size of {len(literal)} characters, which is too long. Try with +asfile| at the start."
                )
            else:
                await interaction.followup.send(f"From query: `{query}`:{notes}{literal}")
    except Exception as e:
        print(f"Failed to send response to /query - likely it took too long: {e}.")
        await interaction.followup.send(f"Your query failed. This is usually because you generated something that Discord's API rejected. Try generating a smaller result set.")


@client.tree.command()
@app_commands.describe(
    username="The case-insensitive username of the player to get a current-season average completion time for."
)
async def average_completion(interaction: discord.Interaction, username: str):
    await run_discord_query(interaction, f"players | filter uuid({username}) | extract nick average_completion")


@client.tree.command()
async def qb_info(interaction: discord.Interaction):
    # First just make sure we don't time out with the interaction.
    await interaction.response.defer(ephemeral=False, thinking=True)

    from klunk.dataset import load_defaults

    loc = open("location.txt").read().strip()
    data = load_defaults(loc, quiet=True, set_discord=True)
    latest = data["most"].l[-1]
    datasets = ", ".join([x for x in data.keys() if not x.startswith("__")])

    # Compose the response.
    s = f"QueryBot active.\nExplicit datasets loaded: {datasets}"
    s += f"\nMost recent match loaded: {latest} (<t:{latest.date}:R>)"

    await interaction.followup.send(s)


def apply_season(season: int | None, query_str: str):
    if season is not None:
        return f"index s{season} | {query_str}"
    return query_str


@client.tree.command(description="Get results for a player (defaults to previous season)")
@app_commands.describe(
    player="The player to get previous-season stats for.",
    season="The season (default: previous)",
)
async def qb_quicklook(interaction: discord.Interaction, player: str, season: int | None = None):
    from klunk.dataset import CURRENT_SEASON as cs
    season = season if season is not None else cs - 1
    s = f"index s{season}"
    p = f"filter uuid({player})"
    tls = f"| {s} | {p} | to_timelines"
    def bast(name):
        return f"| label \"For bastion {name}:\" | {s} | {p} bastion({name}) | players | {p} | extract tournament_fmt | quicksave "

    query = (
            f"+asfile | {s} | {p} | players | {p} | extract tournament_fmt | quicksave " +
            f"{tls} | splits.get_if projectelo.timeline.reset | {p} | count Resets | average time " +
            f"{tls} | splits.get_if projectelo.timeline.death | {p} | count Deaths | average time " +
            f"{tls} | splits.get_if projectelo.timeline.death_spawnpoint | {p} | count DeathResets | average time " +
            f"{tls} | splits.get_if nether.root | {p} | count Nethers | average time " +
            f"{tls} | splits.get_if nether.find_fortress | {p} | count Bastions | average time " +
            f"{tls} | splits.get_if nether.find_bastion | {p} | count Fortresses | average time " +
            f"{tls} | splits.get_if story.follow_ender_eye | {p} | count Strongholds | average time " +
            f"{tls} | splits.get_if story.enter_the_end | {p} | count Ends | average time " +
            bast("TREASURE") +
            bast("STABLES") +
            bast("BRIDGE") +
            bast("HOUSING")
    )

    await run_discord_query(interaction, query)

@client.tree.command(description="Various dynamic leaderboards with multiple options")
@app_commands.choices(
    value=[
        app_commands.Choice(name="Fastest Completions", value="pb"),
        app_commands.Choice(name="Elo", value="elo"),
        app_commands.Choice(name="Average Completion", value="average_completion"),
        app_commands.Choice(name="Average Stronghold Entry", value="average_stronghold"),
        app_commands.Choice(name="Average End Entry", value="average_end"),
    ],
    seed_type=[
        app_commands.Choice(name="None", value=""),
        app_commands.Choice(name="Village", value="filter seed_type(village) | "),
        app_commands.Choice(name="Desert Temple", value="filter seed_type(desert_temple) | "),
        app_commands.Choice(name="Shipwreck", value="filter seed_type(shipwreck) | "),
        app_commands.Choice(name="Ruined Portal", value="filter seed_type(ruined_portal) | "),
        app_commands.Choice(name="Buried Treasure", value="filter seed_type(buried_treasure) | "),
    ]
)
@app_commands.describe(
    value="The type of leaderboard you want to generate.",
    season="The season to generate a leaderboard for. By default, the current season.",
    player="The player you want to get the leaderboard position of (otherwise gets top 10)",
)
async def qb_leaderboard(interaction: discord.Interaction, value: app_commands.Choice[str], season: int | None = None, player: str | None = None, seed_type: app_commands.Choice[str] | None = None):
    from klunk.dataset import CURRENT_SEASON as cs
    sz = cs if season is None else season
    seastr = f'index s{sz} | '
    if seed_type is None:
        ststr = ""
    else:
        ststr = seed_type.value
    leaderboard_queries = {
        "pb": f"{ststr}filter noff | sort duration | take 10 | extract id date winner duration",
        "pb@player": f"{ststr}filter noff | sort duration | enumerate | filter winner({player}) | extract rql_dynamic id date winner duration",
        "elo": f"{ststr}players | drop elo None() | rsort elo | take 10",
        "elo@player": f"{ststr}players | drop elo None() | rsort elo | enumerate | filter uuid({player}) | extract rql_dynamic uuid elo",
        "average_completion": f"{ststr}players | drop average_completion None() | sort average_completion | take 10 | extract nick average_completion match_completions",
        "average_completion@player": f"{ststr}players | drop average_completion None() | sort average_completion | enumerate | filter uuid({player}) | extract rql_dynamic nick average_completion match_completions",
        "average_stronghold": f"{ststr}players lowff manygames | extract uuid | assign VP | {seastr}keepifattrcontained uuid VP | extract timelines | segmentby uuid | splits.get_if story.follow_ender_eye | keepifattrcontained uuid VP | averageby time uuid | sort 1 | take 10",
        "average_stronghold@player!!": f"{ststr}players lowff manygames | extract uuid | assign VP | {seastr}keepifattrcontained uuid VP | extract timelines | segmentby uuid | splits.get_if story.follow_ender_eye | keepifattrcontained uuid VP | averageby time uuid | sort 1 | enumerate | filter 0({player})",
        "average_end": f"{ststr}players lowff manygames | extract uuid | assign VP | {seastr}keepifattrcontained uuid VP | extract timelines | segmentby uuid | splits.get_if story.enter_the_end | keepifattrcontained uuid VP | averageby time uuid | sort 1 | take 10",
        "average_end@player!!": f"{ststr}players lowff manygames | extract uuid | assign VP | {seastr}keepifattrcontained uuid VP | extract timelines | segmentby uuid | splits.get_if story.enter_the_end | keepifattrcontained uuid VP | averageby time uuid | sort 1 | enumerate | filter 0({player})",
    }
    # !! -> these require enumerate to support tuples (not currently possible)
    v = value.value
    if v not in leaderboard_queries:
        await interaction.response.send_message(f"Your value of {v} is not a valid choice.")
        return

    if player is not None:
        v = v + '@player'

    query = apply_season(season, leaderboard_queries[v])

    await run_discord_query(interaction, query)


@client.tree.command(description="See how many games have been completed recently by high-elo players.")
@app_commands.describe(
    elo_min="Minimum elo to scan for. Default: 1500. Minimum: 1000.",
    recent_count="Number of matches you want to scan for activity. Default: 100. Max: 100000.",
)
async def qb_top_activity(interaction: discord.Interaction, elo_min: int = 1500, recent_count: int = 100):
    elo_min = max(elo_min, 1000)
    recent_count = min(recent_count, 100000)
    from klunk.dataset import CURRENT_SEASON as cs

    load_query = f'take last {recent_count} | players | drop elo None() | drop elo lt({elo_min}) | extract uuid | assign highplayers'
    find_query = f' | index s{cs} | take last {recent_count} | filter nodecay | keepifattrcontained uuid highplayers | rsort date | extract date pretty'

    await run_discord_query(interaction, load_query + find_query)


@client.tree.command(description="See stats for a given player/player matchup.")
@app_commands.describe(
    player1="The player you want to get the winrate / stats for.",
    player2="The player you want to get stats against.",
    season="Optional. Default to latest season.",
    to_extract="Optional. Default to | extract winrate.",
)
async def qb_matchup(interaction: discord.Interaction, player1: str, player2: str, season: int | None = None, to_extract: str = "| extract winrate"):
    await run_discord_query(interaction, apply_season(season, f"filter uuid({player1}) uuid({player2}) | players | filter uuid({player1}) {to_extract}"))


FAQ = {
    "update_rate": {
        "name": "Auto Updating",
        "question": "How does the bot's auto-updating work?",
        "answer": "Matches are automatically pulled by ID from the Ranked API. For rate limit reasons, the match puller sometimes waits up to five minutes to pull new matches. Use `/qb_info` to see the latest match that has been ingested into the bot."
    },
    "maintenance": {
        "name": "Ranked Maintenances",
        "question": "Do Ranked maintenance outages affect the bot?",
        "answer": "If the ranked servers are down, the bot will stop loading new matches. However, the query system will still work fine on all matches loaded up until that point. You can see the latest match available in the database with `/qb_info`."
    },
    "about": {
        "name": "About the bot",
        "question": "Who made this bot / how does it work?",
        "answer": "This bot was written by DesktopFolder. It is composed of a Discord bot front-end, which you interact with, and a (bad) database and query system in the backend, which executes your queries. The query language (RQL) and its compiler/runtime are designed from scratch, but based off of APLs (array programming languages) and query languages like Splunk or SQL."
    },
    "averages": {
        "name": "/average_completion",
        "question": "How does `/average_completion` work?",
        "answer": "This command uses a query that finds a player’s average completion time for the current season (ranked matches only!). If you want to get average completion time for another season or a specific set of matches, you will need a to make a custom query with `/query`."
    },
    "learn": {
        "name": "Learning the Language",
        "question": "How do I learn how to use the query language?",
        "answer": "That's the fun part - you don't! More seriously, the language is not particularly well documented. Your best bet is a mix of the following:\n- Reading the results of `/query help` and `/query help FN` for a variety of common functions;\n- Making sure you use `| attrs` to see what attributes are available on the type you're operating over;\n- **Trying out the prewritten queries, like `/average_completion` and `/qb_leaderboard`, and looking at the queries they use;**\n- If you're having difficulties, feel free to ping me, I don't mind :)\n- (Best option if you know Python) Reading the runtime code at <https://github.com/DesktopFolder/rankedquerylanguage/blob/main/klunk/runtime.py> - search for `@Local` to find the source code of the majority of the runtime functions;"
    },
    "correctness": {
        "name": "Dataset Correctness",
        "question": "How correct is the dataset?",
        "answer": "I take data consistency and accuracy very seriously! However, there is a limit to what is possible considering matches are sometimes corrupted, cheated, or not available. Generally speaking, though, data should be completely correct, especially when operating on later seasons. Seasons 0 and 1 had more corrupted/cheated matches (note that both have been removed from the dataset).\nIf you notice a data inconsistency, please let me know :)"
    },
    "cheat": {
        "name": "Cheated Match Filter",
        "question": "How does the 'cheated match filter' work?",
        "answer": f"The dataset includes some cheated (or corrupted?) matches. In all default indices ***except `index all`***, those matches are removed with the `noabnormal` filter. You can apply this filter to your `index all` searches with `| filter noabnormal`. These matches **are autodetected** by the `is_abnormal` function in `match.py`. Matches are considered abnormal in the following situation: They are completed, and their duration is less than 7 minutes, and the winner is not in {NON_ABNORMAL_PLAYERS}."
    },
}

FAQ_CHOICES = [app_commands.Choice(name=v["name"], value=k) for k, v in FAQ.items()]


@client.tree.command(description="Some mediocre responses to RQL questions.")
@app_commands.choices(choice=FAQ_CHOICES)
@app_commands.describe(
    choice="The FAQ you want to access.",
)
async def qb_faq(interaction: discord.Interaction, choice: app_commands.Choice[str]):
    q = choice.value
    if q not in FAQ:
        return await interaction.response.send_message(f"Your choice of {q} is not in the FAQ. Likely a bug?")
    faq = FAQ[q]
    ans = faq["answer"]
    que = faq["question"]
    return await interaction.response.send_message(f"**FAQ: {que}**\n{ans}")


@client.tree.command(description="Make a dynamic query using RQL (see /qb_faq).")
@app_commands.describe(
    query="Your Ranked query string. See #docs for details.",
)
async def query(interaction: discord.Interaction, query: str):
    global ONE_TIME
    if ONE_TIME:
        ONE_TIME = False
        # can't do anything actually useful with this... lol
        await client.change_presence(status=discord.Status.online)
    # Lint level one: Query level.
    import re

    lints = [
        (
            r"^players\s*\|\s*filter\s*nick\([\w\s]*\)\s*\|\s*extract (nick )?average_completion\s*$",
            "*Note: /average\\_completion has been added to simplify this.*",
        ),
        (
            r"keepifattrcontained uuid highplayers",
            "*Note: /qb_top_activity has been added to simplify this.*",
        )
    ]
    notes = None
    for rxp, res in lints:
        if re.search(rxp, query) is not None:
            notes = notes or list()
            notes.append(res)
    await run_discord_query(interaction, query, notes)


def run_cli():
    # CLI version of the bot, so that we can test without a Discord bot.
    print_debug = False
    try:
        import readline  # pyright: ignore
    except:
        print(
            "Warning: Was unable to import the readline module. "
            "To get readline support on windows, try `pip install pyreadline`"
        )
    while True:
        try:
            query = input("> ").strip()
            if query == "+debug":
                print_debug = True
            elif query == "-debug":
                print_debug = False
            else:
                print(ENGINE.run(query, print_debug, no_mq=True).get("literal", "Query had no result as only a file was produced."))
        except EOFError:
            break


def main(args):
    if "--preload" in args:
        print("Preloading all loadable data into engine.")
        sandbox.Query("+debug timing tb | testlog 'Preloaded data.'").run()
        print("Finished preloading. Starting up...")

    if "--fake" in args:
        run_cli()
    else:
        # Any Discord-only configuration should be done here.
        ENGINE.formatter = DiscordFormatter
        client.run(open("token.txt").read().strip())


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
