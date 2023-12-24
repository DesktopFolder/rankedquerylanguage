import discord
from discord import app_commands
from io import BytesIO
from klunk import sandbox
from klunk.language import ParseError

ONE_TIME = True


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
            # all should not be None. handle that first.
            assert not all([x is None for x in [result, file, additional]])

            # OK. We want lits=[additional, result.summarize()]
            if file and not additional:
                return build_literal(None, file)

            additional = additional or list()

            if not file and result is not None:
                additional.append(result.summarize())

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


async def run_discord_query(interaction: discord.Interaction, query: str, notes=None):
    print("Running Discord query:", query)
    await interaction.response.defer(ephemeral=False, thinking=True)
    resp = ENGINE.run(query, False, False, True)
    print("Bot finished running query:", query)
    notes = "" if notes is None else "\n" + "\nNote: ".join(notes)
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


@client.tree.command()
@app_commands.choices(
    value=[
        app_commands.Choice(name="Fastest Completions", value="pb"),
        app_commands.Choice(name="Elo", value="elo"),
        app_commands.Choice(name="Average Completion", value="average_completion"),
    ]
)
@app_commands.describe(
    value="The type of leaderboard you want to generate.",
    season="The season to generate a leaderboard for. By default, the current season.",
)
async def qb_leaderboard(interaction: discord.Interaction, value: app_commands.Choice[str], season: int | None = None):
    leaderboard_queries = {
        "pb": "filter noff | drop duration lt(332324) | sort duration | take 10 | extract id date winner duration",
        "elo": "players | drop elo None() | rsort elo | take 10",
        "average_completion": "players | drop average_completion None() | sort average_completion | take 10 | extract nick average_completion match_completions",
    }
    v = value.value
    if v not in leaderboard_queries:
        await interaction.response.send_message(f"Your value of {v} is not a valid choice.")
    else:
        query = leaderboard_queries[value.value]
        if season is not None:
            query = f"index s{season} | {query}"
        await run_discord_query(interaction, query)


@client.tree.command()
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
            r"players\s*\|\s*filter\s*nick\([\w\s]*\)\s*\|\s*extract (nick )?average_completion\s*$",
            "*Note: /average\\_completion has been added to simplify this.*",
        )
    ]
    notes = None
    for rxp, res in lints:
        if re.match(rxp, query) is not None:
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
