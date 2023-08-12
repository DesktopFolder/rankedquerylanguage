import discord
from discord import app_commands

# leaving this commented in case I want to port this behaviour to v2
#from qb.db import load_raw_matches, to_idx, to_idx_key
#from qb.match import QueryMatch

from klunk import sandbox


MY_GUILD = discord.Object(id=1133544816563716250)  # replace with your guild id
try:
    token = open('token.txt').read().strip()
except:
    token = None


# leaving this commented in case I want to port this behaviour to v2
"""
class QueryEngine:
    def __init__(self):
        import query 
        self.p = query.parse
        self.matches: list[QueryMatch] = load_raw_matches('samples/')
        self.current_season = self.matches[-1].season
        self.default_idx = 'ranked.current'
        self.valid_idxs = {'ranked.nodecay', self.default_idx, 'all'}
        self.idxs: dict[tuple[str, ...], list[QueryMatch]] = {to_idx_key(idx): to_idx(idx, self.matches, self.current_season) for idx in self.valid_idxs}
        self.idxs[('all', )] = self.matches

    def get_index_key(self, idx_str: str | None):
        if idx_str == 'default':
            return to_idx_key(self.default_idx)
        return to_idx_key(idx_str or self.default_idx)

    def run(self, s: str, db=False) -> str:
        import query
        # Generate our result
        parse_result = self.p(s)
        log = parse_result.get_log()

        # More debugging
        db = db or parse_result['debug']
        
        # Generate the start of our result.
        if parse_result.is_error:
            res = f'Found Error: {parse_result}'
        else:
            assert isinstance(parse_result, query.QueryParser)
            res = f'Parsed Query: {parse_result.query}' if db else ""

        # Add in our log if we're in debug mode.
        if db:
            log = log.strip('\n ')
            if log:
                res += '\nDebug Log:\n'
                res += log
                res += "\n"
            else:
                res += '\nNo debug log generated.\n'

        # Now actually run the query.
        if not parse_result.is_error:
            assert isinstance(parse_result, query.QueryParser)

            # Get the index we're supposed to run the query over.
            idx = parse_result.query.idx 

            # Acquire the data from that index.
            idx_key = self.get_index_key(idx)
            pretty_idx_key = '.'.join(idx_key)
            data = self.idxs.get(idx_key)

            # Validation
            if data is None:
                res += f'Error: Index {idx} (key: {pretty_idx_key}) is invalid. Valid indexes: {self.valid_idxs}'
            else:
                if db:
                    res += f'Operating in loaded index {pretty_idx_key}\n'
                res += parse_result.execute(data)

        return res
"""

class QueryEngineV2:
    def __init__(self) -> None:
        self.formatter = None

    def run(self, query: str, debug: bool=False, timing: bool=False, is_bot: bool=False):
        sb = sandbox.Query(query, debug, timing, self.formatter)

        def format_result(s: str):
            if is_bot and sb.runtime and sb.runtime.notes:
                fmt_notes = "\n".join([f'Note: {note}' for note in sb.runtime.notes])
                return f'{fmt_notes}\n{s}'
            return s
        try:
            result = sb.run()
            if sb._result is not None:
                return format_result('\n'.join(sb._result))
            if self.formatter is not None:
                # TODO - maybe format discord here idk man
                return format_result(f'{result.summarize()}')
            return format_result(f'{result.summarize()}')
        except Exception as e:
            if sb._tracebacks:
                import traceback
                return format_result(f'{traceback.format_exc().rstrip()}')
            else:
                return format_result(f'Error: {e}')


# qe = QueryEngine()
qe = QueryEngineV2()


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
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync()


intents = discord.Intents.default()
client = MyClient(intents=intents)


DiscordFormatter = {
    "tick": lambda s: f'`{s}`',
    "ticks": lambda s: f'```\n{s}```',
    "bold": lambda s: f'**{s}**',
    "italics": lambda s: f'_{s}_',
    "underline": lambda s: f'__{s}__',
    "strike": lambda s: f'~{s}~',
    "doc": lambda s: '```\n' + ' '.join([x.strip() for x in s.split('\n') if x.strip() is not None]) + '\n```',
    "username": lambda s: s.replace('_', '\\_')
}


@client.event
async def on_ready():
    if client.user is None:
        print(f'Logged in as None?')
    else:
        print(f'Logged in as {client.user} (ID: {client.user.id})')


@client.tree.command()
async def status(interaction: discord.Interaction):
    await interaction.response.send_message(f'Status is a thing, {interaction.user.mention}')


@client.tree.command()
@app_commands.describe(
    query='Your Ranked query string. See #docs for details.',
)
async def query(interaction: discord.Interaction, query: str):
    # COC-PYWRITE
    # also doc the above lol 
    # also, check how to remove/rename commands...
    print('Running query:', query)
    resp = qe.run(query, False, False, True)
    print('Bot finished running query:', query)
    try:
        await interaction.response.send_message(f'From query: `{query}`:\n{resp}')
    except:
        print('Failed to send response to /query - likely it took too long.')


if __name__ == '__main__':
    import sys
    a = sys.argv[1:]
    if '--preload' in a:
        sandbox.Query("+debug timing tb | testlog 'Preloaded data.'").run()
        print('Finished preloading. Starting up...')
    if '--fake' in a:
        db = False
        import readline
        while True:
            try:
                x = input("> ").strip()
                if x == '+debug':
                    db = True
                elif x == '-debug':
                    db = False
                else:
                    print(qe.run(x, db))
            except EOFError:
                break
    else:
        assert token is not None
        qe.formatter = DiscordFormatter
        client.run(token)
