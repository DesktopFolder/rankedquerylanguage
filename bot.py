import discord
from discord import app_commands
from io import BytesIO

# leaving this commented in case I want to port this behaviour to v2
#from qb.db import load_raw_matches, to_idx, to_idx_key
#from qb.match import QueryMatch

from klunk import sandbox
from klunk.language import ParseError

MY_GUILD = discord.Object(id=1133544816563716250)  # replace with your guild id
try:
    token = open('token.txt').read().strip()
except:
    token = None

class QueryEngineV2:
    def __init__(self) -> None:
        self.formatter = None

    def clean(self, s: str) -> str:
        if self.formatter is not None:
            return self.formatter["clean"](s)
        return s

    def run(self, query: str, debug: bool=False, timing: bool=False, is_bot: bool=False):
        sb = sandbox.Query(query, debug, timing, self.formatter)

        def format_result(s: str):
            if is_bot and sb.runtime and sb.runtime.notes:
                fmt_notes = "\n".join([f'Note: {self.clean(note)}' for note in sb.runtime.notes])
                return f'{fmt_notes}\n{s}'
            return s
        try:
            result = sb.run()
            if sb._do_upload:
                if sb._result is not None:
                    return sb._result
                if type(result.l) is not list:
                    return format_result('This could not be made into a file.')
                return result.l
            if sb._result is not None:
                return format_result('\n'.join(sb._result))
            if self.formatter is not None:
                # TODO - maybe format discord here idk man
                return format_result(f'{result.summarize()}')
            return format_result(f'{result.summarize()}')
        except ParseError as e:
            extra = "\nIt looks like your query failed to parse. "
            extra += "Try `/query help` to get some examples."
            if sb._tracebacks:
                import traceback
                return format_result(f'{traceback.format_exc().rstrip()}{extra}')
            else:
                return format_result(f'Error: {e}{extra}')
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
    "username": lambda s: s.replace('_', '\\_'),
    "clean": lambda s: s.replace('_', '\\_')
}


@client.event
async def on_ready():
    if client.user is None:
        print(f'Logged in as None?')
    else:
        print(f'Logged in as {client.user} (ID: {client.user.id})')


async def run_discord_query(interaction: discord.Interaction, query: str, notes=None):
    # COC-PYWRITE
    # also doc the above lol 
    # also, check how to remove/rename commands...
    print('Running query:', query)
    await interaction.response.defer(ephemeral = False, thinking = True)
    resp = qe.run(query, False, False, True)
    print('Bot finished running query:', query)
    notes = '' if notes is None else '\n' + '\nNote: '.join(notes)
    try:
        if type(resp) is list:
            # Attempt string conversion. LOL this might be a bad idea.
            s = str()
            one_gb = 2 * 1024 * 1024 * 1024
            from klunk import dataset
            for l in resp:
                s += dataset.format_str(l)
                s += '\n'
                if len(s) > one_gb:
                    await interaction.followup.send(f'From query: `{query}`: Failed to upload, too large (>2gb)')
                    return
            s = s.encode("utf-8")
            await interaction.followup.send(f'From query: `{query}`{notes}', file=discord.File(BytesIO(s), "result.txt"))
        else:
            s = str(resp)
            if len(s) > 2000:
                await interaction.followup.send(f'Your query has a result size of {len(s)} characters, which is too long. Try with +asfile| at the start.')
            else:
                await interaction.followup.send(f'From query: `{query}`:{notes}\n{resp}')
    except Exception as e:
        print(f'Failed to send response to /query - likely it took too long: {e}.')


@client.tree.command()
@app_commands.describe(
    username='The CASE-SENSITIVE username of the player to get a current-season average completion time for.'
)
async def average_completion(interaction: discord.Interaction, username: str):
    await run_discord_query(interaction, f'players | filter nick({username}) | extract nick average_completion')


@client.tree.command()
@app_commands.describe(
    query='Your Ranked query string. See #docs for details.',
)
async def query(interaction: discord.Interaction, query: str):
    # Lint level one: Query level.
    import re
    lints = [(r'players\s*|\s*filter\s*nick\([\w\s]*\)\s*|\s*extract (nick )?average_completion\s*$', '*Note: /average\\_completion has been added to simplify this.*')]
    notes = None
    for rxp, res in lints:
        if re.match(rxp, query) is not None:
            notes = notes or list()
            notes.append(res)
    await run_discord_query(interaction, query, notes)


if __name__ == '__main__':
    import sys
    a = sys.argv[1:]
    if '--preload' in a:
        sandbox.Query("+debug timing tb | testlog 'Preloaded data.'").run()
        print('Finished preloading. Starting up...')
    if '--fake' in a:
        db = False
        try:
            import readline # pyright: ignore
        except:
            print('Warning: Was unable to import the readline module. '
                  'To get readline support on windows, try `pip install pyreadline`')
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
