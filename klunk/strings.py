HELP = """Klunk Beta
Compose a query with pipes (|), commands (filter, sort, etc), and arguments (e.g. `filter winner(DesktopFolder)`)
The dataset of all matches (**up until when the bot was last restarted, auto updating is a WIP**) will be ran through your query. For an example match, see e.g. https://mcsrranked.com/api/matches/350000 - note that the language currently cannot see/manipulate some subsets of this data (most importantly splits).
**By default,** the 'default index' will be autoloaded. This index contains matches **only from the current season** (but not *all* of them, as the bot is unlikely to be up to date, see previous note on auto updating), **only ranked matches**, and **does not include decay matches**. To run your query over previous seasonal data, start your command with `index all|` or `index most|`. `index all|` contains ALL ranked matches, *including cheated matches, unranked matches, and decay matches*, which are unlikely to be useful. Either filter out cheated matches with `filter noabnormal|` or use `index most|`, which contains cross-seasonal data, but without cheated/decay/unranked matches.
Throughout the execution of your 'query pipeline', the dataset can be manipulated, sorted, filtered, etc. At any step, you can use commands like `count` or `attrs` to add output information about the dataset at the current point, which may be useful for understanding the query results.

Important notes:
    - `filter`, `sort`, etc use magic extraction methods to query data. To determine what keys are available to sort/filter/etc on, pipe your data first into `attrs` (e.g. `index some-index | some-commands | attrs`)
    - You can list all available commands with `commands`. However, some may be for debugging (or otherwise unhelpful)
    - You can see help for a specific command with `/query help COMMAND`, assuming I remembered to add it

For examples, see /examples"""

EXAMPLES = """Examples:
- `index all | filter season(1) type(2) nodecay | count`
  - Uses the `index` command to switch to the `all` index, which has all matches ever recorded.
  - Uses the `filter` command with `season(1)` to remove all non-season-1 matches, `type(2)` to only have ranked matches, and `nodecay` to remove all decay matches (which are automatically created when a player experiences ELO decay)
  - Uses the `count` command to output the resulting number of matches.
- `index s2 | filter seed_type(shipwreck) | extract timelines | segmentby uuid | drop_list empty | splits.has find_bastion | splits.get find_bastion | sort time`
  - Uses the `index` command to switch to the `s2` index. sN indexes contain ranked, non-decay, non-cheated matches from season N, starting with s0.
  - Uses the `filter` command to keep only matches whose seed_type was `shipwreck`. (other types: ruined_portal, buried_treasure, village, desert_temple)
  - Uses the `extract` command to extract the timelines from these matches. Our dataset is now 'lists of splits from matches' (instead of 'lists of matches')
  - Uses the `segmentby` command to split the lists of 'timelines for each match' into lists of 'timelines for each player-match combination'. This is not confusing, trust me :)
  - Uses the `drop_list empty` command pairing to remove any lists of timelines that are empty. I think this is useless but I left it in, lol.
  - Uses the `splits.has` command to keep only lists of splits where the list contains a find_bastion split.
  - Uses the `splits.get` command to extract the `find_bastion` split out of the list. Our dataset is now 'find_bastion splits' (that is, one large list of all find_bastion splits from season 2)
  - Sorts based on the `time` attribute (which each split object has).
"""

CHANGELOG = """Changelog:
*September 7, 2024*:
- Fixed `winrate` object on Player types. Now compares mostly properly to itself and still extracts properly.
- You can now extract 'wins' or 'losses' on ranked-only datasets to get the actual numbers
*December 24, 2023*:
- Added player extraction to /qb_leaderboard. The first number in outputs, if a player is set in the command, is that player's position on the leaderboard. For /qb_leaderboard pb, this does not really make as much sense, it's mostly useful for the other options currently.
- Added help information to all commands. Access with `/query help command` as always.
- Added `enumerate`. For examples, see `qb_leaderboard` with the player option set. Essentially, tags either match or player objects with their position in the current dataset. By default 1-indexed (for ease of use) but you can opt-in to 0-indexing with `enumerate 0`. Why? I don't know.
*Other Updates*:
- Added `slice` command. See `/help slice`. Essentially, a much more powerful version of `take`, allowing arbitrary positional data extraction.
"""
