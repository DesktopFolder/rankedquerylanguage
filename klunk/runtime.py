from collections import defaultdict
from .extra_types import UUID, Milliseconds, Seconds
from .match import QueryMatch
from .parse_utils import partition_list
from .component import Component
from .expression import Expression
from .dataset import SUPPORTED_ITERABLES, Dataset
from typing import Callable, Any
from . import commands, jobs, splits
from .players import MatchPlayer, PlayerManager
from .parse import parse_boolean
from .utils import average, time_fmt
# Later - this would be nice :)
# from .language import Compiler, Tokenizer


def get_help():
    return """Klunk Beta
Compose a query with pipes (|), commands (filter, sort, etc), and arguments (e.g. `filter winner(DesktopFolder)`)
The dataset of all matches (**up until when the bot was last restarted, auto updating is a WIP**) will be ran through your query. For an example match, see e.g. https://mcsrranked.com/api/matches/350000 - note that the language currently cannot see/manipulate some subsets of this data (most importantly splits).
**By default,** the 'default index' will be autoloaded. This index contains matches **only from the current season** (but not *all* of them, as the bot is unlikely to be up to date, see previous note on auto updating), **only ranked matches**, and **does not include decay matches**. To run your query over previous seasonal data, start your command with `index all|` or `index most|`. `index all|` contains ALL ranked matches, *including cheated matches, unranked matches, and decay matches*, which are unlikely to be useful. Either filter out cheated matches with `filter noabnormal|` or use `index most|`, which contains cross-seasonal data, but without cheated/decay/unranked matches.
Throughout the execution of your 'query pipeline', the dataset can be manipulated, sorted, filtered, etc. At any step, you can use commands like `count` or `attrs` to add output information about the dataset at the current point, which may be useful for understanding the query results.

Important notes:
    - `filter`, `sort`, etc use magic extraction methods to query data. To determine what keys are available to sort/filter/etc on, pipe your data first into `attrs` (e.g. `index some-index | some-commands | attrs`)
    - You can list all available commands with `commands`. However, some may be for debugging (or otherwise unhelpful)
    - You can see help for a specific command with `/query help COMMAND`, assuming I remembered to add it

For examples, see /examples"""

def get_examples():
    return """Examples:
- `index all | filter season(1) type(2) nodecay | count`
  - Uses the `index` command to switch to the `all` index, which has all matches ever recorded.
  - Uses the `filter` command with `season(1)` to remove all non-season-1 matches, `type(2)` to only have ranked matches, and `nodecay` to remove all decay matches (which are automatically created when a player experiences ELO decay)
  - Uses the `count` command to output the resulting number of matches.
- `index s2 | filter seed_type(shipwreck) | extract timelines | segmentby uuid | drop_list empty | splits.has find_bastion | splits.get find_bastion | sort time`
  - Uses the `index` command to switch to the `s2` index. sN indexes contain ranked, non-decay, non-cheated matches from season N, starting with s0.
  - Uses the `filter` command to keep only matches whose seed_type was `shipwreck`.
  - Uses the `extract` command to extract the timelines from these matches. Our dataset is now 'lists of splits from matches' (instead of 'lists of matches')
  - Uses the `segmentby` command to split the lists of 'timelines for each match' into lists of 'timelines for each player-match combination'. This is not confusing, trust me :)
  - Uses the `drop_list empty` command pairing to remove any lists of timelines that are empty. I think this is useless but I left it in, lol.
  - Uses the `splits.has` command to keep only lists of splits where the list contains a find_bastion split.
  - Uses the `splits.get` command to extract the `find_bastion` split out of the list. Our dataset is now 'find_bastion splits' (that is, one large list of all find_bastion splits from season 2)
  - Sorts based on the `time` attribute (which each split object has).
"""


def SmartExtractor(ex, v):
    # Creates and returns a smart extractor for ex
    try:
        ex.extract(v)

        def getter(o):
            return o.extract(v)
        return getter
    except:
        pass
    try:
        _ = ex[v]

        def getter(o):
            return o[v]
        return getter
    except:
        pass
    try:
        _ = ex[0]

        def getter(o):
            return o[int(v)]
        return getter
    except:
        pass
    raise RuntimeError(
        f'Could not find a way to extract {v} from {type(ex)}. Try | attrs.')


def SmartReplacer(ex, a):
    # Creates and returns a smart replacer for ex
    # Basic setattr based stuff.
    def _setattr(o, a, v):
        if a.startswith('_'):
            raise RuntimeError(
                f'Cannot set restricted attribute {a} to value {v} on {o}.')
        setattr(o, a, v)

    # Have to do special things for tuples.
    if type(ex) == tuple:
        i = int(a)

        def tuple_setter(o, v):
            x = list(o)
            x[i] = v
            o = tuple(x)
            return o
        return tuple_setter
    try:
        t = ex.extract(a)
        _setattr(ex, a, t)

        def setter(o, v):
            _setattr(o, a, v)
            return o
        return setter
    except:
        pass
    try:
        t = ex[a]
        ex[a] = t

        def setter(o, v):
            o[a] = v
            return o
        return setter
    except:
        pass
    try:
        t = ex[0]
        ex[0] = t

        def setter(o, v):
            o[int(a)] = v
            return o
        return setter
    except:
        pass
    raise RuntimeError(
        f'Could not find a way to extract {a} from {type(ex)}. Try | attrs.')


class Runtime(Component):
    def __init__(self, datasets: dict[str, Dataset], commands: dict[str, Callable], formatter=None):
        super().__init__("Runtime")

        self.datasets = datasets
        self.commands = commands
        self.formatter = formatter
        self.notes = list()

        alldatalen = len(self.datasets['all'].l)
        if alldatalen < 50000:
            self.notes.append(
                f'Running with total dataset size {alldatalen} - likely in testing mode.')

    def format(self, s, k):
        ff = self.formatter
        if ff is None or k not in ff:
            return s
        return ff[k](s)

    def execute(self, pipeline: list[Expression], parameters):
        # Okay, runtimes can actually be stateful.
        # Wait, no, they can't be. LOL.
        # Eventually we will need stack support. Soooooo
        self.handle_parameters(parameters)

        dataset = self.datasets["default"]

        localfunclist: dict[str, Callable] = {}
        varlist = {}

        # High to low priority.
        # Locals are the lowest priority, and are overwritten otherwise.
        # Is this correct? Idk whatever this is mostly useless segmentation.
        comlists = [splits.COMMANDS, self.commands, commands.basic_commands, localfunclist]

        def lookup_command(name: str):
            for comdict in comlists:
                if name in comdict:
                    return comdict[name]
            return None

        # Now THIS is definitely not correct but whatever lol
        commands.logger = self

        def Local(f: Callable):
            realname = f.__name__[len("local"):]
            localfunclist[realname] = f
            return f

        @Local
        def localvars(_):
            """
            `vars` - list the current dictionary of variables.
            Because variables cannot be set yet, this is useless.
            """
            self.add_result(varlist)

        @Local
        def localdebugecho(_, *args, **kwargs):
            self.add_result(f'Args: {args}, kwargs: {kwargs}')

        @Local
        def localmakelist(_, name: str, item_type: str, *args):
            """
            `makelist(name, item_type, items...)` - create a list variable.
            Supported types: str, int.
            """
            def convert_int(i: str):
                if i.isdecimal(): # safest way to do conversions
                    return int(i)
                raise RuntimeError(f'Could not convert {i} to integer.')
            lc = {'str': lambda x: x, 'int': convert_int}
            if item_type not in lc:
                raise RuntimeError(f'{item_type} is not a valid conversion type. Valid conversion types: {list(lc.keys())}')
            conv = lc[item_type]
            varlist[name] = [conv(a) for a in args]
            # don't return anything - keep current setup.

        @Local
        def localassign(l: Dataset, name: str, attr: str|None = None):
            """
            `assign` - assign to a variable whose name is provided by the first parameter.
            Optionally, the second parameter, attr, may be provided. Instead of assigning the full dataset,
            the values of extract(attr) will be assigned to the variable.
            """
            vals = l.l
            if attr is not None:
                vals = localextract(l, attr)
            varlist[name] = vals
            return l

        @Local
        def localkeepifattrcontained(d: Dataset, attr: str, variable: str):
            if variable not in varlist:
                raise RuntimeError(f'Variable name {variable} does not exist. For a list, see `vars`.')
            s = set(varlist[variable])

            e = SmartExtractor(d.example(), attr)

            return [x for x in d.l if e(x) in s]

        @Local
        def localmetainfo(_):
            """
            `metainfo` - Get some information about the bot/project itself.
            """
            self.add_result("""RankedQueryLanguage is a query system for MCSR matches & players developed by DesktopFolder.
        It is currently in beta & is unlikely to leave that state any time soon.
        The languages used are: Python, Python, and Python.
        To see the codebase/readme/docs(lol), go to: <https://github.com/DesktopFolder/rankedquerylanguage>
        """)

        @Local
        def localindex(_, name: str):
            """
            `index(name)` - Load an index to operate off of. Examples:
            default (the default index) - Current season, ranked, no decay.
            all - All matches, ranked and unranked, including cheated/glitches ones.
            most - All ranked matches ever, with many cheated/glitches ones removed. (no decay)

            Usage example: `index all | filter completed | sort duration | take 5` - top 5 completions of all time.
            """
            self.log(f"Changing dataset to {name}")
            if name.startswith('s') and name[1:].isdecimal():
                return localfilter(localindex(None, 'most'), ('season', name.lstrip('s')))
            if name == 'all':
                self.add_result(f'*Warning: Dataset `all` contains* ***all*** *matches, including decay matches, unranked matches, and cheated matches. `index most` only contains legitimate, ranked, non-decay matches.*')
            if not name in self.datasets:
                raise RuntimeError(f'{name} is not a valid dataset name.')
            return self.datasets[name]

        @Local
        def localcommands(_):
            """
            `commands` - List valid commands.
            A command is the first part of each section of a query.
            For example, in `index default | filter winner(John) | sort`, default/filter/sort are commands.
            """
            lcoms = set()
            for l in comlists:
                for v in l.keys():
                    lcoms.add(v)
            self.add_result(f'Valid commands:', ', '.join(sorted(lcoms)))
            self.add_result(
                'Each query segment must begin with a pipe (|) followed by a command, followed by its arguments. For information on a specific command, use /query help COMMAND')

        @Local
        def localallfuncs(_):
            """
            `allfuncs` - Debugging command for listing all functions, including hidden ones.
            """
            for i, comlist in enumerate(comlists):
                self.add_result(
                    f'Functions with priority {len(comlists)-i}:', ', '.join(comlist.keys()))

        @Local
        def localinfo(_):
            """
            `info` - Gets information on the current dataset being used.
            """
            self.add_result(dataset.info())

        @Local
        def localdetailedinfo(l: Dataset):
            """
            `detailedinfo` - for getting info on the current dataset.
            Mainly added because info is useless. Uh, I should fix that. Anyways...
            """
            self.add_result(l.detailed_info())

        @Local
        def localwait(_, num_seconds):
            """
            `wait(num_seconds)` - wait for some period of time.
            This function is disabled for obvious reasons.
            Try harder.
            """
            pass

        @Local
        def localplayers(l: Dataset):
            """
            `players` - Converts the dataset from a match dataset to a player dataset.
            This changes the datatype that commands operate over.
            """
            return l.clone(list(PlayerManager(l.l).players.values()))

        @Local
        def localexamples(_):
            """
            `examples` - Prints some examples.
            """
            self.add_result(get_examples())

        @Local
        def localdebugsplits(l: Dataset):
            ex = l.example()
            assert type(ex) == QueryMatch
            self.add_result(str(ex.timelines))

        @Local
        def localhelp(_, arg=None):
            """
            `help(command)` - If `command` is not given, prints general help.
            Otherwise, prints help for `command` :)
            """
            if arg is None:
                self.add_result(get_help())
                return
            com = lookup_command(arg)
            if com is None:
                self.add_result(
                    f'{self.format(arg, "tick")} is not a valid command.')
                return
            self.add_result(self.format(com.__doc__, 'doc'))

        @Local
        def localsort(d: Dataset, attribute, **kwargs):
            """
            `sort(attribute)` - Sorts the dataset based on `attribute`. To list attributes, see `help attrs`
            """
            # For now this should do the trick.
            if not d.l:
                return list()
            extractor = SmartExtractor(d.l[0], attribute)
            res = [x for x in d.l if extractor(x) is not None]
            lb = len(d.l)
            la = len(res)
            if la != lb:
                self.notes.append(f'Warning: During sort on {attribute}, {lb - la} '
                                  'items were dropped, as their value was None.')
            return sorted(res, key=lambda x: extractor(x), **kwargs)

        @Local
        def localraw(d: Dataset, *attributes):
            ex = d.example()
            setters = [SmartReplacer(ex, attribute) for attribute in attributes]
            getters = [SmartExtractor(ex, attribute)
                       for attribute in attributes]
            if type(d.l) in [list, set]:
                def do_replacement(o):
                    for s, g in zip(setters, getters):
                        o = s(o, str(g(o)))
                    return o
                return [do_replacement(o) for o in d.l]
            return d.l

        @Local
        def localrsort(l: Dataset, attribute):
            """
            `rsort(attribute)` - Reverse sorts the dataset based on `attribute`. To list attributes, `help attrs`
            """
            # For now this should do the trick.
            return localsort(l, attribute, reverse=True)

        @Local
        def localtake(l: Dataset, *args):
            """
            `take(n)` - Reduce the size of the input data. Examples:
            `take last 5` - Take the last 5 items. `take 3` - Take the first three items. For example, `rsort duration | take 5` gets the 5 slowest runs.
            """
            data = l.l
            args = list(args)
            # ints, sa = partition_list(args, lambda a: type(a) is int)
            # TODO lol
            ints, sa = partition_list(args, lambda a: a.isdigit())
            if len(ints) > 1:
                raise RuntimeError(
                    f'Command `take` got {len(ints)} arguments, requires at most 1.')
            if len(ints) == 0:
                if 'last' in sa:
                    # Special take behaviour.
                    return data[-1]
                return data[0]
            n = int(ints[0])
            if 'last' in sa:
                return data[-1 * n:]
            return data[:n]

        @Local
        def localaverage(l: Dataset, val, *args):
            """
            `average(attribute)` - Compute the average value of an attribute across a dataset.
            Example: `filter completion | sort duration | take 1000 | average duration` gets the average time of the top 1000 completions.
            """
            data = l.l
            if not data:
                return self.add_result(f'Dataset was empty; no average calculable.')
            example = data[0].extract(val)
            if type(example) not in [int, Milliseconds, Seconds, float]:
                return self.add_result(f'Could not average type {type(example)}.')
            result = average([x.extract(val) for x in data])
            if 'time' in args or type(example) in [Milliseconds, Seconds]:
                tf = time_fmt(result, type(example)
                              is Seconds or 'seconds' in args)
                self.add_result(f'Average {val}: {tf}')
            else:
                self.add_result(
                    f'Average {val}: ' + str((result if 'precise' in args else round(result, 2))))

        @Local
        def localaverageby(l: Dataset, to_average: str, by: str):
            """
            `averageby(to_average, by)` - Compute the average value of an attribute across a dataset, by the value of a second attribute.
            Example: `filter completion | sort duration | take 1000 | averageby duration winner` gets the average time of the top 1000 completions by their winner.
            """
            ex = l.example()
            value_extractor = SmartExtractor(ex, to_average)
            key_extractor = SmartExtractor(ex, by)

            t = type(value_extractor(ex))

            avg_dict = defaultdict(lambda: list())

            for o in l.l:
                avg_dict[key_extractor(o)].append(value_extractor(o))
            return [tuple([k, t(average(v))]) for k, v in avg_dict.items()]

        @Local
        def localcount(l: Dataset):
            """
            `count` - Returns the current dataset size.
            """
            if type(l.l) is list:
                self.add_result(f'Current size: {len(l.l)}')
            else:
                self.add_result(f'Dataset currently only has one item.')
            return l

        @Local
        def localextract(l: Dataset, *args):
            """
            `extract(attribute, ...)` - Extract the value of an attribute from all input objects.
            For example, `| extract winner` gets a list that is JUST the names of all winners.
            In programming terms, this turns [Match(winner=x,...), ...] into [x, ...]
            If more than one attribute is supplied, extracts all attributes into a tuple.
            """
            if len(args) == 1:
                return [x.extract(*args) for x in l.l]
            extractors = [SmartExtractor(l.example(), a) for a in args]
            return [tuple(e(x) for e in extractors) for x in l.l]

        @Local
        def localsegmentby(l: Dataset, attribute: str):
            """
            extract(timelines) -> [[Timeline(),...], ]
            extractby(uuid, timelines) -> [[Timeline(), ...], ...]
            """
            ex = None
            for v in l.l:
                if type(v) not in SUPPORTED_ITERABLES:
                    raise RuntimeError(f'Segment by does not support: {type(v)}')
                if len(v) > 0:
                    ex = v[0]
                    break
            if ex is None:
                raise RuntimeError(f'Cannot segment by {attribute} on an empty dataset.')
            # now we have ex as our example value that we are segmenting list of lists on
            extractor = SmartExtractor(ex, attribute)
            newlist = list()
            for sublist in l.l:
                newsublists = dict()
                for item in sublist:
                    v = extractor(item)
                    if v not in newsublists:
                        newsublists[v] = list()
                    newsublists[v].append(item)
                # we now have more lists! maybe
                for newsublist in newsublists.values():
                    newlist.append(newsublist)
            return l.clone(newlist)

        @Local
        def localbetween(d: Dataset, attribute, min_val, max_val):
            """
            `between(attribute, minimum, maximum)` - Filters the dataset to only have objects where 
            min_val <= object.attribute <= max_val. Does floating point comparisons.
            Note: This is a stopgap solution as proper expression parsing is not implemented yet for
            filter expressions. In the future, this will just be `filter attribute<4` or similar.
            """
            l = float(min_val)
            u = float(max_val)

            def is_between(v):
                return v >= l and v <= u
            return [x for x in d.l if is_between(float(x.extract(attribute)))]

        def getslots(e: Any):
            if hasattr(e, '__slots__'):
                return e.__slots__
            return [x for x in dir(e) if not x.startswith('_')]

        @Local
        def localattrs(l: Dataset):
            """
            `attrs` - List the attributes that are available for the current datatype.
            e.g. `attrs` or `players | attrs` are the only cases where you'd want to use this currently.
            """
            example = l.l[0]
            self.add_result(
                f'Known accessible attributes of {type(example)}: ' + ", ".join(getslots(example)))

        @Local
        def localexample(l: Dataset, attribute=None):
            """
            `example(attribute)` - If `attribute` is provided, provides an example value for that attribute. Otherwise,
            provides a full example object layout.
            """
            example = l.l[0]
            if attribute is not None:
                self.add_result(
                    f'Example value of {attribute}: {example.extract(attribute)}')
            else:
                d = dict()
                for k in getslots(example):
                    v = example.extract(k)
                    if type(v) != list:
                        d[k] = v
                    else:
                        d[k] = 'List[...]'
                self.add_result(f'Example object layout: {d}')

        @Local
        def localexampleinfo(l: Dataset):
            inf = list()

            def add_info(n, l, o):
                if isinstance(o, list) or isinstance(o, tuple):
                    inner = list()
                    for i, v in enumerate(o):
                        add_info(str(i), inner, v)
                    l.append(
                        f'<{n}: {type(o)} containing: [' + ', '.join(inner) + ']>')
                    return
                l.append(f'{n}: {type(o)}')
            add_info('Example Object', inf, l.example())
            self.add_result(', '.join(inf))

        @Local
        def localdrop_outliers(d: Dataset, attr: str, factor: str = '4', method = 'diff'):
            """
            drop_high_outliers(attribute, factor=4, method=diff) - drop outliers that are factor* higher than the average
            """
            e = SmartExtractor(d.example(), attr)
            avg = average([e(x) for x in d.l])
            if not factor.isdecimal():
                raise TypeError(f'Factor of {factor} is not an integer number.')
            high_limit = avg + (int(factor) * avg)
            low_limit = avg - (int(factor) * avg)
            methods = {
                'diff': lambda v: v < high_limit and v > low_limit,
                'gt': lambda v: v < high_limit,
                'lt': lambda v: v > low_limit,
            }
            if method not in methods:
                raise ValueError(f'Method {method} is not a valid method, see valid methods: {list(methods.values())}')
            method = methods[method]
            return [x for x in d.l if method(e(x))]

        @Local
        def localdrop_list(d: Dataset, value: str):
            if value == 'empty':
                return d.clone([x for x in d.l if len(x) != 0])
            raise RuntimeError(f'Could not find drop parameter {value}')

        @Local
        def localdrop(d: Dataset, attribute, value):
            """
            `drop(attribute, value)` - Drops any records where attribute is equal to value.
            Use None() to get a value of None (otherwise, 'None' will be the value)
            This is also a temporary solution for filter not being powerful enough.
            Later, it will be possible to just do `filter winner(not(desktopfolder))`
            """
            extractor = SmartExtractor(d.l[0], attribute)
            if type(value) is tuple:
                if value[0] == 'None':
                    value = None
                elif value[0] == 'lt':
                    return [x for x in d.l if x.extract(attribute) >= int(value[1])]
                elif value[0] == 'gt':
                    return [x for x in d.l if x.extract(attribute) <= int(value[1])]
                elif value[0] == 'anylt':
                    a, b = attribute.split('.')
                    return [x for x in d.l if all([y.extract(b) >= int(value[1]) for y in x.extract(a)])]
                elif value[0] == 'test_winner_lower':
                    # lol, ok, whatever, language dev later sometime ig
                    return [m for m in d.l if type(m) == QueryMatch and not m.rql_is_draw() and (m.rql_loser().elo > m.rql_winner().elo)]
            return [x for x in d.l if x.extract(attribute) != value]

        @Local
        def localtest_list(d: Dataset, attribute, operation, destination=None):
            # see filter for more details on how this will work in the future lol
            if operation == 'abs_diff':
                def apply(o, v):
                    if type(o) == tuple:
                        o = list(o)
                    if isinstance(o, list):
                        if destination is None:
                            o.append(v)
                        else:
                            if not destination.isdigit():
                                raise ValueError(
                                    f'Destination {destination} must be integral.')
                            o[int(destination)] = v
                        return o
                    if destination is None:
                        raise RuntimeError(
                            f'Must provide destination for object abs_diffs (t={type(o)})')
                    setattr(o, destination, v)
                    return o

                # ye
                get_values = None
                if '.' in attribute:
                    atts = attribute.split('.')
                    if len(atts) != 2:
                        raise RuntimeError(
                            'cannot currently go more than 2 objects deep, sorry.')
                    a, b = atts
                    ex = d.example()
                    l1 = SmartExtractor(ex, a)
                    l2 = SmartExtractor(l1(ex), b)

                    def get_values_deep(o):
                        return [l2(oval) for oval in l1(o)]
                    get_values = get_values_deep
                else:
                    l1 = SmartExtractor(d.example(), attribute)

                    def get_values_light(o):
                        return l1(o)
                    get_values = get_values_light

                def do_diff(o):
                    vs = get_values(o)
                    return abs(vs[0] - vs[1])

                return [apply(o, do_diff(o)) for o in d.l]

            else:
                raise RuntimeError(f'Unsupported operation: {operation}')

        @Local
        def localfilter(l: Dataset, *args):
            """
            `filter(...)` - Filters the input dataset based on 1 or more filter arguments.
            Filter arguments may be simple functions where equality is desired, for example:
            `filter winner(desktopfolder) loser(mcboyenn)`. For some binary attributes, you
            may also use the simplified filter syntax: `filter noabnormal nodecay ff` is the
            equivalent of `filter is_abnormal(false) is_decay(false) is_ff(true)`
            """
            res: list[Any] = l.l
            for filt in args:
                # Short circuit if we have no objects.
                if not res:
                    self.log(
                        f'Avoided applying filter {filt} and any additional filters (empty result).')
                    break

                # For now, support a bunch of nice boolean autodetection/conversions.
                if type(filt) == str:
                    prefilt = filt
                    b = True
                    if filt.startswith('no'):
                        b = False
                        filt = filt[2:]
                    filt = (f'is_{filt}', b)
                    self.log(
                        f'Found simple boolean filter {prefilt} and converted it to {filt}.')

                if type(filt) == tuple:
                    # Let's be smart about this. Get our desired destination type for conversion.
                    # Is that smart? Whatever, this is a query language built in Python, anyways.
                    first = res[0]
                    varname, desired = filt
                    example = first.extract(varname)
                    if type(example) is bool:
                        desired = parse_boolean(desired)
                    elif type(example) is UUID:
                        try:
                            desired = self.datasets["__users"].l[desired.lower(
                            )]
                        except KeyError:
                            self.notes.append(
                                f'{desired} is not a known username.')
                            desired = None
                    elif type(example) is int:
                        desired = int(desired)
                    preres = len(res)
                    res = [m for m in res if m.extract(varname) == desired]
                    self.log(
                        f'Applied filter {filt} and got {len(res)} resulting objects (from {preres}).')
                    if not res:
                        raise RuntimeError(f'Empty dataset after applying filter: {filt}. Note that filter parameters are case sensitive, so `filter nick(desktopfolder)` is NOT the same as `filter nick(DesktopFolder)`.')
                        # return l.clone(f'Empty dataset after applying filter: {filt}')
                else:
                    raise RuntimeError(
                        f'Unsupported filter type {type(filt)} for filter {filt}')

            return res

        @Local
        def localjob(l, job: tuple[str, str]):
            if type(job) != tuple:
                raise RuntimeError(f'Job {job} was provided without an argument list.')
            return jobs.execute(job, l=l, varlist=varlist)

        def execute_simple(l, fname, args):
            # Executes a command with the listed arguments.
            # Does not (!) evaluate function/expression parameters.
            for comlist in comlists:
                if fname in comlist:
                    self.log(f"Attempting to execute {fname}({args})")
                    return comlist[fname](l, *args)
            raise RuntimeError(
                f"{fname} is not a valid command name. Try `commands` to list valid commands.")

        def try_execute(l, e: Expression):
            if not e.arguments:
                return execute_simple(l, e.command, [])
            if all([type(a) == str for a in e.arguments]):
                return execute_simple(l, e.command, e.arguments)

            # TODO - support functions properly. :) for now no recursion for you, fools! no recursion for anyone! ahahaha
            if all([type(a) in [str, tuple, list] for a in e.arguments]):
                return execute_simple(l, e.command, e.arguments)
            raise RuntimeError(f"Could not execute: {e}")

        while pipeline:
            e = pipeline.pop(0)
            eid = f'Expression@c:{e.loc}'
            self.time(eid)
            res = try_execute(dataset, e)
            # TODO - rolling 'latest dataset metainfo' here for games
            if res is None:
                pass
            elif type(res) in SUPPORTED_ITERABLES:
                dataset = dataset.clone(res)
            else:
                if not type(res) == Dataset:
                    raise RuntimeError(
                        f"Got unhandled result type {type(res)} in {e}")
                dataset = res
            self.log_time(eid)

        self.log("Completed execution. Info:", dataset.info())
        return dataset
