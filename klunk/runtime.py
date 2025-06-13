from collections import defaultdict
from .extra_types import UUID, Milliseconds, Seconds, is_numeric
from .match import QueryMatch
from.parse_utils import partition_list
from .component import Component
from .expression import Expression
from .dataset import SUPPORTED_ITERABLES, Dataset, UUIDDataset
from typing import Callable, Any
from . import commands, jobs, splits
from .players import MatchPlayer, PlayerManager
from .parse import parse_boolean
from .utils import average, median, time_fmt, Percentage
from .strings import HELP, CHANGELOG, EXAMPLES

# Later - this would be nice :)
# from .language import Compiler, Tokenizer

def is_extract_failure(val):
    if isinstance(val, type):
        from .match import ExtractFailure as mef
        from .players import ExtractFailure as pef
        return val == mef or val == pef
    return False

def BasicExtractor(ex, v):
    from .match import ExtractFailure as mef
    from .players import ExtractFailure as pef
    # Creates and returns a smart extractor for ex

    # v can be a list of arguments to a function.
    # in that case it will be a tuple.
    if isinstance(v, tuple):
        if len(v) != 2:
            raise RuntimeError(f'Could not figure out extraction of {v}')
        v, args = v[0], v[1].split(',')
    else:
        args = []

    # Does it have .extract()?
    try:
        example = ex.extract(v, *args)
        if is_extract_failure(example):
            raise RuntimeError(f'Cannot extract attribute {v} from type {type(ex)}. Try `| attrs` to see available attributes at any point in a command.')

        def getter(o):
            return o.extract(v, *args)

        return getter, example
    except:
        if 'extract' in dir(ex):
            raise
        pass

    # Does it have __getitem__(T)?
    try:
        example = ex[v]

        def getter(o):
            return o[v]

        return getter, example
    except:
        pass

    # Does it have __getitem__(int)?
    try:
        example = ex[0]

        def getter(o):
            return o[int(v)]

        return getter, example
    except:
        pass

    # :(
    raise RuntimeError(f"Could not find a way to extract {v} from {type(ex)}. Try | attrs.")


def SmartExtractor(ex, v, *args):
    # Creates and returns a smart extractor for ex
    extractor, example = BasicExtractor(ex, v)

    if isinstance(example, Callable) and v.startswith("rql_"):
        return lambda o: extractor(o)(*args)

    return extractor

def NonNullApplicator(extractor, func, iterable):
    for x in iterable:
        val = extractor(x)
        if val is not None:
            yield func(val)

def NonNullKeeper(extractor, func, iterable):
    for x in iterable:
        val = extractor(x)
        if val is not None and (func is None or func(val)):
            yield val

def AutoExtractor(d: Dataset, attribute: str, *args, no_none=True, allowed=None) -> tuple[Callable, type]:
    """
    Generates a Callable C(o) which returns o->attribute.
    Also simultaneously returns the type that the extractor extracts.
    Arguments:
      allowed -> Constrain the type of the extracted value.
      no_none -> Raise an error if we couldn't find a good value.
    """
    # First, make the extractor.
    example = d.example()
    extractor = SmartExtractor(example, attribute, *args)

    for instance in d.iter():
        extracted = extractor(instance)
        if extracted is not None:
            t = type(extracted)
            if allowed is not None:
                if not any([isinstance(t(), oktype) for oktype in allowed]):
                    raise RuntimeError(f'Could not extract {attribute} from {type(example)}: type {t} is not a permitted type ({allowed})')
            return (extractor, t)

    if no_none:
        raise RuntimeError(f'Could not determine how to extract {attribute} from {type(example)}: all instances of this attribute are None.')
    if allowed is not None and None not in allowed:
        raise RuntimeError(f'Could not extract {attribute} from {type(example)}: type None is not a permitted type ({allowed})')
    return (extractor, type(None))


def FullExtractor(d: Dataset, attribute: str, *args, drop_none=True, **kwargs):
    """
    A full-featured extractor that also yields over the results.
    You don't have to write logic! That's great.
    Not helpful if you want other things than just the extracted value.
    """
    e, t = AutoExtractor(d, attribute, *args, **kwargs)
    for val in d.l:
        res = e(val)
        if res is not None:
            yield t(res)

def FullExtractorWithO(d: Dataset, attribute: str, *args, **kwargs):
    """
    A full-featured extractor that also yields over the results.
    You don't have to write logic! That's great.
    Not helpful if you want other things than just the extracted value.
    """
    e, t = AutoExtractor(d, attribute, *args, **kwargs)
    for val in d.l:
        res = e(val)
        if res is not None:
            yield (t(res), val)

class TypedExtractor:
    def __init__(self, *args, **kwargs) -> None:
        self.extractor, self.result_type = AutoExtractor(*args, **kwargs) 

    def get(self, o):
        return self.result_type(self.extractor(o))

    def valid(self, o):
        return self.extractor(o) is not None


def SmartReplacer(ex, a):
    # Creates and returns a smart replacer for ex
    # Basic setattr based stuff.
    def _setattr(o, a, v):
        if a.startswith("_"):
            raise RuntimeError(f"Cannot set restricted attribute {a} to value {v} on {o}.")
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
    raise RuntimeError(f"Could not find a way to extract {a} from {type(ex)}. Try | attrs.")


class Runtime(Component):
    def __init__(self, datasets: dict[str, Dataset], commands: dict[str, Callable], formatter=None):
        super().__init__("Runtime")

        self.datasets = datasets
        self.user_dataset = UUIDDataset(self.datasets["__users"], self.datasets["__uuids"])
        self.commands = commands
        self.formatter = formatter
        self.notes = list()

        alldatalen = len(self.datasets["all"].l)
        if alldatalen < 50000:
            self.notes.append(f"Running with total dataset size {alldatalen} - likely in testing mode.")

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

        def Local(**kwargs):
            def LFC(f: Callable):
                realname = f.__name__[len("local") :]
                f = commands.Executor(f, **kwargs)
                localfunclist[realname] = f
                return f
            return LFC

        @Local(print_dataset=False)
        def localvars(_):
            """
            `vars` - list the current dictionary of variables.
            """
            self.add_result(varlist)

        @Local(print_dataset=False)
        def localchangelog(_):
            """
            `changelog` - Print relevant recent changes to the bot.
            This is not automatic, so it's just things I thought were cool or important.
            """
            self.add_result(CHANGELOG)
    

        @Local()
        def localdebugecho(_, *args, **kwargs):
            """
            `debugecho` - Gives you information on how arguments are seen. You do not need this.
            """
            self.add_result(f"Args: {args}, kwargs: {kwargs}")

        @Local(print_dataset=False)
        def localvalidate(_):
            """
            `validate` - Completely useless to you. Currently.
            """
            for i, comlist in enumerate(comlists):
                for executor in comlist.values():
                    if not isinstance(executor, commands.Executor):
                        print(f'Bad command: {executor} in list {i}')
                    elif executor.help is None:
                        print(f'Function with no help info: {executor.func}')

        @Local()
        def localmakelist(_, name: str, item_type: str, *args):
            """
            `makelist(name, item_type, items...)` - create a list variable.
            Supported types: str, int.
            """

            def convert_int(i: str):
                if i.isdecimal():  # safest way to do conversions
                    return int(i)
                raise RuntimeError(f"Could not convert {i} to integer.")

            lc = {"str": lambda x: x, "int": convert_int}
            if item_type not in lc:
                raise RuntimeError(f"{item_type} is not a valid conversion type. Valid conversion types: {list(lc.keys())}")
            conv = lc[item_type]
            varlist[name] = [conv(a) for a in args]
            # don't return anything - keep current setup.

        @Local()
        def localassign(l: Dataset, name: str, attr: str | None = None):
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

        @Local()
        def localapplymappeddata(d: Dataset, name: str, by: str):
            """
            `applymappeddata(name, by)` - Assumes that `name` refers to a variable (created with `assign`)
            which is a list of 2-tuples or otherwise convertible to a dictionary.
            Then, applies the *values* of the 2-tuples to each object in the dataset
            if object.`by` matches the *key* of the 2-tuple.
            Example: If the dataset has a `Player` with `uuid = 123abc`, and you have the variable
            `uuidlist` that looks like `[('123abc', '64')]`, then `applymappeddata uuidlist uuid`
            would give that `Player` object a 'dynamically assigned value' of `64`.
            Currently, only one dynamically assigned value can be given at once. This is a runtime limitation.
            This is useful if you want to associate two different pieces of data together.
            Example: `index most | filter noff | drop duration gt(600000) | extract winner | count_uniques | assign subx | index most | players | applymappeddata subx uuid | extract uuid rql_dynamic rql_completions | drop 1 None() | rsort 1`
            With explanation here: https://discord.com/channels/1056779246728658984/1074343944822992966/1187206790149058671
            """
            example = d.example()
            if not hasattr(example, "dynamic"):
                raise RuntimeError(
                    f"Type {type(example)} does not have dynamic data storage available. You may want to convert to the Player type with `| players`."
                )

            ex = SmartExtractor(example, by)

            mapping = dict(varlist[name])
            has_valid = False
            for o in d.l:
                o.dynamic["default"] = mapping.get(ex(o))
                if not has_valid and o.dynamic["default"] is not None:
                    has_valid = True
            if not has_valid:
                self.add_result(
                    f"Warning: During applymappeddata, all data applied was None, which may indicate a problem. The key type of your mapping is: {type(varlist[name][0][0])} (for example, '{varlist[name][0][0]}'. The type of {by} is {type(ex(example))} (for example, '{ex(example)}')"
                )

        @Local()
        def localkeepifattrcontained(d: Dataset, attr: str, variable: str):
            """
            `keepifattrcontained(attr, variable)` - Essentially, for each object in the dataset,
            checks if `object.attr` is present within the variable `variable`.
            This can be used to do multi-filtering with `makelist`, for example if you wanted
            to create a list of players that you want 'any of the victories from'.
            """
            if variable not in varlist:
                raise RuntimeError(f"Variable name {variable} does not exist. For a list, see `vars`.")
            s = set(varlist[variable])

            e = SmartExtractor(d.example(), attr)
            example = e(d.example())
            if isinstance(example, list):
                # Quick fix for now.
                return [o for o in d.l if any([inner in s for inner in e(o)])]

            res = [x for x in d.l if e(x) in s]
            if not res:
                self.add_result(f"Warning: During keepifattrcontained({attr}, {str}), the resulting dataset was empty.")
            return res

        @Local(print_dataset=False)
        def localmetainfo(_):
            """
            `metainfo` - Get some information about the bot/project itself.
            """
            self.add_result(
                """RankedQueryLanguage is a query system for MCSR matches & players developed by DesktopFolder.
        It is currently in beta & is unlikely to leave that state any time soon.
        The languages used are: Python, Python, and Python.
        To see the codebase/readme/docs(lol), go to: <https://github.com/DesktopFolder/rankedquerylanguage>
        """
            )

        @Local()
        def localindex(_, name: str):
            """
            `index(name)` - Load an index to operate off of. Examples:
            default (the default index) - Current season, ranked, no decay.
            all - All matches, ranked and unranked, including cheated/glitches ones.
            most - All ranked matches ever, with many cheated/glitches ones removed. (no decay)

            Usage example: `index all | filter completed | sort duration | take 5` - top 5 completions of all time.
            """
            self.log(f"Changing dataset to {name}")
            if name.startswith("s") and name[1:].isdecimal():
                return localfilter(localindex(None, "most"), ("season", name.lstrip("s")))
            if name == "all":
                self.add_result(
                    f"*Warning: Dataset `all` contains* ***all*** *matches, including decay matches, unranked matches, and cheated matches. `index most` only contains legitimate, ranked, non-decay matches.* **No datasets contain very old matches due to RAM limitations. See `index all | extract season | count_uniques`.**"
                )
            if not name in self.datasets:
                raise RuntimeError(f"{name} is not a valid dataset name.")
            return self.datasets[name]

        @Local(print_dataset=False)
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
            self.add_result(f"Valid commands:", ", ".join(sorted(lcoms)))
            self.add_result(
                "Each query segment must begin with a pipe (|) followed by a command, followed by its arguments. For information on a specific command, use /query help COMMAND"
            )

        @Local(print_dataset=False)
        def localallfuncs(_):
            """
            `allfuncs` - Debugging command for listing all functions, including hidden ones.
            """
            for i, comlist in enumerate(comlists):
                self.add_result(f"Functions with priority {len(comlists)-i}:", ", ".join(comlist.keys()))

        @Local(print_dataset=False)
        def localinfo(_):
            """
            `info` - Gets information on the current dataset being used.
            """
            self.add_result(dataset.info())

        @Local(print_dataset=False)
        def localdetailedinfo(l: Dataset):
            """
            `detailedinfo` - for getting info on the current dataset.
            Mainly added because info is useless. Uh, I should fix that. Anyways...
            """
            self.add_result(l.detailed_info())

        @Local()
        def localwait(_, num_seconds):
            """
            `wait(num_seconds)` - wait for some period of time.
            This function is disabled for obvious reasons.
            Try harder.
            """
            pass

        @Local()
        def localplayers(l: Dataset, *args):
            """
            `players` - Converts the dataset from a match dataset to a player dataset.
            This changes the datatype that commands operate over. Some autofilters may be applied:
            - lowff(n): Filters out players with high forfeit rates (> 10% by default)
            - manygames(n): Filters out players with low matches played (< 100 by default)
            - opponent_above(n) : Adds win % against a certain elo or above
            """
            lowff = None
            manygames = None

            misc = list()
            for arg in args:
                if type(arg) == tuple:
                    arg, n = arg
                    n = int(n)
                else:
                    n = None
                a = arg.lower()
                if a == 'lowff':
                    lowff = n or 10
                elif a == 'manygames':
                    manygames = n or 100
                elif a == 'opponent_above':
                    misc.append((a, n or 2000))
                else:
                    raise RuntimeError(f'Invalid argument: {a} (see `help players`)')

            res = l.clone(list(PlayerManager(l.l, no_unranked=not l.has_unranked, args=misc).players.values()))
            
            if lowff is not None:
                res = res.clone([p for p in res.l if not p.rql_is_highff(lowff)])
            elif manygames:
                res = res.clone([p for p in res.l if p.summed(p.played_per) >= manygames])

            return res

        @Local(print_dataset=False)
        def localexamples(_):
            """
            `examples` - Prints some examples.
            """
            self.add_result(EXAMPLES)

        @Local(print_dataset=False)
        def localdebugsplits(l: Dataset):
            """
            `debugsplits` - More debugging. Carry on, friend.
            """
            ex = l.example()
            assert type(ex) == QueryMatch
            self.add_result(str(ex.timelines))

        @Local(print_dataset=False)
        def localhelp(_, arg=None):
            """
            `help(command)` - If `command` is not given, prints general help.
            Otherwise, prints help for `command` :)
            """
            if arg is None:
                self.add_result(HELP)
                return
            com = lookup_command(arg)
            if com is None:
                self.add_result(f'{self.format(arg, "tick")} is not a valid command.')
                return
            if com.help is None:
                self.add_result(f'Sorry, {self.format(arg, "tick")} does not have a help string yet.')
                return
            self.add_result(self.format(com.help, "doc"))

        @Local()
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
                self.notes.append(
                    f"Warning: During sort on {attribute}, {lb - la} " "items were dropped, as their value was None."
                )
            return sorted(res, key=lambda x: extractor(x), **kwargs)

        @Local()
        def localraw(d: Dataset, *attributes):
            """
            `raw` - Turns strongly typed things into their string representations.
            Useful if you want to print UUIDs or timestamps for debugging or other purposes.
            (Otherwise, UUIDs or timestamps etc are always nicely formatted on printing)
            Generally disabled except for if you are operating on a list of tuples.
            """
            ex = d.example()
            if type(ex) != tuple:
                raise RuntimeError('Sorry, `raw` currently only works on tuples. Try extracting then doing raw [index].')
            setters = [SmartReplacer(ex, attribute) for attribute in attributes]
            getters = [SmartExtractor(ex, attribute) for attribute in attributes]
            if type(d.l) in [list, set]:

                def do_replacement(o):
                    for s, g in zip(setters, getters):
                        o = s(o, str(g(o)))
                    return o

                return [do_replacement(o) for o in d.l]
            return d.l

        @Local()
        def localround(d: Dataset, *attributes):
            """
            `round` - Rounds some data. Can only work on tuples.
            """
            ex = d.example()
            if type(ex) != tuple:
                raise RuntimeError('Sorry, data modification currently only works on tuples. Try extracting then doing raw [index].')
            setters = [SmartReplacer(ex, attribute) for attribute in attributes]
            getters = [SmartExtractor(ex, attribute) for attribute in attributes]
            if type(d.l) in [list, set]:

                def do_replacement(o):
                    for s, g in zip(setters, getters):
                        o = s(o, int(g(o)))
                    return o

                return [do_replacement(o) for o in d.l]
            return d.l

        @Local()
        def localsubtract(d: Dataset, attr1, attr2):
            """
            `subtract(a, b)` - Essentially, (a - b) is added to the tuples passed in.
            """
            ex = d.example()
            if type(ex) != tuple:
                raise RuntimeError('Sorry, data modification currently only works on tuples. Try extracting then doing raw [index].')
            first = SmartExtractor(ex, attr1)
            second = SmartExtractor(ex, attr2)
            if type(d.l) in [list, set]:

                def do_replacement(o: tuple):
                    return o + ((first(o) - second(o)), )

                return [do_replacement(o) for o in d.l]
            return d.l

        @Local()
        def localround(d: Dataset, *attributes):
            """
            `round` - Rounds some data. Can only work on tuples.
            """
            ex = d.example()
            if type(ex) != tuple:
                raise RuntimeError('Sorry, data modification currently only works on tuples. Try extracting then doing raw [index].')
            setters = [SmartReplacer(ex, attribute) for attribute in attributes]
            getters = [SmartExtractor(ex, attribute) for attribute in attributes]
            if type(d.l) in [list, set]:

                def do_replacement(o):
                    for s, g in zip(setters, getters):
                        o = s(o, abs(g(o)))
                    return o

                return [do_replacement(o) for o in d.l]
            return d.l

        @Local()
        def localrsort(l: Dataset, attribute):
            """
            `rsort(attribute)` - Reverse sorts the dataset based on `attribute`. To list attributes, `help attrs`
            """
            # For now this should do the trick.
            return localsort(l, attribute, reverse=True)

        @Local()
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
                raise RuntimeError(f"Command `take` got {len(ints)} arguments, requires at most 1.")
            if len(ints) == 0:
                if "last" in sa:
                    # Special take behaviour.
                    return data[-1]
                return data[0]
            n = int(ints[0])
            if "last" in sa:
                return data[-1 * n :]
            return data[:n]

        @Local()
        def localslice(l: Dataset, *args):
            """
            `slice(expr)` - Return dataset[expr], where expr is a Python-style slice (only x:y style, no step yet)
            Example: `| slice 4:10` returns dataset[4:10]. `| slice [1:-1]` removes the first and last element.
            Due to a current parsing limitation, prefix with 0: if you want to go from the start. Starting with :
            is not supported at present.
            """
            args = list(args)
            if len(args) != 1:
                raise RuntimeError(f'Slice takes exactly one argument, a slice expression (e.g. 4: or 3:5 or 0:10')
            b, _, a = args[0].partition(':')
            b = 0 if not b else int(b)
            if a:
                return l.l[b:int(a)]
            return l.l[b:]

        @Local(print_dataset=False)
        def localaverage(l: Dataset, val, *args):
            """
            `average(attribute)` - Compute the average value of an attribute across a dataset.
            Example: `filter completion | sort duration | take 1000 | average duration` gets the average time of the top 1000 completions.
            """
            if not l.l:
                return self.add_result(f"Dataset was empty; no average calculable.")
            extractor, t = AutoExtractor(l, val)
            if not is_numeric(t):
                return self.add_result(f"Could not average type {t}.")
            result = average([x for x in NonNullKeeper(extractor, None, l.l)])
            if "time" in args or t in [Milliseconds, Seconds]:
                tf = time_fmt(result, t is Seconds or "seconds" in args)
                self.add_result(f"Average {val}: {tf}")
            else:
                self.add_result(f"Average {val}: " + str((result if "precise" in args else round(result, 2))))

        @Local(print_dataset=False)
        def localmedian(l: Dataset, val, *args):
            """
            `median(attribute)` - Compute the median value of an attribute across a dataset.
            Example: `filter completion | sort duration | take 1000 | average duration` gets the median time of the top 1000 completions.
            """
            if not l.l:
                return self.add_result(f"Dataset was empty; no median calculable.")
            extractor, t = AutoExtractor(l, val)
            if not is_numeric(t):
                return self.add_result(f"Could not get median of type {t}.")
            result = median([x for x in NonNullKeeper(extractor, None, l.l)])
            if "time" in args or t in [Milliseconds, Seconds]:
                tf = time_fmt(result, t is Seconds or "seconds" in args)
                self.add_result(f"Median {val}: {tf}")
            else:
                self.add_result(f"Median {val}: " + str((result if "precise" in args else round(result, 2))))

        @Local(print_dataset=False)
        def localsum(l: Dataset, val: str):
            res = sum(FullExtractor(l, val, allowed=[int, float]))

            self.add_result(f'Sum {val} (over {len(l.l)} objects): {res}')

        @Local()
        def localaverageby(l: Dataset, to_average: str, by: str):
            """
            `averageby(to_average, by)` - Compute the average value of an attribute across a dataset, by the value of a second attribute.
            Example: `filter completion | sort duration | take 1000 | averageby duration winner` gets the average time of the top 1000 completions by their winner.
            """
            ex = l.example()
            value_extractor, vt = AutoExtractor(l, to_average)
            key_extractor, kt = AutoExtractor(l, by)

            avg_dict = defaultdict(lambda: list())

            for o in l.l:
                avg_dict[key_extractor(o)].append(value_extractor(o))
            return [tuple([k, vt(average(v))]) for k, v in avg_dict.items()]

        @Local(print_dataset=False)
        def localcount(l: Dataset):
            """
            `count` - Returns the current dataset size.
            """
            if type(l.l) is list:
                self.add_result(f"Current size: {len(l.l)}")
            else:
                self.add_result(f"Dataset currently only has one item.")
            return l

        @Local()
        def localextract(l: Dataset, *args):
            """
            `extract(attribute, ...)` - Extract the value of an attribute from all input objects.
            For example, `| extract winner` gets a list that is JUST the names of all winners.
            In programming terms, this turns [Match(winner=x,...), ...] into [x, ...]
            If more than one attribute is supplied, extracts all attributes into a tuple.
            """
            if len(args) == 1:
                extractor = SmartExtractor(l.example(), *args)
                return [extractor(x) for x in l.l]
            extractors = [SmartExtractor(l.example(), a) for a in args]
            return [tuple(e(x) for e in extractors) for x in l.l]

        @Local()
        def localsegmentby(l: Dataset, attribute: str):
            """
            extract(timelines) -> [[Timeline(),...], ]
            extractby(uuid, timelines) -> [[Timeline(), ...], ...]
            """
            ex = None
            for v in l.l:
                if type(v) not in SUPPORTED_ITERABLES:
                    raise RuntimeError(f"Segment by does not support: {type(v)}")
                if len(v) > 0:
                    ex = v[0]
                    break
            if ex is None:
                raise RuntimeError(f"Cannot segment by {attribute} on an empty dataset.")
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
            
        def localop(d: Dataset, attribute, by=None, f=max):
            # wtf does this do?
            if by is None:
                # THIS IS SO COOL.
                t = TypedExtractor(d, attribute, allowed=[Milliseconds, Seconds, float, int])
                result = f([y for y in d.l if t.valid(y)], key=t.get)
                return d.clone([result])
            res = dict()
            byextractor, t = AutoExtractor(d, by)
            for extracted_value, full_object in FullExtractorWithO(d, attribute, allowed=[float, int]):
                key = byextractor(full_object)
                if key not in res or res[key] < extracted_value:
                    res[key] = extracted_value
            return d.clone(list(res.items()))

        @Local()
        def localmax(d: Dataset, attribute, by=None):
            """
            `max(attribute, by=None)` - computes the maximum value of an attribute.
            If `by` is left default, the result of this operation is the full object
            that contains the maximum attribute, e.g: `max elo` -> MatchMember(Feinberg, ...)
            If `by` is set to another value, the result of this operation is a list
            of tuples (by, maxvalue), e.g.: `max elo uuid` -> [(UUID, theirMax), ...]
            This might change in the future when typing is normalized for this language.
            """
            return localop(d, attribute, by, max)

        @Local()
        def localmin(d: Dataset, attribute, by=None):
            """
            `min(attribute, by=None)` - Like `help max` but minimum values instead.
            """
            return localop(d, attribute, by, min)

        @Local()
        def localbetween(d: Dataset, attribute, min_val, max_val):
            """
            `between(attribute, minimum, maximum)` - Filters the dataset to only have objects where
            min_val <= object.attribute <= max_val. Does floating point comparisons.
            Note: This is a stopgap solution as proper expression parsing is not implemented yet for
            filter expressions. In the future, this will just be `filter attribute<4` or similar.
            """
            l = float(min_val)
            u = float(max_val)
            extractor = SmartExtractor(d.example(), attribute)

            def is_between(v):
                return v >= l and v <= u

            return [x for x in NonNullKeeper(extractor, lambda val: is_between(float(val)), d.l)]

        def getslots(e: Any):
            if hasattr(e, "__slots__"):
                return e.__slots__
            return [x for x in dir(e) if not x.startswith("_")]

        @Local(print_dataset=False)
        def localattrs(l: Dataset):
            """
            `attrs` - List the attributes that are available for the current datatype.
            e.g. `attrs` or `players | attrs` are the only cases where you'd want to use this currently.
            """
            example = l.l[0]
            self.add_result(f"Known accessible attributes of {type(example)}: " + ", ".join(getslots(example)))

        @Local(print_dataset=False)
        def localexample(l: Dataset, attribute=None):
            """
            `example(attribute)` - If `attribute` is provided, provides an example value for that attribute. Otherwise,
            provides a full example object layout.
            """
            example = l.l[0]
            if attribute is not None:
                self.add_result(f"Example value of {attribute}: {example.extract(attribute)}")
            else:
                d = dict()
                for k in getslots(example):
                    v = example.extract(k)
                    if type(v) != list:
                        d[k] = v
                    else:
                        d[k] = "List[...]"
                self.add_result(f"Example object layout: {d}")

        @Local(print_dataset=False)
        def localexampleinfo(l: Dataset):
            """
            `exampleinfo` - Creates example information, what more do you want?
            """
            inf = list()

            def add_info(n, l, o):
                if isinstance(o, list) or isinstance(o, tuple):
                    inner = list()
                    for i, v in enumerate(o):
                        add_info(str(i), inner, v)
                    l.append(f"<{n}: {type(o)} containing: [" + ", ".join(inner) + "]>")
                    return
                l.append(f"{n}: {type(o)}")

            add_info("Example Object", inf, l.example())
            self.add_result(", ".join(inf))


        @Local()
        def localrequire(d: Dataset, attr: str):
            e = SmartExtractor(d.example(), attr)
            return d.clone([x for x in d.l if e(x) is not None])

        @Local()
        def localdrop_outliers(d: Dataset, attr: str, factor: str = "4", method="diff"):
            """
            drop_high_outliers(attribute, factor=4, method=diff) - drop outliers that are factor* higher than the average
            """
            e = SmartExtractor(d.example(), attr)
            avg = average([e(x) for x in d.l])
            if not factor.isdecimal():
                raise TypeError(f"Factor of {factor} is not an integer number.")
            high_limit = avg + (int(factor) * avg)
            low_limit = avg - (int(factor) * avg)
            methods = {
                "diff": lambda v: v < high_limit and v > low_limit,
                "gt": lambda v: v < high_limit,
                "lt": lambda v: v > low_limit,
            }
            if method not in methods:
                raise ValueError(f"Method {method} is not a valid method, see valid methods: {list(methods.values())}")
            method = methods[method]
            return [x for x in d.l if method(e(x))]

        @Local()
        def localdrop_list(d: Dataset, value: str):
            """
            `drop_list(value)` - If `value` is `"empty"`, filters the dataset to remove any objects
            where `len(object) == 0`.
            Otherwise, fails unhelpfully.
            """
            if value == "empty":
                return d.clone([x for x in d.l if len(x) != 0])
            raise RuntimeError(f"Could not find drop parameter {value}")

        @Local()
        def localh2h(d: Dataset, varname: str):
            """
            `h2h(varname)` - `varname` must point to a list of UUIDs. Generates a list of h2h matches.
            Must be used with +asfile. List of UUIDs must be <=32 long.
            """
            if varname not in varlist:
                raise RuntimeError(f'{varname} is not an existing variable. See `makelist`, `assign`')
            l = varlist[varname]
            if len(l) > 32:
                raise RuntimeError(f'{varname} is a list that is {len(l)} long. Must be <= 32 long.' +
                                     ' Contact DesktopFolder if you have a special use case.')
            from .h2h import generate
            return generate(d, l, nickmap=self.user_dataset.uuids_to_users)


        @Local()
        def localdrop(d: Dataset, attribute, value):
            """
            `drop(attribute, value)` - Drops any records where attribute is equal to value.
            Use None() to get a value of None (otherwise, 'None' will be the value)
            This is also a temporary solution for filter not being powerful enough.
            Later, it will be possible to just do `filter winner(not(desktopfolder))`
            """
            extractor, t = AutoExtractor(d, attribute, no_none=False)
            if t() is None:
                raise RuntimeError(f'All values for {attribute} are `None` in | drop {attribute} {value}')
            if not any([isinstance(t(), oktype) for oktype in [int, float, str, Percentage]]):
                raise RuntimeError(f'Comparisons to {t} in drop {attribute} are not supported yet.')
            if isinstance(value, tuple):
                if value[1] != '':
                    value = (value[0], t(value[1]))
                else:
                    value = (value[0], None)
            else:
                value = t(value)
            if type(value) is tuple:
                if value[0] == "None":
                    value = None
                elif value[0] == "lt":
                    return [x for x in d.l if extractor(x) >= value[1]]
                elif value[0] == "gt":
                    return [x for x in d.l if extractor(x) <= value[1]]
                elif value[0] == "anylt":
                    a, b = attribute.split(".")
                    return [x for x in d.l if all([y.extract(b) >= value[1] for y in extractor(x)])]
                elif value[0] == "test_winner_lower":
                    # lol, ok, whatever, language dev later sometime ig
                    return [
                        m
                        for m in d.l
                        if type(m) == QueryMatch and not m.rql_is_draw() and (m.rql_loser().elo > m.rql_winner().elo)
                    ]
            return [x for x in d.l if extractor(x) != value]

        @Local()
        def localtest_list(d: Dataset, attribute, operation, destination=None):
            """
            `test_list` - Frankly, it says test in the name, I'm not going to bother figuring out what it does for you.
            """
            # see filter for more details on how this will work in the future lol
            if operation == "abs_diff":

                def apply(o, v):
                    if type(o) == tuple:
                        o = list(o)
                    if isinstance(o, list):
                        if destination is None:
                            o.append(v)
                        else:
                            if not destination.isdigit():
                                raise ValueError(f"Destination {destination} must be integral.")
                            o[int(destination)] = v
                        return o
                    if destination is None:
                        raise RuntimeError(f"Must provide destination for object abs_diffs (t={type(o)})")
                    setattr(o, destination, v)
                    return o

                # ye
                get_values = None
                if "." in attribute:
                    atts = attribute.split(".")
                    if len(atts) != 2:
                        raise RuntimeError("cannot currently go more than 2 objects deep, sorry.")
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
                raise RuntimeError(f"Unsupported operation: {operation}")

        @Local()
        def localfilter2(l: Dataset, *args):
            """
            `filter2(...)` - Just DesktopFolder testing things out. Language probably
            needs better parsing at a lower level but this will do a few things for now.
            """
            pass

        @Local()
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
                    self.log(f"Avoided applying filter {filt} and any additional filters (empty result).")
                    break

                # For now, support a bunch of nice boolean autodetection/conversions.
                if type(filt) == str:
                    prefilt = filt
                    b = True
                    if filt.startswith("no"):
                        b = False
                        filt = filt[2:]
                    filt = (f"is_{filt}", b)
                    self.log(f"Found simple boolean filter {prefilt} and converted it to {filt}.")

                if type(filt) == tuple:
                    # Let's be smart about this. Get our desired destination type for conversion.
                    # Is that smart? Whatever, this is a query language built in Python, anyways.
                    first = res[0]
                    # varname(desired)
                    # e.g. player(john)
                    varname, desired = filt
                    example = first.extract(varname)

                    def filter_function(li: Any) -> bool:
                        return li.extract(varname) == desired

                    if type(example) is list:
                        if len(example):
                            example = example[0]

                        def filter_function(li: Any) -> bool:
                            return desired in li.extract(varname)

                    if type(example) is bool:
                        desired = parse_boolean(desired)
                    elif type(example) is UUID:
                        try:
                            desired = self.user_dataset.convert_user(desired)
                        except KeyError:
                            self.notes.append(f"{desired} is not a known username.")
                            desired = None
                    elif type(example) is int:
                        desired = int(desired)
                    # elif callable(example):
                    #    def filter_function(li: Any) -> bool:
                    #        return li.extract(varname)(desired)

                    preres = len(res)
                    res = [m for m in res if filter_function(m)]
                    self.log(f"Applied filter {filt} and got {len(res)} resulting objects (from {preres}).")
                    if not res:
                        raise RuntimeError(
                            f"Empty dataset after applying filter: {filt}. This can indicate the wrong attribute is being filtered on, or that the value being searched for is wrong, or just that there are no results that match your query."
                        )
                        # return l.clone(f'Empty dataset after applying filter: {filt}')
                else:
                    raise RuntimeError(f"Unsupported filter type {type(filt)} for filter {filt}")

            return res

        @Local()
        def localjob(l, job: tuple[str, str]):
            """
            `job(arguments...)` - Executes a job. I don't know. Why did I add this. I need help.
            """
            if type(job) != tuple:
                raise RuntimeError(f"Job {job} was provided without an argument list.")
            return jobs.execute(job, l=l, varlist=varlist)

        def execute_simple(fname, args) -> commands.ExecutableExpression:
            # Executes a command with the listed arguments.
            # Does not (!) evaluate function/expression parameters.
            for comlist in comlists:
                if fname in comlist:
                    self.log(f"Creating expression {fname}({args})")
                    return comlist[fname].prime(*args)
            raise RuntimeError(f"{fname} is not a valid command name. Try `commands` to list valid commands.")

        def try_execute(e: Expression) -> commands.ExecutableExpression:
            if not e.arguments:
                return execute_simple(e.command, [])
            if all([type(a) == str for a in e.arguments]):
                return execute_simple(e.command, e.arguments)

            # TODO - support functions properly. :) for now no recursion for you, fools! no recursion for anyone! ahahaha
            if all([type(a) in [str, tuple, list] for a in e.arguments]):
                return execute_simple(e.command, e.arguments)
            raise RuntimeError(f"Could not execute: {e}")

        while pipeline:
            e = pipeline.pop(0)
            eid = f"Expression@c:{e.loc}"
            self.time(eid)
            try:
                exe = try_execute(e)
                res = exe(dataset)
                # TODO - rolling 'latest dataset metainfo' here for games
                if res is None:
                    pass
                elif type(res) in SUPPORTED_ITERABLES:
                    dataset = dataset.clone(res)
                else:
                    if not type(res) == Dataset:
                        raise RuntimeError(f"Got unhandled result type {type(res)} in {e}")
                    dataset = res
                self.log_time(eid)
            except Exception as err:
                raise RuntimeError(f'While executing `{e.command} {" ".join(e.arguments)}`, encountered error of type {type(err)}: {err}') from err

            if not pipeline:
                # Determine if this is terminal.
                self.log("Completed execution. Info:", dataset.info())
                if exe.executor.print_dataset:
                    return dataset
                else:
                    return None

        self.log("Completed execution. Info:", dataset.info())
        return dataset
