from .extra_types import UUID, Milliseconds, Seconds
from .match import QueryMatch
from .parse_utils import partition_list
from .component import Component
from .expression import Expression
from .dataset import Dataset
from typing import Callable, Any
from . import commands
from .players import MatchPlayer, PlayerManager
from .parse import parse_boolean
from .utils import average, time_fmt
# Later - this would be nice :)
# from .language import Compiler, Tokenizer

def get_help():
    return """Klunk Beta
Compose a query with pipes (|), commands (filter, sort, etc), and arguments (e.g. `filter winner(DesktopFolder)`)
The dataset is manipulated, sorted, filtered, etc. through the pipeline, but you may output information from it at any point.

Important notes:
    - `filter`, `sort`, etc use magic extraction methods to query data. To determine what keys are available to sort/filter/etc on,
      pipe your data first into `attrs` (e.g. `index some-index | some-commands | attrs`)
    - You can list all available commands with `commands`. However, some may be for debugging (or otherwise unhelpful)

Examples:
    - `index all | filter season(1) type(2) nodecay | count`
      - Uses the `index` command to switch to the `all` index, which has all matches ever recorded.
      - Uses the `filter` command with `season(1)` to remove all non-season-1 matches, `type(2)` to only have ranked matches,
        and `nodecay` to remove all decay matches (which are automatically created when a player experiences ELO decay)
      - Uses the `count` command to output the resulting number of matches.
"""

class Runtime(Component):
    def __init__(self, datasets: dict[str, Dataset], commands: dict[str, Callable]):
        super().__init__("Runtime")

        self.datasets = datasets
        self.commands = commands
        self.notes = list()

        alldatalen = len(self.datasets['all'].l) 
        if alldatalen < 50000:
            self.notes.append(f'Running with total dataset size {alldatalen} - likely in testing mode.')

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
        comlists = [self.commands, commands.basic_commands, localfunclist]
        # Now THIS is definitely not correct but whatever lol
        commands.logger = self

        def Local(f: Callable):
            realname = f.__name__[len("local"):]
            localfunclist[realname] = f
            return f

        @Local
        def localvars(_):
            self.add_result(varlist)

        @Local
        def localindex(_, name: str):
            self.log(f"Changing dataset to {name}")
            if not name in self.datasets:
                raise RuntimeError(f'{name} is not a valid dataset name.')
            return self.datasets[name]

        @Local
        def localcommands(_):
            lcoms = set()
            for l in comlists:
                for v in l.keys():
                    lcoms.add(v)
            self.add_result(f'Valid commands:', ', '.join(sorted(lcoms)))

        @Local
        def localallfuncs(_):
            for i, comlist in enumerate(comlists):
                self.add_result(f'Functions with priority {len(comlists)-i}:', ', '.join(comlist.keys()))

        @Local
        def localinfo(_):
            self.add_result(dataset.info())

        @Local
        def localwait(_, num_seconds):
            pass

        @Local
        def localplayers(l: Dataset):
            # TODO. lol
            return l.clone(PlayerManager(l.l, lambda x: True))

        @Local
        def localhelp(_):
            self.add_result(get_help())

        @Local
        def localsort(l: Dataset, attribute, **kwargs):
            # For now this should do the trick.
            return sorted(l.l, key=lambda x: x.extract(attribute), **kwargs)

        @Local
        def localrsort(l: Dataset, attribute):
            # For now this should do the trick.
            return localsort(l, attribute, reverse=True)

        @Local
        def localtake(l: Dataset, *args):
            data = l.l
            args = list(args)
            # ints, sa = partition_list(args, lambda a: type(a) is int)
            # TODO lol
            ints, sa = partition_list(args, lambda a: a.isdigit())
            if len(ints) > 1:
                raise RuntimeError(f'Command `take` got {len(ints)} arguments, requires at most 1.')
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
            data = l.l
            if not data:
                return self.add_result(f'Dataset was empty; no average calculable.')
            example = data[0].extract(val)
            if type(example) not in [int, Milliseconds, Seconds]:
                return self.add_result(f'Could not average type {type(example)}.')
            result = average([x.extract(val) for x in data])
            if 'time' in args or type(example) in [Milliseconds, Seconds]:
                tf = time_fmt(result, type(example) is Seconds or 'seconds' in args)
                self.add_result(f'Average {val}: {tf}')
            else:
                self.add_result(f'Average {val}: ' + str((result if 'precise' in args else round(result, 2))))

        @Local
        def localcount(l: Dataset):
            if type(l.l) is list:
                self.add_result(f'Current size: {len(l.l)}')
            else:
                self.add_result(f'Dataset currently only has one item.')

        @Local
        def localattrs(l: Dataset):
            example = l.l[0]
            self.add_result(f'Known accessible attributes of {type(example)}: ' + ", ".join(example.__slots__))

        @Local
        def localexample(l: Dataset, attribute=None):
            example = l.l[0]
            if attribute is not None:
                self.add_result(f'Example value of {attribute}: {example.extract(attribute)}')
            else:
                d = dict()
                for k in example.__slots__:
                    v = example.extract(attribute)
                    if type(v) != list:
                        d[k] = v
                    else:
                        d[k] = 'List[...]'
                self.add_result(f'Example object layout: {d}')

        @Local
        def localfilter(l: Dataset, *args):
            res: list[Any] = l.l
            for filt in args:
                # Short circuit if we have no objects.
                if not res:
                    self.log(f'Avoided applying filter {filt} and any additional filters (empty result).')
                    break

                # For now, support a bunch of nice boolean autodetection/conversions.
                if type(filt) == str:
                    prefilt = filt
                    b = True
                    if filt.startswith('no'):
                        b = False
                        filt = filt[2:]
                    filt = (f'is_{filt}', b)
                    self.log(f'Found simple boolean filter {prefilt} and converted it to {filt}.')

                if type(filt) == tuple:
                    # Let's be smart about this. Get our desired destination type for conversion.
                    # Is that smart? Whatever, this is a query language built in Python, anyways.
                    first = res[0]
                    varname, desired = filt
                    example = first.extract(varname)
                    if type(example) is bool:
                        desired = parse_boolean(desired)
                    elif type(example) is UUID:
                        desired = self.datasets["__users"].l[desired.lower()]
                    elif type(example) is int:
                        desired = int(desired)
                    preres = len(res)
                    res = [m for m in res if m.extract(varname) == desired]
                    self.log(f'Applied filter {filt} and got {len(res)} resulting objects (from {preres}).')
                    if not res:
                        return l.clone(f'Empty dataset after applying filter: {filt}')
                else:
                    raise RuntimeError(f'Unsupported filter type {type(filt)} for filter {filt}')

            return res

        def execute_simple(l, fname, args):
            # Executes a command with the listed arguments.
            # Does not (!) evaluate function/expression parameters.
            for comlist in comlists:
                if fname in comlist:
                    self.log(f"Attempting to execute {fname}({args})")
                    return comlist[fname](l, *args)
            raise RuntimeError(f"{fname} is not a valid command name. Try `commands` to list valid commands.")

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
            if res is None:
                pass
            elif type(res) == list:
                dataset = dataset.clone(res)
            else:
                if not type(res) == Dataset:
                    raise RuntimeError(f"Got unhandled result type {type(res)} in {e}")
                dataset = res
            self.log_time(eid)

        self.log("Completed execution. Info:", dataset.info())
        return dataset
