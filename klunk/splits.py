from klunk.match import Timeline, QueryMatch
from .dataset import Dataset
from typing import Callable
from .commands import Executor

# Special jobs that do complex things that can't really be done feasibly within the language
# currently

"""
1. ['story.root', 'story.mine_stone', 'story.smelt_iron', 'husbandry.root', 'story.iron_tools', 'story.lava_bucket', 'nether.root', 'nether.find_fortress', 'story.enter_the_nether', 'adventure.root', 'adventure.kill_a_mob', 'nether.obtain_blaze_rod', 'story.deflect_arrow', 'story.form_obsidian', 'nether.find_bastion', 'nether.loot_bastion', 'nether.obtain_crying_obsidian', 'story.obtain_armor', 'projectelo.timeline.blind_travel', 'story.follow_ender_eye', 'adventure.sleep_in_bed', 'projectelo.timeline.forfeit']
2. ['story.smelt_iron', 'story.mine_stone', 'story.root', 'story.iron_tools', 'husbandry.root', 'story.lava_bucket', 'nether.root', 'nether.find_fortress', 'story.enter_the_nether', 'adventure.root', 'adventure.kill_a_mob', 'nether.obtain_blaze_rod', 'nether.find_bastion', 'nether.loot_bastion', 'nether.distract_piglin', 'story.form_obsidian', 'nether.obtain_crying_obsidian', 'story.obtain_armor', 'projectelo.timeline.blind_travel', 'story.follow_ender_eye', 'adventure.sleep_in_bed', 'story.enter_the_end', 'end.root', 'adventure.ol_betsy']
3. ['story.obtain_armor', 'story.smelt_iron', 'story.root', 'story.iron_tools', 'story.mine_stone', 'story.lava_bucket', 'story.enter_the_nether', 'nether.root', 'husbandry.root', 'nether.find_bastion', 'nether.loot_bastion', 'nether.obtain_crying_obsidian', 'story.form_obsidian', 'adventure.kill_a_mob', 'adventure.root', 'nether.find_fortress', 'nether.obtain_blaze_rod', 'projectelo.timeline.blind_travel']
4. ['story.obtain_armor', 'story.smelt_iron', 'story.root', 'story.iron_tools', 'story.lava_bucket', 'nether.root', 'story.enter_the_nether', 'husbandry.root', 'nether.find_bastion', 'nether.loot_bastion', 'nether.obtain_crying_obsidian', 'story.mine_stone', 'story.form_obsidian', 'projectelo.timeline.forfeit']
5. ['story.smelt_iron', 'story.root', 'story.iron_tools', 'story.lava_bucket', 'story.mine_stone', 'nether.root', 'story.enter_the_nether', 'nether.find_bastion', 'nether.loot_bastion', 'nether.obtain_crying_obsidian', 'story.form_obsidian', 'husbandry.root', 'nether.find_fortress', 'adventure.root', 'adventure.kill_a_mob', 'nether.obtain_blaze_rod', 'projectelo.timeline.blind_travel', 'story.follow_ender_eye', 'end.root', 'story.enter_the_end']
6. ['story.smelt_iron', 'story.root', 'story.iron_tools', 'story.lava_bucket', 'story.mine_stone', 'nether.root', 'story.enter_the_nether', 'nether.find_bastion', 'nether.loot_bastion', 'story.form_obsidian', 'nether.obtain_crying_obsidian', 'husbandry.root', 'story.obtain_armor', 'nether.find_fortress', 'adventure.root', 'adventure.kill_a_mob', 'nether.obtain_blaze_rod', 'projectelo.timeline.blind_travel', 'story.follow_ender_eye', 'adventure.sleep_in_bed', 'story.enter_the_end', 'end.root', 'adventure.ol_betsy']
7. ['story.smelt_iron', 'story.root', 'story.iron_tools', 'story.mine_stone', 'story.lava_bucket', 'story.enter_the_nether', 'nether.root', 'husbandry.root', 'projectelo.timeline.forfeit']
8. ['story.root', 'husbandry.root', 'story.smelt_iron', 'story.iron_tools', 'story.lava_bucket', 'story.mine_stone', 'nether.root', 'story.enter_the_nether']
9. ['story.smelt_iron', 'story.iron_tools', 'story.form_obsidian', 'story.obtain_armor', 'story.mine_stone', 'story.root', 'adventure.root', 'story.lava_bucket', 'story.enter_the_nether', 'nether.root', 'husbandry.root', 'nether.find_bastion', 'nether.obtain_crying_obsidian', 'nether.loot_bastion', 'nether.find_fortress', 'adventure.kill_a_mob', 'nether.obtain_blaze_rod', 'projectelo.timeline.blind_travel', 'story.follow_ender_eye', 'adventure.sleep_in_bed', 'story.enter_the_end', 'end.root']
10. ['story.form_obsidian', 'story.obtain_armor', 'story.smelt_iron', 'story.iron_tools', 'story.mine_stone', 'story.root', 'adventure.root', 'story.lava_bucket', 'husbandry.root', 'story.enter_the_nether', 'nether.root', 'nether.find_bastion', 'nether.loot_bastion', 'nether.obtain_crying_obsidian', 'adventure.kill_a_mob', 'nether.find_fortress', 'nether.obtain_blaze_rod', 'projectelo.timeline.blind_travel', 'story.follow_ender_eye''
"""

COMMANDS = {}


def Split(f: Callable):
    realname = f.__name__
    COMMANDS[f"splits.{realname}"] = Executor(f)
    return Executor(f)


def split_eq(*, real: str, query: str):
    return real == query or real.partition(".")[2] == query


def has_split(l: list[Timeline], split_id: str):
    for split in l:
        if split_eq(real=split.id, query=split_id):
            return True
    return False


def get_split(l: list[Timeline], split_id: str):
    for split in l:
        if split_eq(real=split.id, query=split_id):
            return split
    return None


@Split
def filter(ds: Dataset, *args):
    """
    `splits.filter(args)` - Honestly I don't remember what this does.
    Here's the code if you want to figure it out, lol:
    `[[y for y in x if any([split_eq(real=y.id, query=a) for a in args])] for x in ds.l]`
    """
    return [[y for y in x if any([split_eq(real=y.id, query=a) for a in args])] for x in ds.l]


@Split
def has(ds: Dataset, split_id: str):
    """
    `splits.has(split_id)` - todo - support multiple ids
    """
    return [s for s in ds.l if has_split(s, split_id)]


@Split
def get(ds: Dataset, split_id: str):
    """
    `splits.get(split_id)` - Don't use this. You probably want splits.get_if. This one gives you None if the split doesn't exist. That one just drops such results. Better!
    """
    return [get_split(s, split_id) for s in ds.l]


@Split
def get_if(ds: Dataset, split_id: str):
    """
    `splits.get_if(split_id)` - Gets a split, if it exists.
    """
    l = list()
    for s in ds.l:
        split = get_split(s, split_id=split_id)
        if split is not None:
            l.append(split)
    return l


@Split
def diff(ds: Dataset, split_id_lt: str, split_id_gt: str):
    """
    `diff(split_id_lt, split_id_gt)` - compute the time delta between split_id_lt and split_id_gt for each split.
    All split lists which do not contain both lt & gt will be dropped.
    """
    l = list()
    id_res = f"{split_id_gt}-{split_id_lt}"
    for s in ds.l:
        lt = get_split(s, split_id_lt)
        if lt is None:
            continue
        gt = get_split(s, split_id_gt)
        if gt is None:
            continue
        if gt.uuid != lt.uuid:
            raise RuntimeError(
                f"UUIDs during diff of {split_id_lt} and {split_id_gt} did not match. Maybe you forgot to segment the lists by uuid."
            )
        # Recombine into a weird Timeline... thing.
        l.append(Timeline.from_items(gt.time - lt.time, id_res, lt.uuid))
    return l


@Split
def dump_ids(ds: Dataset):
    """
    `splits.dump_ids` - Gives you the IDs of all splits in your list of splits.
    This is just for debugging. You probably don't need or want it.
    """
    return [[split.id for split in l] for l in ds.l]

@Split
def match_has_any(ds: Dataset, split_id: str):
    l: list[QueryMatch] = ds.l
    
    def has_split(q: QueryMatch):
        return q.timelines.earlist_time(split_id) is not None

    return ds.clone([o for o in l if has_split(o)])

@Split
def match_has_none(ds: Dataset, split_id: str):
    l: list[QueryMatch] = ds.l
    
    def has_split(q: QueryMatch):
        return q.timelines.earlist_time(split_id) is not None

    return ds.clone([o for o in l if not has_split(o)])
