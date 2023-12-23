from .dataset import Dataset
from typing import Callable

# Special jobs that do complex things that can't really be done feasibly within the language
# currently

JOBS = {}


def Job(f: Callable):
    realname = f.__name__
    JOBS[realname] = f
    return f


def execute(job, l: Dataset, varlist):
    # we actually use variables, lol
    job, args = job
    if job in JOBS:
        return JOBS[job](ds=l, args=args, variables=varlist)
    raise RuntimeError(f"Job {job} does not exist. Valid jobs: {list(JOBS.keys())}")


@Job
def generate_splits_by_uuids(ds: Dataset, args: str, variables):
    return ds
