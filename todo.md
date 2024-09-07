### BUG FIXES!

- `filter decay | extract winner` -> drawn matches?
- `index all | filter season(1) | count` -> gets us nothing, which is wrong, we should have playoffs matches

### Performance / References

- https://stackoverflow.com/questions/42364044/python-interpreter-string-pooling-optimization
- https://stackoverflow.com/questions/67650841/how-to-find-memory-usage-with-memory-profiler-python

---

- randomselect
- dumpjson
- raw
- whatever fulhamn needed idk
- `output` -> `output file`, `output graph`, etc
- | timefmt / | durationfmt

- make commands a separate object etc (commandctx) @Command(t=list)
- live update

- lazy evaluation.
    - where possible, return EvaluatableFilter
    - Terminals that require a certain # of things are PullingFilter or something?

- tournament.py but as a job
- tournament creation system

---

### Priority Todos

- Format discord messages with underscores!!!
- Add notes for new people - commands, attrs, help? dunno
- Error earlier if we get nothing
- Add command for average completion
- Add more help - stuff like index info etc in every message!!! don't let people use index all? idk
- Add AST-based analysis (e.g. you should use index most or s2 etc)

---

## Use Cases

### Tournament Post Analysis

I want to see the splits of the tournament winner's games, throughout the final match (7 games).
Specifically, to graph them against / compare to top 50 players'.

So. `index s2 | players | rsort elo | take 50 | drop nick [name] | assign TOP50 uuid`
-> Gets us a variable with the UUIDs of the top 50 players (not including the noted user)

Then we need something like: jobs.splitsmap maybe? idk, I don't want to fully support splits yet,
way too much work for now.

`extract timelines | segmentby uuid | drop_list empty | splits.has smelt_iron | splits.get smelt_iron`

`index s2 | job generate_splits_by_uuids(TOP50)`
