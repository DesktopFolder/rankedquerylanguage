class AutoDataset:
    def __init__(self, name: str, root=False):
        self.name = name
        self.has_unranked: bool = True
        """
        if root and isinstance(self.l, list):
            if self.l and isinstance(self.l[0], QueryMatch):
                # if all matches are ranked
                if all([m.is_ranked() for m in l]):
                    self.has_unranked = False
        """

    def __len__(self):
        if self.has_iterable():
            return len(self.l)
        return 1

    def has_iterable(self):
        if isinstance(self.l, str):
            return False
        if hasattr(self.l, '__len__'):
            return True
        return False

    def iter(self):
        if self.has_iterable():
            return iter(self.l)
        return [self.l]

    def clone(self, l):
        d = AutoDataset(self.name)
        d.has_unranked = self.has_unranked
        return d

    def update(self, other):
        last_mid = self.l[-1].id
        # ensure we don't get duplicate matches
        ilen = len(other)
        other = [m for m in other if m.id > last_mid]
        print(f"removed {ilen - len(other)} matches from update (dupes)")
        self.l.extend(other)

    def update_overwrite_dict(self, other: dict[str, str]):
        for k, v in other.items():
            self.l[k] = v

    def info(self):
        from klunk.dataset import format_str
        if type(self.l) in [list, dict]:
            return f"Dataset {self.name}, currently with {len(self.l)} objects."
        return f"Dataset containing {format_str(self.l)}"

    def detailed_info(self):
        return f"Dataset {self.name}. Contains {len(self)} items. Type of first item: {type(self.example())}"

    def summarize(self, formatter=None):
        from klunk.match import MatchMember, QueryMatch
        from klunk.dataset import format_str
        def cleaned(s):
            if formatter is not None and 'clean' in formatter:
                return formatter["clean"](s)
            return s
        val = self.l
        if type(val) == dict:
            val = list(val.items())
        if isinstance(val, list):
            length = len(val)
            if length == 1:
                return cleaned(format_str(val[0]))
            res = cleaned("\n".join([f"{i+1}. {format_str(v)}" for i, v in enumerate(val[0:10])]))
            if length > 10:
                res += f"\n... ({length - 10} values trimmed)"
            return res
        if type(val) == str:
            return cleaned(val)
        if type(val) in [tuple, MatchMember, QueryMatch]:
            return format_str(val)
        raise RuntimeError(f'Could not summarize {type(val)}')

    def example(self):
        from klunk.dataset import first_not_none
        if self.has_iterable():
            try:
                if type(self.l) == dict:
                    return first_not_none(list(self.l.values()))
                return first_not_none(self.l)
            except:
                return None
        return self.l

    @property
    def l(self):
        return []

class AutoDatasets:
    def __init__(self):
        pass

    def __getitem__(self, key: str):
        return AutoDataset(key)
