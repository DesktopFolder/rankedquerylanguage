# might want to port this functionality - mainly better/dynamic index parsing? not sure what else

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
