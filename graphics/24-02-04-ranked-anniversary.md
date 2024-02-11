Total sub 10s: `index matchanalysis | filter nodecay completed | drop duration gt(600000) | rsort duration | count`
Total unique players: `index most | players | extract uuid | count`
Total games played: `index matchanalysis | count`
Total hours played: `index matchanalysis | sum duration`
