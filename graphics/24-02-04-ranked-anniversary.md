Total sub 10s: `index most | filter nodecay completed | drop duration gt(600000) | rsort duration | count`
Total unique players: `index most | players | extract uuid | count`
Total games played: `index most | count`
