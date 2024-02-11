RANKED ONLY STATS

Total sub 10s: `index matchanalysis | filter nodecay completed | drop duration gt(600000) | rsort duration | count`
1855
Total unique players: `index most | players | extract uuid | count`
13813
Total games played: `index matchanalysis | count`
554222
Total hours played: `index matchanalysis | sum duration`
401545121805ms

seconds: 401,545,12111,5401
minutes: 6,692,418
hours:   111,540
PER PLAYER: 223,080

title text: 132
numbers text: 196
