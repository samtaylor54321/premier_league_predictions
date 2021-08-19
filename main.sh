#!/bin/bash

files=`ls ./data`

echo ",ppg_home_team,recent_ppg_home_team,home_ppg_home_team,away_ppg_home_team,goals_scored_home_team,goals_conceded_home_team,oppg_home_team,ppg_away_team,recent_ppg_away_team,home_ppg_away_team,away_ppg_away_team,goals_scored_away_team,goals_conceded_away_team,oppg_away_team,result" > dataset.csv

for file in $files; do
  if [ ${file: -13} == "_fixtures.csv" ]
  then
    cat ./data/$file | grep 'home\|away\|draw' | tail -n+2 >> dataset.csv
  fi
done
