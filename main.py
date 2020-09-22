import pandas as pd
import numpy as np
from glob import glob
from src.predictor import Predictor
from src.cleaner import Cleaner
from ruamel_yaml import safe_load


def main():
    # Instantiate empty dataset for all results
    # TODO Make this into one dictionary rather than several
    away_goals_scored_dataset = {}
    away_goals_conceded_dataset = {}
    home_goals_scored_dataset = {}
    home_goals_conceded_dataset = {}

    with open("config.yml", "r") as file:
        config = safe_load(file)

    # Parse in datasets to be added to the database
    for file in glob("data/*results.csv"):
        dataset = pd.read_csv(file)
        # Instantiate cleaner object
        cleaner = Cleaner(dataset)

        # Parse away goals
        away_goals_scored, away_goals_conceded = cleaner.generate_away_goals()
        # Append away goals to dataset
        away_goals_scored_dataset.update(away_goals_scored)
        away_goals_conceded_dataset.update(away_goals_conceded)

        # Parse home goals and append to database
        home_goals_scored, home_goals_conceded = cleaner.generate_home_goals()

        # TODO tidy this section so it only get's called once

        # Make the that these have both got the same keys
        new_home_goals_scored = {}
        for i, value in enumerate(away_goals_scored.keys()):
            new_home_goals_scored[value] = home_goals_scored[i]

        home_goals_scored_dataset.update(new_home_goals_scored)

        # # Make the that these have both got the same keys
        new_home_goals_conceded = {}
        for i, value in enumerate(away_goals_conceded.keys()):
            new_home_goals_conceded[value] = home_goals_conceded[i]

        home_goals_conceded_dataset.update(new_home_goals_conceded)

    # Loop through prompted teams
    for abb, team, filepath in zip(["LEE", "FUL", "WBA"], ["Leeds United", "Fulham", "West Bromwich Albion"],
                                 ["data/premier_league_2003_2004.csv", "data/premier_league_results_2018_2019.csv",
                                  "data/premier_league_results_2017_2018.csv"]):
        # TODO Tidy this section so it's a bit neater
        # Read the dataset
        with open(filepath) as file:
            dataset = pd.read_csv(file)
            # Parse away goals from the given dataset
            prompted_team_away_goals_scored, prompted_team_away_goals_conceded = \
                cleaner.generate_promoted_team_away_goals(abb, dataset)
            # Update away goals in original dataset
            away_goals_scored_dataset[abb] = prompted_team_away_goals_scored
            away_goals_conceded_dataset[abb] = prompted_team_away_goals_conceded

            # Parse away goals from the given dataset
            prompted_team_home_goals_scored, prompted_team_home_goals_conceded = \
                cleaner.generate_promoted_team_home_goals(team, dataset)
            # Update away goals in original dataset
            home_goals_scored_dataset[abb] = prompted_team_home_goals_scored
            home_goals_conceded_dataset[abb] = prompted_team_home_goals_conceded

    # Use new datasets to update results for the current campaing.
    for file in glob("data/*_new_season.csv"):
        with open(file) as dataset:
            dataset = pd.read_csv(dataset)
            cleaner = Cleaner(dataset)
            # TODO tidy this section so it's less repetative
            # Parse new seasons away goals
            away_goals_scored_new, away_goals_conceded_new = cleaner.generate_away_goals()

            # Update away goals scored dictionary with result from the new season
            for key, values in away_goals_scored_new.items():
                away_goals_scored_dataset[key] = np.append(away_goals_scored_dataset[key],
                                                           away_goals_scored_new[key])

            # Update away goals scored dictionary with result from the new season
            for key, values in away_goals_conceded_new.items():
                away_goals_conceded_dataset[key] = np.append(away_goals_conceded_dataset[key],
                                                             away_goals_conceded_new[key])

            # Parse new seasons home scored goals
            home_goals_scored_new_season, home_goals_conceded_new_season = cleaner.generate_home_goals()

            # Update dictionary keys
            home_goals_scored_dataset_update = {}
            for i, value in enumerate(away_goals_scored_new.keys()):
                home_goals_scored_dataset_update[value] = home_goals_scored_new_season[i]

            # Update home goals dictionary with results from the new season
            for key, values in home_goals_scored_dataset_update.items():
                home_goals_scored_dataset[key] = np.append(home_goals_scored_dataset[key],
                                                           home_goals_scored_dataset_update[key])

            # Update dictionary keys
            home_goals_conceded_dataset_update = {}
            for i, value in enumerate(away_goals_conceded_new.keys()):
                home_goals_conceded_dataset_update[value] = home_goals_conceded_new_season[i]

            # Update home goals dictionary with results from the new season
            for key, values in home_goals_conceded_dataset_update.items():
                home_goals_conceded_dataset[key] = np.append(home_goals_conceded_dataset[key],
                                                             home_goals_conceded_dataset_update[key])

    # Create predictor object
    predictor = Predictor(config, home_goals_scored_dataset, away_goals_scored_dataset)

    # Print predictions to screen
    predictor.make_predictions()


if __name__ == "__main__":
    main()
