import pandas as pd
from glob import glob
from src.predictor import Predictor
from src.cleaner import Cleaner
from ruamel_yaml import safe_load


def main():
    # Instantiate empty dataset for all results
    away_goals_dataset = {}
    home_goals_dataset = {}

    with open("config.yml", "r") as file:
        config = safe_load(file)

    # Parse in datasets to be added to the database
    for file in glob("data/*.csv"):
        dataset = pd.read_csv(file)
        # Instantiate cleaner object
        cleaner = Cleaner(dataset)

        # Parse away goals and append to database
        away_goals_dict = cleaner.generate_away_goals()

        away_goals_dataset.update(away_goals_dict)

        # Parse home goals and append to database
        home_goals_dict = cleaner.generate_home_goals()

        # Make the that these have both got the same keys
        new_home_goals = {}
        for i, value in enumerate(away_goals_dict.keys()):
            new_home_goals[value] = home_goals_dict[i]

        home_goals_dataset.update(new_home_goals)

    # Create predictor object
    predictor = Predictor(config, home_goals_dataset, away_goals_dataset)

    # Print predictions to screen
    predictor.make_predictions()


if __name__ == "__main__":
    main()
