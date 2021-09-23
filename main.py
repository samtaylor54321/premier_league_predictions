import numpy as np
from src.predictor import Predictor
from src.scrapper import Scrapper
from ruamel_yaml import safe_load


def main():
    # Parse YAML config file
    with open("config.yml", "r") as file:
        config = safe_load(file)

    # Scrap data from Sky Sports
    scrapper = Scrapper(config)
    dataset = scrapper.build_dataset()

    # Extract goal summary from dataset
    teams = dataset["home_team"].unique()
    goal_summary = {}

    for team in teams:
        goal_summary[team] = dataset[
            (dataset["home_team"] == team) | (dataset["away_team"] == team)
        ].head(config["form"])

        goals_scored = []
        goals_conceeded = []

        for index, row in goal_summary[team].iterrows():
            if row["home_team"] == team:
                goals_scored.append(row["home_goals"])
                goals_conceeded.append(row["away_goals"])
            else:
                goals_scored.append(row["away_goals"])
                goals_conceeded.append(row["home_goals"])

        goal_summary[team] = {
            "goals_scored": np.mean(goals_scored),
            "goals_conceeded": np.mean(goals_conceeded),
        }

    # Make predictions for the gameweek
    predictor = Predictor(config, goal_summary)
    predictor.make_predictions()


if __name__ == "__main__":
    main()
