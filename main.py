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

    # Generate summary datasets to be used for predictions
    home_goals_summarised = dataset.groupby(["home_team"]).head(config["form"]).groupby(["home_team"]).agg([np.mean])
    away_goals_summarised = dataset.groupby(["home_team"]).head(config["form"]).groupby(["away_team"]).agg([np.mean])

    # Make predictions for the gameweek
    predictor = Predictor(config, home_goals_summarised, away_goals_summarised)
    predictor.make_predictions()


if __name__ == "__main__":
    main()
