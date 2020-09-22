import pandas as pd
import numpy as np


class Cleaner:
    def __init__(self, dataset):
        """Tidies data into the correct format for making predictions

        Args:
            dataset (pd.DataFrame): dataset containing the result for a given league in a given season in matrix form.
                An example is available at https://en.wikipedia.org/wiki/2019%E2%80%9320_EFL_Championship#Results
        """
        self.dataset = self._parse_bad_characters(dataset)
        self.dataset = self.dataset.set_index("Home \ Away")

    def generate_home_goals(self):
        """Produces a dictionary which contains the home goals for a given dataset

        Return:
            tuple: a tuple of arrays which represents the number of home goals scored and conceded
            by each team in a given season
        """
        # Instantiate empty dictionaries
        home_goals_scored = {}
        home_goals_conceded = {}

        # Loop through dataset
        for row in range(self.dataset.shape[0]):
            # Excluding missing values
            mask = ~self.dataset.iloc[row].isnull().values
            # Append goals scored at home
            home_goals_scored[row] = self.dataset.iloc[row][mask].str[0].values.astype(int)
            # Append goals conceded at home
            home_goals_conceded[row] = self.dataset.iloc[row][mask].str[2].values.astype(int)

        return home_goals_scored, home_goals_conceded

    def generate_away_goals(self):
        """Produces a dictionary which contains the away goals for a given dataset

        Return:
            tuple: a tuple of arrays which represents the number of away goals scored and conceded
            by each team in a given season
        """
        # Instantiate empty dictionaries
        away_goals_scored = {}
        away_goals_conceded = {}

        # Loop through dataset
        for col in self.dataset.columns:
            # Exclude missing values
            mask = ~self.dataset[col].isnull().values
            # Append goals scored away
            away_goals_scored[col] = self.dataset[col][mask].str[2].values.astype(int)
            # Append goals conceded away
            away_goals_conceded[col] = self.dataset[col][mask].str[0].values.astype(int)

        return away_goals_scored, away_goals_conceded

    def generate_promoted_team_away_goals(self, promoted_team, dataset):
        """Overwrites existing values for teams which have been prompted to the premier league this season

        Args:
            promoted_team (str): 3 letter abb for the prompted team
            dataset (DataFrame): DataFrame of result for the team's last outing in the premier league.

        Return:
            tuple: a tuple of arrays which represents the number of away goals scored and conceded
            by each team in a given season
        """
        # Remove bad characters from dataset
        dataset = self._parse_bad_characters(dataset)

        # Set team name as index
        try:
            dataset = dataset.set_index("Home \ Away")
        except KeyError:
            print("Index column 'Home \ Away' not present in dataset")

        # Loop through the dataframe until the correct column is reached
        for col in dataset.columns:
            # Ignore the column if it's not the prompted team in question
            if col != promoted_team:
                pass
            else:
                # Exclude missing values
                mask = ~dataset[col].isnull().values
                # Append goals scored away
                away_goals_scored = dataset[col][mask].str[2].values.astype(int)
                # Append goals conceded away
                away_goals_conceded = dataset[col][mask].str[0].values.astype(int)

                return away_goals_scored, away_goals_conceded

    def generate_promoted_team_home_goals(self, promoted_team, dataset):
        """Overwrites existing values for teams which have been prompted to the premier league this season

        Args:
            promoted_team (str): Name of team to replace in index
            filepath (str): Filepath to result for the team's last outing in the premier league.

        Return:
            tuple: a tuple of arrays which represents the number of home goals scored and conceded
            by each team in a given season
        """
        # Remove bad characters from dataset
        dataset = self._parse_bad_characters(dataset)

        # Set team name as index
        try:
            dataset = dataset.set_index("Home \ Away")
        except KeyError:
            print("Index column 'Home \ Away' not present in dataset")

        # Get position of promoted team in index
        team_index_value = dataset.index.get_loc(promoted_team)
        # Exclude missing values
        mask = ~dataset.iloc[team_index_value].isnull().values
        # Append goals scored at home
        home_goals_scored = dataset.iloc[team_index_value][mask].str[0].values.astype(int)
        # Append goals conceded at home
        home_goals_conceded = dataset.iloc[team_index_value][mask].str[2].values.astype(int)

        return home_goals_scored, home_goals_conceded

    def _parse_bad_characters(self, dataset):
        """Removes bad characters from the dataset

        Notes:
            By default, this method will replace bad characters with np.nan

        Returns:
            DataFrame: DataFrame with bad characters replaced
        """
        # Pass dataset to class
        self.dataset = dataset
        # Loop through known bad characters
        for character in ["a", "�", "-", " "]:
            # Remove this and replace with NaN
            self.dataset = self.dataset.replace(character, np.nan)

        return self.dataset


