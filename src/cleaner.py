import pandas as pd
import numpy as np


class Cleaner:
    def __init__(self, dataset):
        """Tidies data into the correct format for making predictions

        Args:
            dataset (pd.DataFrame): dataset containing the result for a given league in a given season in matrix form.
                An example is available at https://en.wikipedia.org/wiki/2019%E2%80%9320_EFL_Championship#Results
        """
        self.dataset = dataset
        self.dataset = self.dataset.set_index("Home \ Away")

    def generate_away_goals(self):
        """Produces a dictionary which contains the away goals for a given dataset

        Return:
            dict: a dictionary of array which represents the number of away goals scored by each team in a given season
        """
        # Instantiate an empty dictionary to be populated
        away_goals_dict = {}

        # Generate away goals for each team in the dataset
        for col in self.dataset.columns:
            self.dataset.loc[self.dataset[col].isna(), col] = "-"
            away_goals = self.dataset[col].str[2:]

            # Check which value is null and remove
            array_filter = (away_goals != "")
            away_goals_dict[col] = away_goals.array[array_filter].astype(int)

        return away_goals_dict

    def generate_home_goals(self):
        """Produces a dictionary which contains the home goals for a given dataset

        Return:
            dict: a dictionary of array which represents the number of home goals scored by each team in a given season
        """
        # Instantiate an empty dictionary to be populated
        home_goals_dict = {}

        # Outer loop calculates the result for each team in the dataset
        for row in range(self.dataset.shape[0]):
            home_scores = []
            row = row
            # Inner loop calculates the result for each opposition team faced
            for col in range(self.dataset.shape[1]):
                home_scores.append(self.dataset.iloc[row, col][0])
            # Summary of the goals scored by the team is appended to the dictionary
            home_goals_dict[row] = np.asarray(home_scores)

        # Address non-numeric contains contained within the dataset
        for i in home_goals_dict.keys():
            # Remove '�'
            mask_one = home_goals_dict[i] != '�'
            home_goals_dict[i] = home_goals_dict[i][mask_one]

            # Remove "-"
            mask_two = home_goals_dict[i] != '-'
            home_goals_dict[i] = home_goals_dict[i][mask_two]

            # Remove "-"
            mask_three = home_goals_dict[i] != ' '
            home_goals_dict[i] = home_goals_dict[i][mask_three]

            # Convert array to an int type
            home_goals_dict[i] = home_goals_dict[i].astype(int)

        return home_goals_dict
