import numpy as np


class Predictor:
    """Predicts the outcomes of a given set of matches

    Notes:
        Loops through the config file which contains the gameweek fixtures

    Args:
        config (dict): Dictionary which contains the games to be played for a given week
        home_goals_dict (dict): Dictionary which contains an array of home goals scored where each team name is a key
        home_goals_dict (dict): Dictionary which contains an array of away goals scored where each team name is a key
    """
    def __init__(self, config, home_goals_dict, away_goals_dict):
        self.config = config
        self.home_goals_dict = home_goals_dict
        self.away_goals_dict = away_goals_dict

    def make_predictions(self):

        for i in range(1, 11):
            fixture = "game_" + str(i)
            result = self.predict_match(self.config[fixture]["home"], self.config[fixture]["away"],
                                        self.config["simulations_to_run"])
            print(self.config[fixture]["home"] + ":" + str(result[0]) + " / " +
                  self.config[fixture]["away"] + ":" + str(result[1]) + " / " +
                  "Draw : " + str(result[2]))

    def predict_match(self, home_team, away_team, sims):
        """Predicts the outcome for an individual match

        Args:
            home_team (str): 3 letter abb of the home team
            away_team (str): 3 letter abb of the away team
            sims (int): Number of simulations to be run

        Return:
            tuple: containing the probability of a home win, away win or draw
        """
        away_win = 0
        home_win = 0
        draw = 0

        for i in range(sims):
            away_goals = np.random.choice(self.away_goals_dict[away_team], 1)
            home_goals = np.random.choice(self.home_goals_dict[home_team], 1)

            if away_goals > home_goals:
                away_win += 1
            elif home_goals > away_goals:
                home_win += 1
            else:
                draw += 1

        return home_win / sims, away_win / sims, draw / sims
