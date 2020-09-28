import numpy as np


class Predictor:
    """Predicts the outcomes of a given set of matches

    Notes:
        Loops through the config file which contains the gameweek fixtures

    Args:
        config (dict): Dictionary which contains the games to be played for a given week
        home_goals_summarised (pd.DataFrame): DataFrame containing the summarised home goals for each team
        away_goals_summarised (pd.DataFrame): DataFrame containing the summarised away goals from each team
    """
    def __init__(self, config, home_goals_summarised, away_goals_summarised):
        self.config = config
        self.home_goals_summarised = home_goals_summarised
        self.away_goals_summarised = away_goals_summarised

    def make_predictions(self):
        """Outputs predictions for a given set of fixtures

        Notes:
            Makes predictions for a set of fixtures defined in the config file.
        """
        # Loop through each of the fixtures in a given game week
        for i in range(1, 11):
            # Get game reference
            fixture = "game_" + str(i)
            # Get prediction for outcome
            result = self._predict_match(self.config[fixture]["home"], self.config[fixture]["away"])

            # Get predictions for scoreline
            home_goals_probs, away_goals_probs = self._predict_score(self.config[fixture]["home"],
                                                                     self.config[fixture]["away"])
            predicted_scoreline = self._generate_score_probabilities(home_goals_probs, away_goals_probs)

            # Instantiate loop
            possible_results = []
            counter = 0

            # Loop through scoreline dictionary
            while counter < self.config["requested_results"]:
                for key, value in predicted_scoreline.items():
                    # If we don't have the required number keep going
                    if counter < self.config["requested_results"]:
                        possible_results.append(str(key) + " - " + str(round(predicted_scoreline[key], 3) * 100) + "%")
                        counter += 1
                    else:
                        break

            # Output results
            print(self.config[fixture]["home"] + ":" + str(round(result[0], 1) * 100) + "% / " +
                  self.config[fixture]["away"] + ":" + str(round(result[1], 1) * 100) + "% / " +
                  "Draw: " + str(round(result[2], 1) * 100) + "%")
            print(possible_results)

    def _predict_match(self, home_team, away_team):
        """Predicts the outcome for an individual match

        Args:
            home_team (str): Name of the home team in the fixture
            away_team (str): Name of the away team in the fixture

        Return:
            tuple: containing the probability of a home win, away win or draw
        """
        # Instantiate empty results
        home_wins = 0
        home_score = 0
        away_wins = 0
        away_score = 0
        draw = 0

        # Get the home and away team's location in the result
        home_loc = self.home_goals_summarised.index.get_loc(home_team)
        away_loc = self.away_goals_summarised.index.get_loc(away_team)

        # Loop through required number of simulations
        for i in range(self.config["simulations_to_run"]):
            # Extract away goals
            away_goals_scored = np.random.poisson(self.away_goals_summarised.iloc[away_loc, 1], 1)
            home_goals_conceded = np.random.poisson(self.home_goals_summarised.iloc[home_loc, 1], 1)
            # Weight away goals
            away_goals_weighted = np.mean([away_goals_scored, home_goals_conceded])
            # Extract home goals
            home_goals_scored = np.random.poisson(self.home_goals_summarised.iloc[home_loc, 0], 1)
            away_goals_conceded = np.random.poisson(self.away_goals_summarised.iloc[away_loc, 0])
            # Weight away goals
            home_goals_weighted = np.mean([home_goals_scored, away_goals_conceded])

            # Add these results to tally
            home_score += home_goals_scored
            away_score += away_goals_scored

            # Update results with outcome of the simulation
            if home_goals_weighted > away_goals_weighted:
                home_wins += 1
            elif home_goals_weighted < away_goals_weighted:
                away_wins += 1
            else:
                draw += 1

        return home_wins / self.config["simulations_to_run"], away_wins / self.config["simulations_to_run"], \
            draw / self.config["simulations_to_run"]

    def _predict_score(self, home_team, away_team):
        """Builds a probability distribution for home and away goals scored

        Args:
            home_team (str): Name of the name team in the fixture
            away_team (str): Name of the away team in the fixture

        Returns:
            tuple: contains the goal probabilites for the home and away teams
        """
        # Build distribution for Home team
        home_loc = self.home_goals_summarised.index.get_loc(home_team)
        home_goals_distribution = np.random.poisson(self.home_goals_summarised.iloc[home_loc, 0],
                                                    self.config["simulations_to_run"])

        # Build distribution for Away team
        away_loc = self.away_goals_summarised.index.get_loc(away_team)
        away_goals_distribution = np.random.poisson(self.away_goals_summarised.iloc[away_loc, 1],
                                                    self.config["simulations_to_run"])
        # Instantiate empty dictionary
        home_goals_probs = {}
        # Loop through possible goals
        for goals in range(10):
            home_goals_probs[goals] = np.mean(goals == home_goals_distribution)
        # Instantiate through empty dictionary
        away_goals_probs = {}
        # Loop through possible goals
        for goals in range(10):
            away_goals_probs[goals] = np.mean(goals == away_goals_distribution)

        return home_goals_probs, away_goals_probs

    def _generate_score_probabilities(self, home_goals_probs, away_goals_probs):
        """Order results dictionary

        Args:
            home_goals_probs (np.array): array which contains the probabilities for a given number of goals to be
                scored by the home team
            away_goals_probs (np.array): array which contains the probabilities for a given number of goals to be
                scored by the away team

        Returns:
            dict: Dictionary containing possible scorelines and their respective conditional probabolities.
        """
        # Instantiate empty dictionary for scorelines
        score_probabilities = {}

        # Loop through home and away goals and get conditional probabilities for both
        for home_goals in range(10):
            for away_goals in range(10):
                score_probabilities[str(home_goals) + "-" + str(away_goals)] = round(
                    home_goals_probs[home_goals] * away_goals_probs[away_goals], 3)

        # Order probabilities to make identifying the top results easier
        score_probabilities = {k: v for k, v in
                               sorted(score_probabilities.items(), key=lambda item: item[1], reverse=True)}

        return score_probabilities

