import pandas as pd
import requests
from bs4 import BeautifulSoup
from re import search


class Scrapper:
    def __init__(self, config):
        self.config = config

    def build_dataset(self):
        """Create dataset to be used for predictions

        Notes:
            Uses predefined config to extract the necessary data from Sky Sports and build a dataset

        Returns:
            pd.DataFrame: A DataFrame containing the fixtures for the leagues and seasons specified.

        """
        # Instantiate empty DataFrame
        dataset = pd.DataFrame()

        # Loop through divisions and seasons specified in the config and return results.
        for division in self.config["divisions"]:
            for season in self.config["seasons"]:
                dataset = pd.concat([dataset, self._generate_season(self.config["URL"] + division\
                                                                    + "-results/", season)], ignore_index=True)
        return dataset

    def _generate_season(self, url, season):
        """Generate results for a given season

        Notes:
            The method will be limited to only the last 200 records in (10 matches) in a season.

        Args:
            url (str): Link to the dataset in question
            season (str): Season to obtain results for in the form "yyyy-yy" for previous season.
                Current season requires ""

        Returns:
            pd.DataFrame: DataFrame containing the results for a given season and league.
        """
        # Parse URL
        season_url = url + season

        # Return Page and parse soup
        page = requests.get(season_url)
        soup = BeautifulSoup(page.content, 'html.parser')

        # Generate home and away goals
        home_goals, away_goals = self._parse_goals(soup)
        home_teams, away_teams = self._parse_teams(soup)

        return pd.DataFrame({"home_team": home_teams, "home_goals": home_goals,
                             "away_goals": away_goals, "away_team": away_teams})

    def _parse_goals(self, soup):
        """Products a list of home and away goals scored each season

        Args:
            soup (bs4.BeautifulSoup): Soup scrapped from Sky Sports football results

        Returns:
            tuple: A tuple of lists containing the home goals scored in each fixture and away goals scored in
                each fixtures.
        """
        # Get results from Soup
        result_elems = soup.find_all(class_="matches__teamscores-side")

        # Instantiate empty results
        home_goals = []
        away_goals = []

        # Loop through results appending goals
        for position, result in enumerate(result_elems):
            if position % 2 == 0:
                home_goals.append(result.text.replace("\n", ""))
            else:
                away_goals.append(result.text.replace("\n", ""))

        # Tidy to return integers
        for i in range(len(home_goals)):
            home_goals[i] = int(search(r'\d+', home_goals[i]).group())

        for i in range(len(away_goals)):
            away_goals[i] = int(search(r'\d+', away_goals[i]).group())

        return home_goals, away_goals

    def _parse_teams(self, soup):
        """Products a list of home and away team names for each fixture

        Args:
            soup (bs4.BeautifulSoup): Soup scrapped from Sky Sports football results.

        Returns:
            tuple: A tuple of lists containing the home and away teams contesting each fixture.
        """
        # Extract team names from the soup
        team_elems = soup.find_all(class_="swap-text--bp30")

        # Instantiate empty list for home and away teams
        home_team = []
        away_team = []

        # Parse soup to extract and populate the empty lists
        for position, job in enumerate(team_elems):
            if position % 2 == 0:
                away_team.append(job.text.replace("\n", ""))
            else:
                home_team.append(job.text.replace("\n", ""))

        # Delete the entry at the start which contains the league name
        del away_team[0]

        return home_team, away_team
