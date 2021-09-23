import requests
from bs4 import BeautifulSoup
from scrapy import Selector
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta


class Scrapper:
    def __init__(self):
        pass

    def parse_html(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        result_elems = soup.find_all(class_="trow8")

        return result_elems

    def extract_data(self, result_elems):

        index_start = 0
        number_of_elements = len(result_elems)
        index_end = int(number_of_elements / 6)
        increments = int(number_of_elements / 6)

        league_table = pd.DataFrame(
            [
                result.text.strip("\n").split("\n")
                for result in result_elems[index_start:index_end]
            ],
            columns=["Team", "Played", "Missing", "Result"],
        ).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        form_table = pd.DataFrame(
            [
                result.text.strip("\n").split("\n")
                for result in result_elems[index_start:index_end]
            ],
            columns=["Team", "Played", "Missing", "Result"],
        ).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        home_table = pd.DataFrame(
            [
                result.text.strip("\n").split("\n")
                for result in result_elems[index_start:index_end]
            ],
            columns=["Team", "Played", "Missing", "Result"],
        ).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        away_table = pd.DataFrame(
            [
                result.text.strip("\n").split("\n")
                for result in result_elems[index_start:index_end]
            ],
            columns=["Team", "Played", "Missing", "Result"],
        ).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        goals_scored = pd.DataFrame(
            [
                result.text.strip("\n").split("\n")
                for result in result_elems[index_start:index_end]
            ],
            columns=["Team", "Played", "Missing", "Result"],
        ).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        goals_conceded = pd.DataFrame(
            [
                result.text.strip("\n").split("\n")
                for result in result_elems[index_start:index_end]
            ],
            columns=["Team", "Played", "Missing", "Result"],
        ).drop(["Missing"], axis=1)

        return (
            league_table,
            form_table,
            home_table,
            away_table,
            goals_scored,
            goals_conceded,
        )

    def build_dataset(
        self,
        league_table,
        form_table,
        home_table,
        away_table,
        goals_scored,
        goals_conceded,
    ):

        dataset = {}

        for team in league_table["Team"]:

            if (
                league_table.loc[league_table["Team"] == team, "Played"].values == "0"
                or home_table.loc[home_table["Team"] == team, "Played"].values == "0"
                or away_table.loc[away_table["Team"] == team, "Played"].values == "0"
            ):
                dataset[team] = {
                    "ppg": 0,
                    "recent_ppg": 0,
                    "home_ppg": 0,
                    "away_ppg": 0,
                    "goals_scored": 0,
                    "goals_conceded": 0,
                }
            else:
                dataset[team] = {
                    "ppg": int(league_table[league_table["Team"] == team]["Result"])
                    / int(league_table[league_table["Team"] == team]["Played"]),
                    "recent_ppg": int(form_table[form_table["Team"] == team]["Result"])
                    / int(form_table[form_table["Team"] == team]["Played"]),
                    "home_ppg": int(home_table[home_table["Team"] == team]["Result"])
                    / int(home_table[home_table["Team"] == team]["Played"]),
                    "away_ppg": int(away_table[away_table["Team"] == team]["Result"])
                    / int(away_table[away_table["Team"] == team]["Played"]),
                    "goals_scored": int(
                        goals_scored[goals_scored["Team"] == team]["Result"]
                    )
                    / int(goals_scored[goals_scored["Team"] == team]["Played"]),
                    "goals_conceded": int(
                        goals_conceded[goals_conceded["Team"] == team]["Result"]
                    )
                    / int(goals_conceded[goals_conceded["Team"] == team]["Played"]),
                }

        return dataset

    def build_fixture(self, results_dataframe, home_team, away_team):

        home_team_data = (
            results_dataframe.loc[[home_team], :].reset_index().drop(["index"], axis=1)
        )
        home_team_data.columns = [
            column + "_home_team" for column in results_dataframe.columns.values
        ]

        away_team_data = (
            results_dataframe.loc[[away_team], :].reset_index().drop(["index"], axis=1)
        )
        away_team_data.columns = [
            column + "_away_team" for column in results_dataframe.columns.values
        ]

        fixture = pd.concat([home_team_data, away_team_data], axis=1)

        return fixture

    def _extract_oppg(self, url):
        relative_performance = requests.get(url)

        rp_selector = Selector(relative_performance)

        oppg_values = self._extract_oppg_value(rp_selector)

        oppg_teams = self._extract_oppg_teams(rp_selector)

        oppg = {}

        for oppg_team, oppg_value in zip(oppg_teams, oppg_values):
            oppg[oppg_team] = {"oppg": oppg_value}

        oppg_df = pd.DataFrame.from_dict(oppg, orient="index")

        return oppg_df

    def _extract_oppg_value(self, selector):
        opponent_ppg = selector.xpath('//a[@class="tooltip2"]//b').extract()

        all_oppg = []

        for oppg in opponent_ppg:
            result = re.sub("\D", "", oppg)
            if len(result) == 0:
                pass
            else:
                all_oppg.append(float(result) / 100)

        return all_oppg

    def _extract_oppg_teams(self, selector):
        rp_teams = selector.xpath("//td//a/@title").extract()

        teams_oppg = []

        for rp_team in rp_teams:
            if " stats" in rp_team:
                teams_oppg.append(rp_team.split(" stats")[0])

        return teams_oppg


data_scrapper = Scrapper()

# Get URLs for leagues
leagues = requests.get("https://www.soccerstats.com/leagues.asp")

leagues_sel = Selector(leagues)

links = leagues_sel.xpath("//a/@href").extract()

urls = [
    "https://www.soccerstats.com/" + link for link in links if link[0:6] == "latest"
]

# Build relative performance dataset
rp_urls = [url.replace("latest", "table") + "&tid=rp" for url in urls]

oppg_results = pd.DataFrame(columns=["oppg"])

for url in rp_urls:
    print(f"extracting results for {url}")
    try:
        df = data_scrapper.extract_oppg(url)
        oppg_results = pd.concat([oppg_results, df], axis=0)
    except (ValueError):
        print(f"unable to extract results for {url}")


oppg_results["team"] = oppg_results.index
oppg_results = oppg_results.drop_duplicates()
oppg_results = oppg_results[~oppg_results.index.duplicated(keep="first")]
oppg_results = oppg_results.drop(["team"], axis=1)

# Build main dataset
base_urls = [url.replace("latest", "formtable") for url in urls]

all_results = {}

for url in base_urls:
    print(url)
    result_elems = data_scrapper.parse_html(url)

    if len(result_elems) == 0:
        print("No results for " + url)
        pass

    try:
        (
            league_table,
            form_table,
            home_table,
            away_table,
            goals_scored,
            goals_conceded,
        ) = data_scrapper.extract_data(result_elems)
    except (ValueError):
        print("Unable to parse data for " + url)

    dataset = data_scrapper.build_dataset(
        league_table, form_table, home_table, away_table, goals_scored, goals_conceded
    )
    all_results.update(dataset)

results = pd.DataFrame.from_dict(all_results, orient="index")

results = results.merge(oppg_results, how="left", left_index=True, right_index=True)


# Build dataset for todays results
todays_matches = requests.get("https://www.soccerstats.com/matches.asp?matchday=1")
match_soup = BeautifulSoup(todays_matches.content, "html.parser")

matches_today = match_soup.find_all(class_="steam")

match_home_team = []
match_away_team = []

for i in range(len(matches_today)):
    if i % 2 == 0:
        match_home_team.append(matches_today[i].text)
    else:
        match_away_team.append(matches_today[i].text)

todays_fixtures = pd.DataFrame(
    columns=[
        "ppg_home_team",
        "recent_ppg_home_team",
        "home_ppg_home_team",
        "away_ppg_home_team",
        "goals_scored_home_team",
        "goals_conceded_home_team",
        "oppg_home_team",
        "ppg_away_team",
        "recent_ppg_away_team",
        "home_ppg_away_team",
        "away_ppg_away_team",
        "goals_scored_away_team",
        "goals_conceded_away_team",
        "oppg_away_team",
    ]
)

index = []
bad_results = []
i = 0

for home_team, away_team in zip(match_home_team, match_away_team):
    if home_team in results.index and away_team in results.index:
        result = data_scrapper.build_fixture(results, home_team, away_team)
        todays_fixtures = pd.concat([todays_fixtures, result], ignore_index=True)
        index.append(home_team + " vs " + away_team)
    else:
        bad_results.append(f"Unable to build result for {home_team} vs {away_team}")

todays_fixtures.index = index

dt = datetime.today().strftime("%Y-%m-%d")

todays_fixtures.to_csv(
    "/Users/sam/Documents/projects/premier_league_predictions/data/"
    + str(dt)
    + "_fixtures.csv"
)

results_yesterday = requests.get(
    "https://www.soccerstats.com/matches.asp?matchday=0&daym=yesterday"
)

sel = Selector(results_yesterday)

high_level = sel.xpath("//tr[@height=18]/td[@class='steam']/text()").extract()

scores = sel.xpath("//tr[@height=18]/td[2]").extract()

matchday_scores = []

for score in scores:
    if score.find("<b>(.+?)</b>"):
        try:
            matchday_scores.append(re.search("<b>(.+?)</b>", score).group(1))
        except (AttributeError):
            matchday_scores.append("pp")
    else:
        print("pp")

scores_home_team = []
scores_away_team = []

for i in range(len(matchday_scores)):
    if i % 2 == 0:
        scores_home_team.append(matchday_scores[i])
    else:
        scores_away_team.append(matchday_scores[i])

match_result = []

for home, away in zip(scores_home_team, scores_away_team):
    try:
        if int(home) > int(away):
            match_result.append("home")
        if int(away) > int(home):
            match_result.append("away")
        if int(home) == int(away):
            match_result.append("draw")
    except (ValueError):
        match_result.append("pp")

result_home_team = []
result_away_team = []

for i in range(len(high_level)):
    if i % 2 == 0:
        result_home_team.append(high_level[i])
    else:
        result_away_team.append(high_level[i])

ind = []

for home, away in zip(result_home_team, result_away_team):
    ind.append(home + " vs " + away)

yesterdays_results = pd.DataFrame(match_result, columns=["result"])

yesterdays_results.index = ind

yesterdays_results

yesterday = datetime.now() - timedelta(1)

yesterday = datetime.strftime(yesterday, "%Y-%m-%d")

try:
    yesterdays_fixtures = pd.read_csv(
        "/Users/sam/Documents/projects/premier_league_predictions/data/"
        + yesterday
        + "_fixtures.csv",
        index_col=0,
    )

    yesterdays_fixtures = pd.merge(
        yesterdays_fixtures,
        yesterdays_results,
        left_index=True,
        right_index=True,
        how="left",
    )

    yesterdays_fixtures.to_csv(
        "/Users/sam/Documents/projects/premier_league_predictions/data/"
        + str(yesterday)
        + "_fixtures.csv"
    )
except (FileNotFoundError):
    print(f"Unable to find csv for {yesterday}")

week_31_fixtures = pd.DataFrame(
    columns=[
        "ppg_home_team",
        "recent_ppg_home_team",
        "home_ppg_home_team",
        "away_ppg_home_team",
        "goals_scored_home_team",
        "goals_conceded_home_team",
        "oppg_home_team",
        "ppg_away_team",
        "recent_ppg_away_team",
        "home_ppg_away_team",
        "away_ppg_away_team",
        "goals_scored_away_team",
        "goals_conceded_away_team",
        "oppg_away_team",
    ]
)

week_31_home = [
    "Chelsea",
    "Everton",
    "Leeds Utd",
    "Leicester City",
    "Manchester Utd",
    "Watford",
    "Brentford",
    "Southampton",
    "Arsenal",
    "Blackburn",
]

week_31_away = [
    "Manchester C.",
    "Norwich City",
    "West Ham Utd",
    "Burnley",
    "Aston Villa",
    "Newcastle Utd",
    "Liverpool",
    "Wolverhampton",
    "Tottenham",
    "Cardiff City",
]

rows = []
index = []

for current_week_home, current_week_away in zip(week_31_home, week_31_away):
    rows.append(
        data_scrapper.build_fixture(results, current_week_home, current_week_away)
    )
    index.append(current_week_home + " vs " + current_week_away)

for row in rows:
    week_31_fixtures = week_31_fixtures.append(row)

week_31_fixtures.index = index

week_31_fixtures.to_csv(
    "/Users/sam/Documents/projects/premier_league_predictions/current_gameweek.csv"
)
