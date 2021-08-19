import scrapy
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

    def start_requests(self):
        leagues = requests.get("https://www.soccerstats.com/leagues.asp")

        leagues_sel = Selector(leagues)

        links = leagues_sel.xpath("//a/@href").extract()

        urls = ["https://www.soccerstats.com/" + link for link in links if link[0:6] == "latest"]

        urls = [url.replace("latest", "formtable") for url in urls]

        for url in urls:
            print("Getting results for " + url)
            yield scrapy.request(url=url, callback=self.parse_fixtures)

    def parse_fixtures(self, response):
        soup = BeautifulSoup(response.content, 'html.parser')
        result_elems = soup.find_all(class_="trow8")

        all_results = {}

        for url in urls:
            print(url)
            result_elems = parse_html(url)

            if len(result_elems) == 0:
                print("No results for " + url)
                pass

            try:
                league_table, form_table, home_table, \
                    away_table, goals_scored, goals_conceded = self._extract_data(result_elems)
            except(ValueError):
                print("Unable to parse data for " + url)

            dataset = build_dataset(league_table, form_table, home_table, away_table, goals_scored, goals_conceded)
            all_results.update(dataset)

        return result_elems

    def _extract_data(self, result_elems):
        index_start = 0
        number_of_elements = len(result_elems)
        index_end = int(number_of_elements / 6)
        increments = int(number_of_elements / 6)

        league_table = pd.DataFrame([result.text.strip("\n").split("\n") for result in result_elems[index_start:index_end]],
                                    columns=["Team", "Played", "Missing", "Result"]).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        form_table = pd.DataFrame([result.text.strip("\n").split("\n") for result in result_elems[index_start:index_end]],
                                  columns=["Team", "Played", "Missing", "Result"]).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        home_table = pd.DataFrame([result.text.strip("\n").split("\n") for result in result_elems[index_start:index_end]],
                                  columns=["Team", "Played", "Missing", "Result"]).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        away_table = pd.DataFrame([result.text.strip("\n").split("\n") for result in result_elems[index_start:index_end]],
                                  columns=["Team", "Played", "Missing", "Result"]).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        goals_scored = pd.DataFrame([result.text.strip("\n").split("\n") for result in result_elems[index_start:index_end]],
                                    columns=["Team", "Played", "Missing", "Result"]).drop(["Missing"], axis=1)

        index_start += increments
        index_end += increments

        goals_conceded = pd.DataFrame(
            [result.text.strip("\n").split("\n") for result in result_elems[index_start:index_end]],
            columns=["Team", "Played", "Missing", "Result"]).drop(["Missing"], axis=1)

        return league_table, form_table, home_table, away_table, goals_scored, goals_conceded

    def build_dataset(self, league_table, form_table, home_table, away_table, goals_scored, goals_conceded):
        dataset = {}

        for team in league_table["Team"]:

            if (league_table.loc[league_table["Team"] == team, "Played"].values == "0" or
                    home_table.loc[home_table["Team"] == team, "Played"].values == "0" or
                    away_table.loc[away_table["Team"] == team, "Played"].values == "0"):
                dataset[team] = {"ppg": 0,
                                 "recent_ppg": 0,
                                 "home_ppg": 0,
                                 "away_ppg": 0,
                                 "goals_scored": 0,
                                 "goals_conceded": 0
                                 }
            else:
                dataset[team] = {"ppg": int(league_table[league_table["Team"] == team]["Result"]) / int(
                    league_table[league_table["Team"] == team]["Played"]),
                                 "recent_ppg": int(form_table[form_table["Team"] == team]["Result"]) / int(
                                     form_table[form_table["Team"] == team]["Played"]),
                                 "home_ppg": int(home_table[home_table["Team"] == team]["Result"]) / int(
                                     home_table[home_table["Team"] == team]["Played"]),
                                 "away_ppg": int(away_table[away_table["Team"] == team]["Result"]) / int(
                                     away_table[away_table["Team"] == team]["Played"]),
                                 "goals_scored": int(goals_scored[goals_scored["Team"] == team]["Result"]) / int(
                                     goals_scored[goals_scored["Team"] == team]["Played"]),
                                 "goals_conceded": int(goals_conceded[goals_conceded["Team"] == team]["Result"]) / int(
                                     goals_conceded[goals_conceded["Team"] == team]["Played"])
                                 }

        return dataset

    def fixture_builder(self, results_dataframe, home_team, away_team):

        home_team_data = results_dataframe.loc[[home_team], :].reset_index().drop(["index"], axis=1)
        home_team_data.columns = [column + "_home_team" for column in results_dataframe.columns.values]

        away_team_data = results_dataframe.loc[[away_team], :].reset_index().drop(["index"], axis=1)
        away_team_data.columns = [column + "_away_team" for column in results_dataframe.columns.values]

        fixture = pd.concat([home_team_data, away_team_data], axis=1)

        return fixture



