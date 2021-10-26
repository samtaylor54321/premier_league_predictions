import numpy as np
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def scrape_data():
    overall = []
    BASE_URL = "https://fbref.com/en/comps/"

    for i in [9, 11, 12, 13]:
        url = BASE_URL + str(i)
        print(url)
        html = requests.get(url, allow_redirects=False)
        soup = BeautifulSoup(html.text, "html.parser")

        table_body = soup.find_all("tbody")
        headers = soup.find_all(
            "th",
            {
                "class": [
                    "poptip sort_default_asc center",
                    "poptip hide_non_quals center",
                    "poptip center",
                    "poptip center group_start",
                ]
            },
        )
        column_names = [x.text for x in headers]

        datasets = []

        for i in range(len(table_body)):

            content = table_body[i].find_all(
                ["td", "th"], {"class": ["left", "right", "right group_start"]}
            )

            table_content = [x.text for x in content]

            number_of_teams = 20

            data = pd.DataFrame(
                np.reshape(
                    table_content,
                    (number_of_teams, int(len(table_content) / number_of_teams)),
                )
            )

            datasets.append(data)

        results = pd.DataFrame()

        for i in range(len(datasets)):
            if i < 2:
                results = pd.concat(
                    [
                        results,
                        datasets[i]
                        .sort_values([1])
                        .reset_index()
                        .drop(["index"], axis=1),
                    ],
                    axis=1,
                    ignore_index=True,
                )
            else:
                results = pd.concat([results, datasets[i]], axis=1, ignore_index=True)

        results.columns = column_names
        overall.append(results)

    final_results = pd.DataFrame()

    for dataframe in overall:
        final_results = pd.concat([final_results, dataframe], ignore_index=True, axis=0)

    # Build dataset for todays matches
    url = "https://fbref.com/en/matches/" + str(
        datetime.strftime(datetime.now(), "%Y-%m-%d")
    )

    todays_matches = requests.get(url)
    match_soup = BeautifulSoup(todays_matches.content, "html.parser")

    home = [
        (x.text, x.find("a")["href"])
        for x in match_soup.find_all(attrs={"data-stat": "squad_a"})
        if x.text != "Home" and x.find("a") is not None
    ]
    away = [
        (x.text, x.find("a")["href"])
        for x in match_soup.find_all(attrs={"data-stat": "squad_b"})
        if x.text != "Away" and x.find("a") is not None
    ]

    home = [x[0] for x in home if x[1].find("U23") == -1]
    away = [x[0] for x in away if x[1].find("U23") == -1]

    todays_matches = pd.DataFrame({"home": home, "away": away})

    final_results.index = [x.strip() for x in final_results.iloc[:, 1].values]
    todays_matches = todays_matches.merge(
        final_results, how="inner", left_on="home", right_index=True
    )
    todays_matches = todays_matches.merge(
        final_results, how="inner", left_on="away", right_index=True
    )

    todays_matches.to_csv(
        "/opt/airflow/data/"
        + str(datetime.strftime(datetime.now(), "%Y-%m-%d"))
        + "_fixtures_new.csv",
        index=False,
    )

    # Build dataset for yesterdays results
    url = "https://fbref.com/en/matches/" + str(
        datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d")
    )

    yesterdays_results = requests.get(url)
    result_soup = BeautifulSoup(yesterdays_results.content, "html.parser")

    home = [
        x.text
        for x in result_soup.find_all(attrs={"data-stat": "squad_a"})
        if x.text != "Home"
    ]
    away = [
        x.text
        for x in result_soup.find_all(attrs={"data-stat": "squad_b"})
        if x.text != "Away"
    ]
    score = [
        x.text
        for x in result_soup.find_all(attrs={"data-stat": "score"})
        if x.text != "Score"
    ]

    yesterdays_results = pd.DataFrame({"home": home, "away": away, "score": score})

    yesterdays_results[["home_score", "away_score"]] = yesterdays_results[
        "score"
    ].str.split("–", expand=True)

    outcomes = []

    for i in range(yesterdays_results.shape[0]):
        try:
            if int(yesterdays_results.iloc[i, 3]) > int(yesterdays_results.iloc[i, 4]):
                outcomes.append("home")
            elif int(yesterdays_results.iloc[i, 3]) < int(
                yesterdays_results.iloc[i, 4]
            ):
                outcomes.append("away")
            else:
                outcomes.append("draw")
        except ValueError:
            outcomes.append("no result")

    yesterdays_results["result"] = outcomes

    try:
        yesterdays_fixtures = pd.read_csv(
            "/opt/airflow/data/"
            + str(datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d"))
            + "_fixtures_new.csv"
        )

        if "result" in yesterdays_fixtures.columns:
            print("results from yesterday already populated")
        else:
            yesterdays_fixtures = yesterdays_fixtures.merge(
                yesterdays_results[["home", "away", "score"]],
                how="inner",
                left_on=["home", "away"],
                right_on=["home", "away"],
            )

            yesterdays_fixtures.to_csv(
                "/opt/airflow/data/"
                + str(datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d"))
                + "_fixtures_new.csv",
                index=False,
            )

    except (FileNotFoundError, KeyError):
        print("No file found")


with DAG(
    "Premier_League_Predictions_New_Features",
    default_args=default_args,
    description="Scrape data for making predictions about the premier league",
    start_date=datetime(2021, 10, 26),
    schedule_interval="30 12 * * *",
    tags=["prem_preds"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id="scrape_data",
        python_callable=scrape_data,
    )

    # t2 = PythonOperator(task_id="train_model", python_callable=train_model)

t1
