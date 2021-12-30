import numpy as np
import pandas as pd
import pathlib
import pickle
import re
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import LeaveOneOut
from sklearn.ensemble import GradientBoostingClassifier

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "Premier_League_Predictions_New_Features",
    default_args=default_args,
    description="Scrape data for making predictions about the premier league",
    start_date=datetime(2021, 10, 26),
    schedule_interval="30 12 * * *",
    tags=["prem_preds"],
) as dag:

    def scrape_data():
        from bs4 import BeautifulSoup

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

            for _ in range(len(table_body)):

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

            for k in range(len(datasets)):
                if k < 2:
                    results = pd.concat(
                        [
                            results,
                            datasets[k]
                            .sort_values([1])
                            .reset_index()
                            .drop(["index"], axis=1),
                        ],
                        axis=1,
                        ignore_index=True,
                    )
                else:
                    results = pd.concat(
                        [results, datasets[k]], axis=1, ignore_index=True
                    )

            results.columns = column_names
            overall.append(results)

        final_results = pd.DataFrame()

        for dataframe in overall:
            final_results = pd.concat(
                [final_results, dataframe], ignore_index=True, axis=0
            )

        final_results.index = [x.strip() for x in final_results.iloc[:, 1].values]

        columns_to_be_dropped = [
            0,
            16,
            17,
            18,
            19,
            20,
            47,
            74,
            101,
            122,
            143,
            171,
            199,
            219,
            239,
            264,
            289,
            317,
            345,
            364,
            383,
            409,
            435,
            462,
            489,
            511,
            533,
            552,
        ]

        final_results = final_results.drop(
            final_results.columns[columns_to_be_dropped], axis=1
        )

        final_results["Last 5"] = final_results["Last 5"].apply(
            lambda x: (x.count("W") * 3 + x.count("D") * 1) / 15
        )

        final_results["Attendance"] = final_results["Attendance"].apply(
            lambda x: int("".join(re.findall(r"\d", x)))
        )

        final_results.to_csv("/opt/airflow/data/team_database_new.csv", index=True)

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
                if int(yesterdays_results.iloc[i, 3]) > int(
                    yesterdays_results.iloc[i, 4]
                ):
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
                    yesterdays_results[["home", "away", "result"]],
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

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id="scrape_data",
        python_callable=scrape_data,
    )

    def train_model():
        results = pd.DataFrame()

        for path in pathlib.Path("./data/").rglob("*_fixtures_new.csv"):
            data = pd.read_csv(path)
            if "result" in data.columns:
                string_columns = data.loc[:, data.dtypes == "object"].columns
                for column in string_columns:
                    if column in ["home", "away", "result"]:
                        continue
                    else:
                        data[column] = [int(x.replace(",", "")) for x in data[column]]
                data = data.drop(["home", "away"], axis=1)
                results = pd.concat([results, data])

        if results.shape[0] == 0:
            print("No results to process")
        else:
            results = results[results["result"] != "no result"]

            y = []

            for result in results["result"]:
                if result == "home":
                    y.append(0)
                elif result == "away":
                    y.append(1)
                else:
                    y.append(2)

            X = results.loc[:, results.columns.values != "result"]

            for col in X.columns:
                if X[col].dtype == "O" and (col != "home" and col != "away"):
                    X[col] = X[col].apply(lambda x: int("".join(re.findall(r"\d", x))))

            y = np.asarray(y)

            pipe = Pipeline(
                [
                    ("imp", SimpleImputer(missing_values=np.nan, fill_value=0)),
                    ("scaler", StandardScaler()),
                ]
            )
            clf = GradientBoostingClassifier(
                n_estimators=500,
                learning_rate=1.0,
                max_depth=3,
                random_state=0,
                max_features="sqrt",
            )
            loo = LeaveOneOut()

            result = []

            for train_index, test_index in loo.split(X):
                X_train = X.iloc[train_index, :]
                X_test = X.iloc[test_index, :]
                y_train = y[train_index]
                # y_test = y[test_index]

                X_train = pipe.fit_transform(X_train)
                X_test = pipe.transform(X_test)

                X_train = np.float32(X_train)
                X_test = np.float32(X_test)

                clf.fit(X_train, y_train)
                pred = clf.predict(X_test)
                result.append(pred)

            X = pipe.fit_transform(X)
            clf.fit(X, y)

            # Output Model
            try:
                pickle.dump(clf, open("/opt/airflow/model/clf_new.pkl", "wb"))
                pickle.dump(pipe, open("/opt/airflow/model/pipeline_new.pkl", "wb"))
                print(f"Model Performance: {np.mean(result)}")
                print("Model objections successfully pickled")
            except Exception:
                print("Unable to output pickled objects")

    t2 = PythonOperator(task_id="train_model", python_callable=train_model)

t1 >> t2
