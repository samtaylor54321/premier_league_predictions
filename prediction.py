import numpy as np
import pandas as pd
import pickle
import pathlib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer

# Parse results data
results = pd.DataFrame()

for path in pathlib.Path(
    "/Users/sam/Documents/projects/premier_league_predictions/data"
).rglob("*.csv"):
    data = pd.read_csv(path)
    results = pd.concat([results, data])

# Remove missing values
results = results.drop(
    ["Unnamed: 0", "result_x", "result_y", "away_ppg_home_team", "home_ppg_away_team"],
    axis=1,
)

results = results[results.result.values != "pp"]
results = results[~pd.isnull(results.result.values)]

X = results.loc[:, results.columns != "result"].values

y = results.iloc[:, -1].values

y_values = []

for value in y:
    if value == "home":
        y_values.append(0)
    elif value == "away":
        y_values.append(1)
    else:
        y_values.append(2)

y = y_values

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.33, random_state=42, shuffle=True
)

imp = SimpleImputer(missing_values=np.nan, strategy="mean")

imp.fit(X_train)
X_train = imp.transform(X_train)
X_test = imp.transform(X_test)


clf = GradientBoostingClassifier(
    learning_rate=1, max_depth=2, n_estimators=1000, random_state=0
)

clf.fit(X_train, y_train)

y_preds = clf.predict(X_test)

np.mean(np.asarray(y_test) == y_preds)

# Make predictions for this week
this_week = pd.read_csv(
    "/opt/airflow/data/current_gameweek.csv",
    index_col=0,
)

preds = []

this_week = this_week.drop(["away_ppg_home_team", "home_ppg_away_team"], axis=1)

this_week = imp.transform(this_week)

for result in clf.predict(this_week):
    if result == 0:
        preds.append("home")
    elif result == 1:
        preds.append("away")
    else:
        preds.append("draw")

print(preds)

# Output Model
pickle.dump(clf, open("model.pkl", "wb"))
