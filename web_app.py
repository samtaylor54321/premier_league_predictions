import numpy as np
import pandas as pd
import pickle
from flask import Flask, request, jsonify, render_template


app = Flask(__name__)

clf = pickle.load(open("./model/clf.pkl", "rb"))
pipeline = pickle.load(open("./model/pipeline.pkl", "rb"))
team_database = pd.read_csv("./data/team_database.csv", index_col=0)


@app.route("/home")
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    home, away = [str(x) for x in request.form.values()]

    home = team_database.loc[
        team_database.index == home, team_database.columns != "away_ppg"
    ]
    away = team_database.loc[
        team_database.index == away, team_database.columns != "home_ppg"
    ]

    data = np.concatenate((home.values, away.values), axis=None)

    data = pipeline.transform([data])
    prediction = clf.predict(data)

    # Format prediction
    if prediction == 0:
        output = "home"
    elif prediction == 1:
        output = "away"
    else:
        output = "draw"

    return render_template(
        "index.html",
        prediction_text="Predicted to be a {} win".format(output),
    )


@app.route("/results", methods=["POST"])
def results():
    # Parse result and transform
    data = request.get_json(force=True)
    data = pipeline.transform(data)

    # Make prediction
    prediction = clf.predict([np.array(list(data.values()))])
    output = prediction[0]

    return jsonify(output)


if __name__ == "__main__":
    app.run(debug=True)
