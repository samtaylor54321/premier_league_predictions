import numpy as np
import pandas as pd
import pickle
import re
from flask import Flask, request, jsonify, render_template


app = Flask(__name__)
app.debug = True

clf = pickle.load(open("./model/clf_new.pkl", "rb"))
pipe = pickle.load(open("./model/pipeline_new.pkl", "rb"))
team_database = pd.read_csv("./data/team_database_new.csv", index_col=0)


@app.route("/home")
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    #   try:
    home_name, away_name = [str(x) for x in request.form.values()]

    home = team_database.loc[team_database.index == home_name, :]
    away = team_database.loc[team_database.index == away_name, :]

    data = np.concatenate((home.values, away.values), axis=None)

    data = pipe.transform([data])

    pred = clf.predict(data)

    if pred == 0:
        output = "Home"
    elif pred == 1:
        output = "Away"
    else:
        output = "Draw"

    # output = f"{home_name} - {round(probs[0][0], 2) * 100}% / {away_name} - {round(probs[0][1], 2) * 100}% / Draw - {round(probs[0][2], 2) * 100}%"

    return render_template("index.html", prediction_text="{}".format(output))


#   except ValueError:
#       return f"Whoops! Unable to process results for {home_name} vs {away_name}"


@app.route("/results", methods=["POST"])
def results():
    # Parse result and transform
    data = request.get_json(force=True)

    # Make prediction
    prediction = clf.predict([np.array(list(data.values()))])
    output = prediction[0]

    return jsonify(output)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
