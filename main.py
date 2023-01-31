import numpy as np
import pandas as pd
import joblib
from flask import Flask, request, render_template
from google.cloud import storage

BUCKET_NAME = "premier-league-predictions"
MODEL_NAME = "model_pipeline.pkl"

client = storage.Client()

# Download the model into memory
bucket = client.get_bucket(BUCKET_NAME)
blob = bucket.blob(MODEL_NAME)
blob.download_to_filename(MODEL_NAME)

with open(MODEL_NAME, "rb") as fo:
    model_pipeline = joblib.load(fo)

# Download the team database into memory
team_database = pd.read_csv(f"gs://{BUCKET_NAME}/team-database.csv")

team_database.set_index("Unnamed: 0_level_0_Squad", inplace=True)

# Ensure that columns are of the correct data type
for column in team_database.columns:
    if (team_database[column].dtype == "object") and (column != "Outcome"):
        team_database[column] = team_database[column].astype("float64")

# Generate flask app
app = Flask(__name__)


@app.route("/home")
def home_page():
    """Render HTML for Webpage"""
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    """Method for generating predictions from the model"""
    # Parse team names from request
    home_name, away_name = [str(x) for x in request.form.values()]

    # Lookup values in the team database
    home = team_database.loc[team_database.index == home_name, :]
    away = team_database.loc[team_database.index == away_name, :]

    # Preprocess dataset and make predictions
    data = np.concatenate((home.values, away.values), axis=None)
    pred = model_pipeline.predict(data.reshape(1, -1))

    return render_template("index.html", prediction_text="{}".format(pred[0].title()))


if __name__ == "__main__":
    app.run(port=8080)
