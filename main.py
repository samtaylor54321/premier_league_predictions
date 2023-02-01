import numpy as np
import pandas as pd
import joblib
import tempfile
import os
from flask import Flask, request, render_template
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from google.cloud import storage

BUCKET_NAME = os.getenv("BUCKET_NAME")
MODEL_NAME = os.getenv("MODEL_NAME")

# Download the team database into memory
team_database = pd.read_csv(f"gs://{BUCKET_NAME}/team-database.csv")

team_database.set_index("Unnamed: 0_level_0_Squad", inplace=True)

# Ensure that columns are of the correct data type
for column in team_database.columns:
    if (team_database[column].dtype == "object") and (column != "Outcome"):
        team_database[column] = team_database[column].astype("float64")

# Download the model into memory
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)
blob = bucket.blob(MODEL_NAME)
tmpdir = tempfile.gettempdir()
blob.download_to_filename(f"{tmpdir}/{MODEL_NAME}")

with open(f"{tmpdir}/{MODEL_NAME}", "rb") as fo:
    model_pipeline = joblib.load(fo)

# Generate flask app
app = Flask(__name__)
auth = HTTPBasicAuth()

users = {os.getenv("API_USER"): generate_password_hash(os.environ.get("API_PASSWORD"))}


@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


@app.route("/")
@auth.login_required
def index():
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
    app.run(host="0.0.0.0", port=8080)
