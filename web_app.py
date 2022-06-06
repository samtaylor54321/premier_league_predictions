import awswrangler as wr
import boto3
import joblib
import pickle
import numpy as np
import pandas as pd
import pickle
import tempfile

from flask import Flask, request, jsonify, render_template
from tensorflow import keras

BUCKET_NAME = "premierleaguepredictions"

app = Flask(__name__)
app.debug = True


def get_keys():
    ssm = boto3.client("ssm", "eu-west-2")

    access_key = ssm.get_parameter(Name="ACCESS_KEY", WithDecryption=True)

    secret_access_key = ssm.get_parameter(Name="SECRET_ACCESS_KEY", WithDecryption=True)

    return access_key["Parameter"]["Value"], secret_access_key["Parameter"]["Value"]


# Get access keys for AWS
access_key, secret_access_key = get_keys()
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    region_name="eu-west-2",
)

# Pull in data from S3
team_database = wr.s3.read_csv(
    path="s3://premierleaguepredictions/team_database.csv",
    boto3_session=session,
    index_col=0,
)

# Drop columns from team database
cols_to_drop = []

for col in team_database.columns:
    if team_database[col].dtype == "object":
        cols_to_drop.append(col)

    if "notes" in col:
        cols_to_drop.append(col)

team_database = team_database.drop(cols_to_drop, axis=1)


s3_client = boto3.client("s3")
key = "pipeline.pkl"

# READ
with tempfile.TemporaryFile() as fp:
    s3_client.download_fileobj(Fileobj=fp, Bucket=BUCKET_NAME, Key="pipeline.pkl")
    fp.seek(0)
    pipe = joblib.load(fp)

# Load model
with tempfile.TemporaryFile() as fp:
    s3_client.download_fileobj(
        Fileobj=fp, Bucket=BUCKET_NAME, Key="premier-league-predictions-model"
    )
    fp.seek(0)
    model = joblib.load(fp)


@app.route("/home")
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    home_name, away_name = [str(x) for x in request.form.values()]

    home = team_database.loc[team_database.index == home_name, :]
    away = team_database.loc[team_database.index == away_name, :]

    data = np.concatenate((home.values, away.values), axis=None)

    data = pipe.transform([data])

    pred = model.predict(data)

    return render_template("index.html", prediction_text="{}".format(pred))


@app.route("/results", methods=["POST"])
def results():
    # Parse result and transform
    data = request.get_json(force=True)

    # Make prediction
    prediction = model.predict([np.array(list(data.values()))])
    output = prediction[0]

    return jsonify(output)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4444, debug=True)
