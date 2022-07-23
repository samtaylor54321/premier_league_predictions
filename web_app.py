import tempfile
import boto3
import joblib
import awswrangler as wr
import numpy as np

from flask import Flask, request, jsonify, render_template

BUCKET_NAME = "premierleaguepredictions"

app = Flask(__name__)
app.debug = True


# def get_keys():
#     """Collect AWS Credentials

#     Gather access key and secret access key for AWS account

#     Returns:
#         tuple: Access Key and Secret Access Key for SSMs
#     """
#     ssm = boto3.client("ssm", "eu-west-2")

#     access_key_value = ssm.get_parameter(Name="ACCESS_KEY", WithDecryption=True)

#     secret_access_key_value = ssm.get_parameter(
#         Name="SECRET_ACCESS_KEY", WithDecryption=True
#     )

#     return (
#         access_key_value["Parameter"]["Value"],
#         secret_access_key_value["Parameter"]["Value"],
#     )


# Get access keys for AWS
# access_key, secret_access_key = get_keys()

# Instantiate boto3 session
session = boto3.Session(
    #    aws_access_key_id=access_key,
    #    aws_secret_access_key=secret_access_key,
    region_name="eu-west-2",
)

# Read pipeline into memory
with tempfile.TemporaryFile() as fp:
    boto3.client("s3").download_fileobj(
        Fileobj=fp, Bucket=BUCKET_NAME, Key="pipeline.pkl"
    )
    fp.seek(0)
    pipe = joblib.load(fp)

# Load model into memory
with tempfile.TemporaryFile() as fp:
    boto3.client("s3").download_fileobj(
        Fileobj=fp, Bucket=BUCKET_NAME, Key="premier-league-predictions-model"
    )
    fp.seek(0)
    model = joblib.load(fp)

# Pull in data from S3
team_database = wr.s3.read_csv(
    path="s3://premierleaguepredictions/team_database.csv",
    boto3_session=session,
    index_col=0,
)

# Extract top scorer to feed into model
top_scorers = team_database["top_team_scorers"].astype("category")

# Drop columns from team database
cols_to_drop = []

for col in team_database.columns:
    if team_database[col].dtype == "object":
        cols_to_drop.append(col)

    if "notes" in col:
        cols_to_drop.append(col)

    cols_to_drop.append("points_avg")

team_database = team_database.drop(cols_to_drop, axis=1)


@app.route("/home")
def home():
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
    data = pipe.transform([data])
    pred = model.predict([data, top_scorers.cat.codes[home_name].reshape((1))])

    if np.argmax(pred) == 0:
        pred = "Away Win"
    elif np.argmax(pred) == 1:
        pred = "Draw"
    else:
        pred = "Home Win"

    return render_template("index.html", prediction_text="{}".format(pred))


@app.route("/results", methods=["POST"])
def results():
    """Return results back to user"""
    # Parse result and transform
    data = request.get_json(force=True)

    # Make prediction
    prediction = model.predict([np.array(list(data.values()))])
    output = prediction[0]

    return jsonify(output)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4444, debug=True)
