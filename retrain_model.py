from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler
from tensorflow import keras

import awswrangler as wr
import boto3
import joblib
import math
import os
import tempfile
import time

K = keras.backend
BUCKET_NAME = "premierleaguepredictions"


def get_keys():
    """Pull keys from AWS

    Returns:
        tuple: AWS Access Key and AWS Secret Access Key
    """
    ssm = boto3.client("ssm", "eu-west-2")

    access_key = ssm.get_parameter(Name="ACCESS_KEY", WithDecryption=True)

    secret_access_key = ssm.get_parameter(Name="SECRET_ACCESS_KEY", WithDecryption=True)

    return access_key["Parameter"]["Value"], secret_access_key["Parameter"]["Value"]


def get_run_logdir():
    """Generate log directory

    Returns:
        os.path: generated log file directory
    """
    run_id = time.strftime("run_%Y_%m_%d-%H_%M_%S")
    return os.path.join(root_logdir, run_id)


class OneCycleScheduler(keras.callbacks.Callback):
    def __init__(
        self,
        iterations,
        max_rate,
        start_rate=None,
        last_iterations=None,
        last_rate=None,
    ):
        self.iterations = iterations
        self.max_rate = max_rate
        self.start_rate = start_rate or max_rate / 10
        self.last_iterations = last_iterations or iterations // 10 + 1
        self.half_iteration = (iterations - self.last_iterations) // 2
        self.last_rate = last_rate or self.start_rate / 1000
        self.iteration = 0

    def _interpolate(self, iter1, iter2, rate1, rate2):
        return (rate2 - rate1) * (self.iteration - iter1) / (iter2 - iter1) + rate1

    def on_batch_begin(self, batch, logs):
        if self.iteration < self.half_iteration:
            rate = self._interpolate(
                0, self.half_iteration, self.start_rate, self.max_rate
            )
        elif self.iteration < 2 * self.half_iteration:
            rate = self._interpolate(
                self.half_iteration,
                2 * self.half_iteration,
                self.max_rate,
                self.start_rate,
            )
        else:
            rate = self._interpolate(
                2 * self.half_iteration,
                self.iterations,
                self.start_rate,
                self.last_rate,
            )
        self.iteration += 1
        K.set_value(self.model.optimizer.learning_rate, rate)


# Configure logging
root_logdir = os.path.join(os.curdir, "logs")

run_logdir = get_run_logdir()

# Pull access keys and instantiate session
access_key, secret_access_key = get_keys()
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    region_name="eu-west-2",
)

# Read training data into memory
training_data = wr.s3.read_csv(
    path="s3://premierleaguepredictions/training_data.csv",
    boto3_session=session,
    index_col=0,
)

# Generate labels from training data
encoder = LabelEncoder()
y = encoder.fit_transform(training_data.result)

# Extract top scorer to feed into model
top_scorer = training_data["top_team_scorers_x"].astype("category")
top_scorer_id = top_scorer.cat.codes

# Preprocess string columns in the dataset
cols_to_drop = []

for col in training_data.columns:
    if training_data[col].dtype == "object":
        cols_to_drop.append(col)

    if "notes" in col:
        cols_to_drop.append(col)

training_data = training_data.drop(cols_to_drop, axis=1)

# Generate pipeline and apply to training data
pipe = Pipeline(
    [
        ("imp", SimpleImputer()),
        ("scaler", StandardScaler()),
    ]
)
training_data = pipe.fit_transform(training_data)

# Write pipeline to s3
with tempfile.TemporaryFile() as fp:
    joblib.dump(pipe, fp)
    fp.seek(0)
    boto3.client("s3").put_object(
        Body=fp.read(), Bucket=BUCKET_NAME, Key="pipeline.pkl"
    )

# Instantiate scheduler for training model
onecycle = OneCycleScheduler(math.ceil(len(training_data) / 32) * 100, max_rate=0.05)

# Create model summary

# Generate text features
text_input = keras.Input(shape=(1,))
input_layer = keras.layers.Input(shape=training_data.shape[1:])

embed_layer = keras.layers.Embedding(
    input_dim=training_data.shape[0], input_length=1, output_dim=1
)(text_input)
flatten_text_layer = keras.layers.Flatten()(embed_layer)

dense_layer_1 = keras.layers.Dense(
    30,
    activation="elu",
    kernel_initializer="he_normal",
    kernel_regularizer=keras.regularizers.l2(0.01),
)(input_layer)
dense_layer_2 = keras.layers.Dense(
    30,
    activation="elu",
    kernel_initializer="he_normal",
    kernel_regularizer=keras.regularizers.l2(0.01),
)(dense_layer_1)
dense_layer_3 = keras.layers.Dense(
    30,
    activation="elu",
    kernel_initializer="he_normal",
    kernel_regularizer=keras.regularizers.l2(0.01),
)(dense_layer_2)
dense_layer_4 = keras.layers.Dense(
    30,
    activation="elu",
    kernel_initializer="he_normal",
    kernel_regularizer=keras.regularizers.l2(0.01),
)(dense_layer_3)

concat_layer = keras.layers.Concatenate()([dense_layer_4, flatten_text_layer])

output_layer = keras.layers.Dense(3, activation="softmax")(dense_layer_4)

# Instantiate model and compile
model = keras.Model(inputs=[input_layer, text_input], outputs=[output_layer])
optimizer = keras.optimizers.SGD(momentum=0.9, nesterov=True)
model.compile(
    loss="sparse_categorical_crossentropy", optimizer="nadam", metrics=["accuracy"]
)

# Fit model to training data
model.fit(
    [training_data, top_scorer_id.values],
    y,
    epochs=100,
    validation_split=0.3,
    callbacks=[
        keras.callbacks.EarlyStopping(patience=5, restore_best_weights=True),
        onecycle,
    ],
    batch_size=32,
)

# Write model to s3
with tempfile.TemporaryFile() as fp:
    joblib.dump(model, fp)
    fp.seek(0)
    boto3.client("s3").put_object(
        Body=fp.read(), Bucket=BUCKET_NAME, Key="premier-league-predictions-model"
    )
