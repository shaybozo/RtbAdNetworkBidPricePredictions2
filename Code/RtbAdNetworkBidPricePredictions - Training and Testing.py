# Databricks notebook source
import tensorflow as tf
from tensorflow.keras import layers
import pandas as pd
from pandas import DataFrame
from pandas import Series
from pandas import DatetimeIndex
from pandas import concat

import matplotlib.pyplot as plt
import sklearn as sk
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error
from numpy import concatenate

from tensorflow import keras
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.layers import Dense
from tensorflow.python.keras.layers import LSTM

log_dir = "/tmp/tensorflow_log_dir"
dbutils.tensorboard.start(log_dir)

# COMMAND ----------

train_file = '/dbfs/mnt/ia-staging-test/MlTrain/part-00000-tid-7115613127019349065-510d18c8-a25c-4669-b1fb-ac734c593359-239727-c000.csv'
test_file = '/dbfs/mnt/ia-staging-test/MlTest/part-00000-tid-8823567470948334527-61bdfb3b-1c37-48ac-ad71-576135138458-240274-c000.csv'
model_directory = '/dbfs/mnt/ia-staging-test/MlModel'

# COMMAND ----------

import numpy as np
import pandas as pd

train_dataset = pd.read_csv(train_file, header=0, index_col=False)
test_dataset = pd.read_csv(test_file, header=0, index_col=False)

train_dataset = train_dataset.dropna()
test_dataset = test_dataset.dropna()

# COMMAND ----------

# Under the hood, each of the partitions is fully loaded in memory, which may be expensive.
# This ensure that each of the paritions has a small size.
train_dataset = train_dataset.repartition(100)
test_dataset = test_dataset.repartition(100)

# COMMAND ----------

train_stats = train_dataset.describe()
train_stats.pop("actualBidPrice")
train_stats = train_stats.transpose()

# COMMAND ----------

mapped.isna().sum()
train_labels = train_dataset.pop('actualBidPrice')
test_labels = test_dataset.pop('actualBidPrice')

def build_model():
  model = keras.Sequential([
    layers.Dense(64, activation='relu', input_shape=[len(train_dataset.keys())]),
    layers.Dense(64, activation='relu'),
    layers.Dense(1)
  ])

  optimizer = tf.keras.optimizers.RMSprop(0.001)

  model.compile(loss='mse',
                optimizer=optimizer,
                metrics=['mae', 'mse'])
  return model

model = build_model()


# COMMAND ----------

model = build_model()

# COMMAND ----------

model.summary()

# COMMAND ----------

example_batch = normed_train_data[:10]
example_result = model.predict(example_batch)
example_result

# COMMAND ----------

class PrintDot(keras.callbacks.Callback):
  def on_epoch_end(self, epoch, logs):
    if epoch % 100 == 0: print('')
    print('.', end='')

EPOCHS = 100

history = model.fit(
  normed_train_data, train_labels,
  epochs=EPOCHS, validation_split = 0.2, verbose=0,
  callbacks=[PrintDot()])

# COMMAND ----------

hist = pd.DataFrame(history.history)
hist['epoch'] = history.epoch
display(hist)
