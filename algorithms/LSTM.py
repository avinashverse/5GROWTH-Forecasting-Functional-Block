#!/usr/bin/env python
# coding: utf-8

# In[1]:


import seaborn as sns
import csv
import tensorflow as tf
import pandas as pd
import time
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pylab as plt
import matplotlib.dates as mdates
import hashlib
from tqdm import tqdm

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras import Sequential, optimizers
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.metrics import mean_squared_error


TIME_STEPS = 72  # 6 hours
BATCH_SIZE = 20


class LSTM:
    def __init__(self):
        self.model = Sequential()
        self.min_max_scaler = MinMaxScaler()

    def buildTimeseries(self, mat, y_col_index):
        # y_col_index is the index of column that would act as output column
        # total number of time-series samples would be len(mat) - TIME_STEPS
        dim_0 = mat.shape[0] - TIME_STEPS
        dim_1 = mat.shape[1]
        print("dim_0", dim_0)
        print("length of time of mat, time-series", len(mat), (len(mat) - TIME_STEPS))  # kote
        x = np.zeros((dim_0, TIME_STEPS, dim_1))
        y = np.zeros((dim_0,))
        print("length of time of x and y", len(x), len(y))  # kote
        for i in tqdm(range(dim_0)):
            x[i] = mat[i:TIME_STEPS + i]
            # print(i,"-->", x[i,-1,:])
            y[i] = mat[TIME_STEPS + i, y_col_index]

        print("length of time-series i/o", x.shape, y.shape)
        return x, y

    def parseData(self, file):
        #df = pd.read_csv(f'C:/Users/User/Documents/5GROWTH/LSTM_DES_TES_code/torino-traffic.csv')
        df = pd.read_csv(file)
        df.head()

        gen_time_col = pd.to_datetime(df['generation_time'])
        df['day_of_week'] = gen_time_col.dt.dayofweek
        df['hour'] = gen_time_col.dt.hour
        df['minute'] = gen_time_col.dt.minute
        df['month'] = gen_time_col.dt.month
        df['day'] = gen_time_col.dt.day
        df['year'] = gen_time_col.dt.year

        df.pop('generation_time')
        df.pop('start_time')
        df.pop('end_time')
        df.pop('location_reference')

        df.head()

        # In[3]:

        df_francia = df[df['lcd1'] == 40021]
        df_francia.head(10)
        # every 5 mnts data available

        # In[57]:

        df_francia.describe()

        # In[4]:

        df_francia.pop('Road_LCD')
        df_francia.pop('lat')
        df_francia.pop('lng')
        df_francia.pop('period')
        df_francia.pop('lcd1')
        df_francia.pop('Road_name')
        df_francia.pop('offset')
        df_francia.pop('direction')

        df_francia.head()

        df_francia['hour_min'] = pd.Series(df_francia['hour'] + df_francia['minute'] / 60)
        df_francia.pop('hour')
        df_francia.pop('minute')
        df_francia.head()
        df_francia.tail()

        train_cols = ["flow", "day_of_week", "hour_min"]
        df_train, df_test = train_test_split(df_francia, train_size=0.8, test_size=0.2, shuffle=False)
        print("Train and Test size", len(df_train), len(df_test))
        # scale the feature MinMax, build array
        x = df_train.loc[:, train_cols].values
        x_train = self.min_max_scaler.fit_transform(x)
        x_test = self.min_max_scaler.transform(df_test.loc[:, train_cols])
        x_train.shape
        test_labels = df_test.pop('flow')

    def trimDataset(self, mat, batch_size):
        """
        trims dataset to a size that's divisible by BATCH_SIZE
        """
        no_of_rows_drop = mat.shape[0] % batch_size
        if no_of_rows_drop > 0:
            return mat[:-no_of_rows_drop]
        else:
            return mat

    def dataSet(self, x_test, x_train):
        x_t, y_t = self.buildTimeseries(x_train, 0)
        x_t = self.trimDataset(x_t, BATCH_SIZE)
        y_t = self.trimDataset(y_t, BATCH_SIZE)
        x_temp, y_temp = self.buildTimeseries(x_test, 0)
        x_val, x_test_t = np.split(self.trimDataset(x_temp, BATCH_SIZE), 2)
        y_val, y_test_t = np.split(self.trimDataset(y_temp, BATCH_SIZE), 2)
        return x_val, x_test_t, x_t, y_val, y_test_t, y_t

    def createModel(self, x_val, x_t, y_val, y_t):
        self.model.add(LSTM(100, batch_input_shape=(BATCH_SIZE, TIME_STEPS, x_t.shape[2]), dropout=0.0,
                                recurrent_dropout=0.0, stateful=True, kernel_initializer='random_uniform'))
        self.model.add(Dropout(0.5))
        self.model.add(Dense(20, activation='relu'))
        self.model.add(Dense(1, activation='sigmoid'))
        optimizer = optimizers.RMSprop(lr=0.00010000)
        self.model.compile(loss='mean_squared_error', optimizer=optimizer)
        history = self.model.fit(x_t, y_t, epochs=100, verbose=2, batch_size=BATCH_SIZE, shuffle=False,
                                     validation_data=(
                                     self.trimDataset(x_val, BATCH_SIZE), self.trimDataset(y_val, BATCH_SIZE)))

    def loadModel(self, file):
        self.model=file

    def predict(self, x_test_t, y_test_t):
        y_pred = self.model.predict(self.trimDataset(x_test_t, BATCH_SIZE), batch_size=BATCH_SIZE)
        y_pred = y_pred.flatten()
        y_test_t = self.trimDataset(y_test_t, BATCH_SIZE)
        error = mean_squared_error(y_test_t, y_pred)
        print("Error is", error, y_pred.shape, y_test_t.shape)
        print(y_pred[0:15])
        print(y_test_t[0:15])
        y_pred_org = (y_pred * self.min_max_scaler.data_range_[0]) + self.min_max_scaler.data_min_[0]
        # min_max_scaler.inverse_transform(y_pred)
        y_test_t_org = (y_test_t * self.min_max_scaler.data_range_[0]) + self.min_max_scaler.data_min_[0]
        # min_max_scaler.inverse_transform(y_test_t)
        print(y_pred_org[0:15])
        print(y_test_t_org[0:15])

#plots


sns.set(style="whitegrid")
plot_df = x_test_t.copy()
test_labels = test_labels.astype(int)
print('test labels', test_labels)
plot_df['flow'] = test_labels
plot_df['hour'] = plot_df['hour_min'].apply(lambda hm: int(hm))
plot_df['minute'] = plot_df['hour_min'].apply(lambda hm: int((hm - int(hm)) * 60))
plot_df['time'] = pd.to_datetime(plot_df[['year', 'month', 'day', 'hour', 'minute']])  # .astype(int)
plot_df['predicted_flow'] = y_pred

plot_df = pd.DataFrame({
    'real_flow': plot_df['flow'],
    'predicted_flow': plot_df['predicted_flow'],
    'time': plot_df['time']
}).set_index('time')

# ax = plot_df.plot()
fig, ax = plt.subplots()
plot_df.plot(ax=ax)

plt.ylabel('flow')

# set ticks every week
ax.xaxis.set_major_locator(mdates.AutoDateLocator())
# set major ticks format
ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y - %H:%M'))

get_ipython().run_line_magic('debug', '')

from matplotlib import pyplot as plt

plt.figure()
plt.plot(y_pred_org)
plt.plot(y_test_t_org)
plt.title('Prediction vs Real')

plt.ylabel('flows')
plt.xlabel('time')
plt.legend(['Prediction', 'Real'], loc='upper left')
