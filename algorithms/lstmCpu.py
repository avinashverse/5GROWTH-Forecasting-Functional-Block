# Copyright 2021 Scuola Superiore Sant'Anna www.santannapisa.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# python and projects imports
import pandas
import numpy as np
from numpy import array
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.models import load_model
from tensorflow.python.keras.layers import LSTM
from tensorflow.python.keras.layers import Dense
from tensorflow.python.keras.layers import RepeatVector
from tensorflow.python.keras.layers import TimeDistributed
from tensorflow.python.keras.callbacks import Callback
from sklearn.preprocessing import MinMaxScaler

import logging

log = logging.getLogger("Forecaster")

class lstmcpu:
    def __init__(self, file, ratio, back, forward, accuracy):
        log.debug("initaite the lstm module")
        if file is None:
            self.train_file = "../data/example-fin.csv"
        else:
            self.train_file = file
        self.dataset = None
        if ratio is None:
            self.trainset_ratio = 0.8
        else:
            self.trainset_ratio = ratio
        self.test = None
        self.train = None
        self.look_backward = back
        self.look_forward = forward
        self.trainX = None
        self.trainY = None
        self.testX = None
        self.testY = None
        if accuracy is None:
            self.desired_accuracy = 0.90
        else:
            self.desired_accuracy = accuracy
        self.model = None
        self.history = None
        self.prediction = None
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.n_features = 0

    def get_history(self):
        return self.history

    def get_dataset(self, scale, scalemin, scalemax):
        dataframe = pandas.read_csv(self.train_file, engine='python', skiprows=1)
        dataset = dataframe.values
        # normlization of the given dataset with range between 0 and 1
        if scale:
            self.scaler = MinMaxScaler(feature_range=(scalemin, scalemax))
            self.dataset = self.scaler.fit_transform(dataset)
        else:
            self.dataset = dataset
        if self.trainset_ratio < 1:
            self.train, self.test = self.split_dataset()

    def split_dataset(self):
        # split datset into train and test sets
        train_size = int(len(self.dataset) * self.trainset_ratio)
        test_size = len(self.dataset) - train_size
        train, test = self.dataset[0:train_size, :], self.dataset[train_size:len(self.dataset), :]
        return train, test

    def split_sequences_train(self):
        X, y = list(), list()
        for i in range(len(self.train)):
            # find the end of this pattern
            end_index_x = i + self.look_backward
            out_end_index_x = end_index_x + self.look_forward
            # check if we are beyond the dataset
            if out_end_index_x > len(self.train):
                break
            # gather input and output parts of the pattern
            seqX, seqY = self.train[i:end_index_x, :], self.train[end_index_x:out_end_index_x, :]
            X.append(seqX)
            y.append(seqY)
        self.trainX = array(X)
        self.trainY = array(y)

    def split_sequences_test(self):
        X, y = list(), list()
        for i in range(0, len(self.test), self.look_forward):
            # find the end of this pattern
            end_index_x = i + self.look_backward
            out_end_index_x = end_index_x + self.look_forward
            # check if we are beyond the dataset
            if out_end_index_x > len(self.test):
                break
            # gather input and output parts of the pattern
            seqX, seqY = self.test[i:end_index_x, :], self.test[end_index_x:out_end_index_x, :]
            X.append(seqX)
            y.append(seqY)
        self.testX = array(X)
        self.testY = array(y)

    def reshape(self):
        self.n_features = self.trainX.shape[2]
        # reshpare trainX and testX dataset
        self.trainX = np.reshape(self.trainX, (self.trainX.shape[0], self.trainX.shape[1], self.n_features))
        self.testX = np.reshape(self.testX, (self.testX.shape[0], self.testX.shape[1], self.n_features))

    # train the model
    def train_lstm(self, save, filename):
        # Callbacks:\n",

        class mycallback(Callback):
            def on_epoch_end(self, epoch, logs={}):
                if (logs.get('accuracy') is not None and logs.get('accuracy') >= self.desired_accuracy):
                    print("\n Reached 90% accuracy so cancelling training")
                    self.model.stop_training = True

        callbacks = mycallback()

        self.model = Sequential()
        self.model.add(LSTM(200, activation='relu', input_shape=(self.look_backward, self.n_features)))
        self.model.add(RepeatVector(self.look_forward))
        self.model.add(LSTM(200, activation='relu', return_sequences=True))
        self.model.add(TimeDistributed(Dense(self.n_features)))
        self.model.compile(optimizer='adam', loss='mse', metrics=['accuracy'])

        # fit model\n",
        self.history = self.model.fit(self.trainX, self.trainY, epochs=1200, callbacks=[callbacks])
        if save:
            self.model.save(filename)
        # return epoch number and pecentage of accuracy
        #return history.epoch, history.history['accuracy'][-1]
        return self.model

    def load_trained_model(self, filename):
        log.info("LSTM: Loading the lstm model from file {}".format(filename))
        self.model = load_model(filename)
        return self.model

    def predict(self, column, data, scaler, features):
        log.debug("LSTM: Predicting the value")
        if data is None:
            prediction = self.model.predict(self.testX, verbose=0)
        else:
            prediction = self.model.predict(data, verbose=0)
        # print(testPrediction)
        if scaler is None:
            scaler = self.scaler
        return self.predicit_column_sssa(prediction, column, scaler, features)

    def predicit_column_sssa(self, testPrediction, prediction_column, scaler, n_features):
        # flat the forecast data
        if n_features is None:
            n_features = self.n_features
        testPrediction_flat = testPrediction[..., prediction_column]
        testPrediction_flat = testPrediction_flat.flatten()
        testPrediction_extended = np.zeros((len(testPrediction) * self.look_forward, n_features))
        for i in range(0, len(testPrediction_flat)):
            testPrediction_extended[i] = testPrediction_flat[i]
            # perfrom inverse transfrom in order to get the orignal data
        testPrediction_fin = scaler.inverse_transform(testPrediction_extended)[:, prediction_column]
        return testPrediction_fin

