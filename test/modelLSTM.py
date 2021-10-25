# multivariate multi-step encoder-decoder LSTM example

# Import required packages for this LSTM example
import pandas
import math
import numpy as np
from numpy import array
from numpy import hstack
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.models import load_model
from tensorflow.python.keras.layers import LSTM
from tensorflow.python.keras.layers import Dense
from tensorflow.python.keras.layers import RepeatVector
from tensorflow.python.keras.layers import TimeDistributed
from tensorflow.python.keras.callbacks import Callback
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error


def get_dataset():
    # define input sequence\n",
    dataframe = pandas.read_csv('../data/parsedData.csv', engine='python',skiprows=1)
    dataset = dataframe.values
    return dataset


dataset = get_dataset()

#normlization of the given dataset with range between 0 and 1
scaler = MinMaxScaler(feature_range=(0, 1))
dataset = scaler.fit_transform(dataset)

#this varialbe is used to select the train and test ratio. Please change if required!
trainset_ratio = 0.8


def split_dataset(trainset_ratio):
    # split datset into train and test sets
    train_size = int(len(dataset)*trainset_ratio)
    test_size = len(dataset) - train_size
    train, test = dataset[0:train_size, :], dataset[train_size:len(dataset), :]
    return train, test


train, test = split_dataset(trainset_ratio)

# choose a number of time steps

look_backward, look_forward = 1, 1


# covert into input/output
def split_sequences_train(sequences, look_backward, look_forward):
    X, y = list(), list()
    for i in range(len(sequences)):
        # find the end of this pattern
        end_index_x = i + look_backward
        out_end_index_x = end_index_x + look_forward
        # check if we are beyond the dataset
        if out_end_index_x > len(sequences):
            break
        # gather input and output parts of the pattern
        seqX, seqY = sequences[i:end_index_x, :], sequences[end_index_x:out_end_index_x, :]
        X.append(seqX)
        y.append(seqY)
    return array(X), array(y)


def split_sequences_test(sequences, look_backward, look_forward):
    X, y = list(), list()
    for i in range(0, len(sequences), look_forward):
        # find the end of this pattern
        end_index_x = i + look_backward
        out_end_index_x = end_index_x + look_forward
        # check if we are beyond the dataset
        if out_end_index_x > len(sequences):
            break
        # gather input and output parts of the pattern
        seqX, seqY = sequences[i:end_index_x, :], sequences[end_index_x:out_end_index_x, :]
        X.append(seqX)
        y.append(seqY)
    return array(X), array(y)


trainX, trainY = split_sequences_train(train, look_backward, look_forward)
testX, testY = split_sequences_test(test, look_backward, look_forward)


'''
the dataset knows the number of features
for example, the current dataset containers four columns i.e., n_features is set to 4
'''
n_features = trainX.shape[2]

print(str(trainX.shape[2]))


#reshpare trainX and testX dataset
trainX = np.reshape(trainX, (trainX.shape[0], trainX.shape[1], n_features))

testX = np.reshape(testX, (testX.shape[0], testX.shape[1], n_features))


#train the model
def train_lstm_sssa():
    # Callbacks:\n",
    desired_accuracy = 0.90

    class mycallback(Callback):
        def on_epoch_end(self, epoch, logs={}):
            if(logs.get('accuracy') is not None and logs.get('accuracy') >= desired_accuracy):
                print("\n Reached 90% accuracy so cancelling training")
                self.model.stop_training = True
    callbacks = mycallback()

    model = Sequential()
    model.add(LSTM(200, activation='relu', input_shape=(look_backward, n_features)))
    model.add(RepeatVector(look_forward))
    model.add(LSTM(200, activation='relu', return_sequences=True))
    model.add(TimeDistributed(Dense(n_features)))
    model.compile(optimizer='adam', loss='mse', metrics=['accuracy'])

    # fit model\n",
    history = model.fit(trainX, trainY, epochs=1200, callbacks=[callbacks])
    model.save('lstm11bis.h5')
    # retun epoch number and pecentage of accuracy
    return history.epoch, history.history['accuracy'][-1]


epoch_num, his_percentage = train_lstm_sssa()


def prediction_lstm_sssa(saved_model):
    # demonstrate prediction
    testPrediction = saved_model.predict(testX, verbose=0)
    #print(testPrediction)
    return testPrediction


saved_model    = load_model("lstm11bis.h5")
testPrediction = prediction_lstm_sssa(saved_model)


def predicit_column_sssa(testPrediction, prediction_column, scaler):
    #flat the forecast data
    testPrediction_flat= testPrediction[..., prediction_column]
    testPrediction_flat= testPrediction_flat.flatten()
    testPrediction_extended = np.zeros((len(testPrediction)*look_forward,n_features))
    for i in range(0, len(testPrediction_flat)):
        testPrediction_extended[i] = testPrediction_flat[i]
        #perfrom inverse transfrom in order to get the orignal data
    testPrediction_fin = scaler.inverse_transform(testPrediction_extended)[:,prediction_column]
    return testPrediction_fin


# thrid column in the given dataset
prediction_column = 2
testPrediction_fin = predicit_column_sssa(testPrediction, prediction_column, scaler)


def testy_column_sssa(testY, prediction_column, scaler):
    testY_flat = testY[...,prediction_column]
    testY_flat = testY_flat.flatten()
    testY_extended = np.zeros((len(testPrediction)*look_forward,n_features))
    for i in range(0, len(testY_flat)):
        testY_extended [i] = testY_flat[i]
    #perfrom inverse transfrom in order to get the orignal data
    testY_fin = scaler.inverse_transform(testY_extended)[:,prediction_column]
    return testY_fin


testY_fin = testy_column_sssa (testY, prediction_column, scaler)


def get_rmse_sssa(testY_fin, testPrediction_fin):
    rmse = math.sqrt(mean_squared_error(testY_fin, testPrediction_fin))
    return rmse


#get rmse between real test dataset and predicited fataset
rmse = get_rmse_sssa (testY_fin, testPrediction_fin)
print('testScore: %.2f RMSE' % (rmse))








