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

# python imports

from confluent_kafka import KafkaError, KafkaException
import numpy as np
import json
from confluent_kafka.error import ConsumeError

from io import StringIO

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from algorithms.lstmCpu import lstmcpu

import logging

log = logging.getLogger("Forecaster")


class ForecastingJob:
    """
    Make it a thread with a consumer with consumerid=id.
    Each message mast be added with the add data function.
    https://github.com/confluentinc/confluent-kafka-python
    """
    def __init__(self, id, data_type, model, metric, il, steps=None):
        self.model = model
        self.job_id = id
        self.nstype = data_type
        self.forecast = False
        self.names = {}
        self.metric = metric
        if steps is None:
            self.time_steps = 10
        else:
            self.time_steps = steps
        self.batch_size = 10
        if self.model == "Test":
            self.data = np.arange(self.time_steps).reshape(self.time_steps, 1)
        else:
            self.data = None
        self.trained_model = None
        self.lstm_data = None
        self.il = il

    def data_parser(self, json_data):
        loaded_json = json.loads(json_data)
        log.debug("Forecasting Job: received data: \n{}".format(loaded_json))
        names = {}
        if self.model == "Test":
            if "cpu" or "CPU" or "Cpu" in self.metric:
                for element in loaded_json:
                    mtype = element['type_message']
                    if mtype == "metric":
                        instance = element['metric']['instance']
                        cpu = element['metric']['cpu']
                        mode = element['metric']['mode']
                        nsid = element['metric']['nsId']
                        vnfdif = element['metric']['vnfdId']
                        t = element['value'][0]
                        val = element['value'][1]
                        if instance not in names.keys():
                            names[instance] = {}
                            names[instance]['cpus']= []
                            names[instance]['modes']= []
                        names[instance]['cpus'].append(cpu)
                        names[instance]['modes'].append(mode)
                        a1 = np.array([[round(float(val), 2)]])
                        self.add_data(a1)
                self.names = names
        if self.model == "lstm":
            if "cpu" or "CPU" or "Cpu" in self.metric:
                for element in loaded_json:
                    #print(element)
                    mtype = element['type_message']
                    if mtype == "metric":
                        instance = element['metric']['instance']
                        cpu = element['metric']['cpu']
                        mode = element['metric']['mode']
                        nsid = element['metric']['nsId']
                        vnfdif = element['metric']['vnfdId']
                        t = element['value'][0]
                        val = element['value'][1]
                        if instance not in names.keys():
                            names[instance] = {}
                            names[instance]['cpus'] = []
                            names[instance]['modes'] = []
                            names[instance]['values'] = []
                            names[instance]['timestamp'] = []
                        names[instance]['cpus'].append(cpu)
                        names[instance]['modes'].append(mode)
                        names[instance]['values'].append(round(float(val),2))
                        names[instance]['timestamp'].append(t)
                self.names = names
                avg_cpu = 0
                t = None
                for key in names.keys():
                    avg_cpu = sum(names[key]['values'])/len(names[key]['values'])
                    if t is None:
                        t = names[key]['timestamp'][0]
                string = str(t) + ";" + str(self.il) + ";" +str(avg_cpu)+ ";48;1"
                #print("csv data: {}".format(string))
                self.lstm_data = StringIO("col1;col2;col3;col4;col5\n"+string+"\n")

                #1605184144.25,1,81.48,96.06,1

        else:
            print("Forecasting Job: model not supported")

    def get_names(self):
        return self.names

    def run(self, event, consumer):
        log.debug("Forecasting Job: Starting the Kafka Consumer")
        while not event.is_set():
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        log.error('Forecatsing Job: %% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    #no error -> message received
                    #print("Received new data")
                    #print(msg.value())
                    self.data_parser(msg.value())

            except ConsumeError as e:
                log.error("Forecasting Job: Consumer error: {}".format(str(e)))
                # Should be commits manually handled?
                consumer.close()

    def str(self):
        return '{ Forecasting Job:\n\tmodel: ' + str(self.model) + '\n\tjob_id: ' + str(self.job_id) + \
        '\n\tns_type: ' + str(self.nstype) + \
        '\n\ttime_steps: ' + str(self.time_steps) + '\n\tbatch_size: ' + str(self.batch_size) + '\n}'

    def set_data(self, data):
        self.data = data

    def add_data(self, data):
        if len(data) == self.time_steps:
            self.data = data
        elif len(data) > self.time_steps:
            self.data = data[-self.time_steps:]
        else:
            init = self.time_steps - len(data)
            temp = self.data[-init:]
            self.data = np.concatenate((temp, data), axis=0)

    def set_model(self, back, forward, load, filename):
        if self.model == "lstm":
            if load:
                self.load_lstm_model(back, forward, filename)
            else:
                self.train_model(0.8, back, forward, None, filename)

    def get_forecasting_value(self, n_features, desired):
        if self.model == "Test":
            return round(float(np.sum(self.data.astype(np.float)) / len(self.data)), 2)
        elif self.model == "lstm":
            df = pd.read_csv(self.lstm_data, sep=";")
            ds = df.values
            scaler = MinMaxScaler(feature_range=(0, 1))
            dsx = scaler.fit_transform(ds)
            testX = dsx
            test = np.reshape(testX, (1, 1, n_features))
            value = self.trained_model.predict(desired, test, scaler, n_features)
            return round(float(value[0]), 2)

        else:
            return 0

    def is_forecasting(self):
        return self.forecast

    def get_model(self):
        return self.model

    def set_trained_model(self, model):
        self.trained_model = model

    def load_lstm_model(self, back, forward, filename):
        log.debug("Forecasting Job: loading the LSTM forecasting model")
        lstm = lstmcpu(None, None, back, forward, None)
        lstm.load_trained_model(filename)
        self.set_trained_model(lstm)
        return self.trained_model

    def train_model(self, ratio, back, forward, data_file, model_file):
        if data_file is None:
            data_file = "../data/example-fin.csv"
        lstm = lstmcpu(data_file, ratio, back, forward, 0.90)
        lstm.get_dataset(True, 0, 1)
        lstm.split_sequences_train()
        lstm.reshape()
        lstm.train_lstm(True, model_file)
        self.set_trained_model(lstm)
        return self.trained_model





