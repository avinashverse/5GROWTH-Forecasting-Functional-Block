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


class ForecastingJob:
    """
    Make it a thread with a consumer with consumerid=id.
    Each message mast be added with the add data function.
    https://github.com/confluentinc/confluent-kafka-python
    """
    def __init__(self, id, data_type, model, steps=None):
        self.model = model
        self.job_id = id
        self.data_type = data_type
        self.forecast = False
        self.names = {}
        if steps is None:
            self.time_steps = 10
        else:
            self.time_steps = steps
        self.batch_size = 10
        self.data = np.arange(self.time_steps).reshape(self.time_steps, 1)


    def dataParser(self, json_data):
        loaded_json = json.loads(json_data)
        names = {}
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
                self.addData(a1)
        self.names = names

    def getNames(self):
        return self.names

    def run(self, event, consumer):
        print("starting the consumer")
        while not event.is_set():
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    #no error -> message received
                    print("new data received")
                    self.dataParser(msg.value())

            except ConsumeError as e:
                print("Consumer error: {}".format(str(e)))
                # Should be commits manually handled?
        consumer.close()

    def str(self):
        return '{ Forecasting job:\n\tmodel: ' + str(self.model) + '\n\tjob_id: ' + str(self.job_id) + \
               '\n\tdata_type: ' + str(self.data_type) + \
               '\n\ttime_steps: ' + str(self.time_steps) + '\n\tbatch_size: ' + str(self.batch_size) + '\n}'

    def setData(self, data):
        self.data = data

    def addData(self, data):
        if len(data) == self.time_steps:
            self.data = data
        elif len(data) > self.time_steps:
            self.data = data[-self.time_steps:]
        else:
            init = self.time_steps - len(data)
            temp = self.data[-init:]
            self.data = np.concatenate((temp, data), axis=0)

    def getForecastingValue(self):
        return np.sum(self.data.astype(np.float))/len(self.data)

    def isForecasting(self):
        return self.forecast

    def setForecasting(self, val):
        self.forecasting = val


