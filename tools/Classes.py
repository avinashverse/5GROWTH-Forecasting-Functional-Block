from multiprocessing.process import current_process
from threading import Event
from confluent_kafka import Consumer, KafkaError


import psutil
import numpy as np
import json
from time import sleep

from confluent_kafka.error import ConsumeError


def kill_child_processes():
    """
    This method should be invoked only within a function,
    handled by a python process, in which you used
    a tcl console object. Unluckily, wexpect library, which
    is used to spawn tcl consoles, does not handle correctly
    the creation of those object when called from another process.
    This leads to have useless threads in background once killed.
    For this reason, at the end of the function in your process,
    this method should be invoked in order to kill those threads.
    """
    p = psutil.Process(current_process().pid)
    children = p.children(recursive=True)
    for child in children:
        child.kill()


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
        if steps is None:
            self.time_steps = 10
        else:
            self.time_steps = steps
        self.batch_size = 10
        self.data = np.arange(self.time_steps).reshape(self.time_steps, 1)
        #self.value = ""

    def run(self, event, consumer):
        print("starting the consumer")
        while not event.is_set():
            print("test")
            try:
                print(consumer.list_topics())
                '''
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                else:
                    value = msg.value().decode('utf-8')
                    print(value)
                    # Insert here code to write with addData
                '''
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                elif not msg.error():
                    print('Received message: {0}'.format(msg.value()))
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached {0}/{1}'
                          .format(msg.topic(), msg.partition()))
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

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


