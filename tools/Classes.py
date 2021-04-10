from multiprocessing.process import current_process
import psutil
import numpy as np


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
    def __init__(self, id, mon_id, data_type, model, steps=None):
        self.model = model
        self.job_id = id
        self.mon_id = mon_id
        self.data_type = data_type
        self.forecast = False
        if steps is None:
            self.time_steps = 10
        else:
            self.time_steps = steps
        self.batch_size = 10
        self.data = np.arange(self.time_steps).reshape(self.time_steps, 1)

    def str(self):
        return '{ Forecasting job:\n\tmodel: ' + str(self.model) + '\n\tjob_id: ' + str(self.job_id) + \
               '\n\tmon_id: ' + str(self.mon_id) + '\n\tdata_type: ' + str(self.data_type) + \
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




