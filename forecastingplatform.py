from flask import Flask, make_response, request
from flask_restplus import Resource, Api

from threading import Thread

import uuid
from multiprocessing import Event, Manager, Queue
from multiprocessing import Process
from time import sleep
from tools.Classes import ForecastingJob, Task
import numpy as np
import time
#######
import json

from prometheus_client import REGISTRY, generate_latest
from prometheus_client.metrics_core import GaugeMetricFamily

#API
#Add data
#PUT http://127.0.0.1:8888/Forecasting/adddata/<string:value>/<string:job>
#Start job
#PUT http://127.0.0.1:8888/Forecasting/start/<string:mon_id>/<string:data_type>
#stop job
#DELETE http://127.0.0.1:8888/Forecasting/stop/<string:job_id>
#control status
#GET http://127.0.0.1:8888/Forecasting/control

#######

PORT = 8888
active_jobs = {}
POLLING = 2
data = {}  # Map to save queue for each peer ip
#map_prometheus_request_time = {} #map of last Prometheus requests for each peer ip


# Flask and Flask-RestPlus configuration
app = Flask(__name__)
api = Api(app, version='1.0', title='ForecastingPlatformAPI',
          description='Api to enable the submission of requests to activate forecasting jobs. \n'
                      'Author: Andrea Sgambelluri')
restApi = api.namespace('Forecasting', description='input REST API for forecasting requests')
prometheusApi = api.namespace('', description='REST API used by the Prometheus exporter')


class SummMessages(object):
    def __init__(self):
        self.dict_sum = {}
        self.dict_number = {}

    def add(self, object):
        #job = object.get("job")
        print(object)
        del object['job']
        for key, value in object.items():
            if key in self.dict_sum.keys():
                self.dict_sum[key] += value
                self.dict_number[key] += 1
            else:
                self.dict_sum[key]= value
                self.dict_number[key] = 1

    def get_result(self):
        dict_result = {}
        for parameter, value in self.dict_sum.items():
            number = self.dict_number[parameter]
            result = round(value / number, 1)
            dict_result[parameter] = result
        # self.dict_sum.clear()
        # self.dict_number.clear()
        return dict_result


class CustomCollector(object):
    def __init__(self):
        self.id = ""

    def collect(self):
        global data
        found_key = ""
        for key in data.keys():
            if self.id == key:
                found_key = key
        if found_key == "":
            return None

        element = data[found_key]
        print(element)
        msgs = SummMessages()
        #while not queue.empty():
        msgs.add(element)
        result = msgs.get_result()
        metrics = []
        for parameter, value in result.items():
            gmf = GaugeMetricFamily(parameter, "avg", labels='avg')
            gmf.add_metric([parameter], value)
            metrics.append(gmf)
        for metric in metrics:
            yield metric

    def set_parameters(self, r):
        self.id = r

'''
                gmf = GaugeMetricFamily(parameter, parameter, labels=['host'])
            for value in values:
                gmf.add_metric([value['host']], value['value'])
            metrics.append(gmf)
        for metric in metrics:
            yield metric

'''
cc = CustomCollector()
REGISTRY.register(cc)


@restApi.route('/start/<string:mon_id>/<string:data_type>')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not started')
class _ForecastingStart(Resource):
    @restApi.doc(description="handling new forecasting requests")
    def put(self, mon_id, data_type):
        global active_jobs
        global data
        print(f'mon_id: {mon_id} and data_type: {data_type}')
        #req_id = uuid.uuid1()
        req_id = "b4338be3-9ec3-11eb-9558-dc7196d747fd"
        model = "LSTM"
        fj = ForecastingJob(id, mon_id, data_type, model)
        task = Task(str(req_id), mon_id, 0, fj, data, POLLING)
        t = Thread(target=task.run, args=())
        t.start()
        active_jobs[str(req_id)] = {'thread': t, 'job': fj, 'task': task}
        return str(req_id)


@restApi.route('/adddata/<string:value>/<string:job>')
@restApi.response(200, 'Success')
@restApi.response(404, 'not found')
class _ForecastingAdd(Resource):
    @restApi.doc(description="handling new forecasting requests")
    def put(self, value, job):
        global active_jobs
        #print(active_processes)
        f = active_jobs[str(job)].get('job')
        a1 = np.array([[value]])
        f.addData(a1)
        print(f.data)
        print(str(f.getForecastingValue()))
        f.setForecasting(True)
        return "ok"


@restApi.route('/control')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not started')
class _ForecastingCheck(Resource):
    @restApi.doc(description="handling new forecasting requests")
    def get(self):
        global active_jobs
        global data
        print("processes")
        print(active_jobs)
        print("data")
        print(data)

        return "ok"


@restApi.route('/stop/<string:job_id>')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not found')
class _ForecastingStop(Resource):
    @restApi.doc(description="handling stop forecasting requests")
    def delete(self, job_id):
        global active_jobs
        print(f'job_id: {job_id}')
        if job_id in active_jobs.keys():
            element = active_jobs[job_id]
            task = element.get('task')
            thr = element.get('task')
            task.terminate()
            thr.join()
            active_jobs.pop(job_id)
            print(active_jobs)
            return 'Forecasting job '+job_id+ ' Successfully stopped'

        else:
            return 'Forecasting job not found', 404


@prometheusApi.route('/metrics/<string:job_id>')
@prometheusApi.response(200, 'Success')
@prometheusApi.response(404, 'Not found')
class _PrometheusExporter(Resource):
    @prometheusApi.doc(description="handling Prometheus connections")
    def get(self, job_id):
        global data
        global active_jobs
        f = active_jobs[str(job_id)].get('job')
        value = f.getForecastingValue()
        print(str(value))
        return_data = {
            'job': job_id,
            'mon_id': value
        }
        return_data_str = json.dumps(return_data)
        json_obj2 = json.loads(return_data_str)
        if job_id not in data.keys():
            data[job_id] = {}
        data[job_id] = json_obj2
        print(return_data_str)

        id = job_id
        isExists = False
        for key in data.keys():
            if key == id:
                isExists = True
                break
        if not isExists:
            return 'Forecasting job not found', 404
        cc.set_parameters(id)
        dataF = generate_latest(REGISTRY)
        response = make_response(dataF , 200)
        response.mimetype = "text/plain"
        return response


if __name__ == '__main__':
    #loadConfig()
    #setupConfigProcesses()
    #manager = Manager()
    #active_jobs = manager.dict()
    #active_processes = manager.dict()
    app.run(host='0.0.0.0', port=PORT)
