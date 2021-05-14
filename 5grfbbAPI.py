from flask import Flask, make_response, request, jsonify
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

#New API implemented
#Start job
#POST http://127.0.0.1:8888/Forecasting/
#input json
'''
{ 
        "nsId" : "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d",
        "vnfdId" : "spr2",
        "performanceMetric" :  "VcpuUsageMean",
        "nsdId" : nsEVS_aiml,
        "IL" : 1
}
'''

#Update IL
#PUT http://127.0.0.1:8888/Forecasting?job_id=job&IL=x
#Get list of active jobs
#GET http://127.0.0.1:8888/Forecasting
#Get details of job_id job
#GET http://127.0.0.1:8888/Forecasting?job_id=job
#stop job
#DELETE http://127.0.0.1:8888/Forecasting?job_id=job


#Add data
#PUT http://127.0.0.1:8888/Forecasting/adddata/<string:value>/<string:job>
#control status
#GET http://127.0.0.1:8888/Forecasting/control

#######

PORT = 8888
active_jobs = {}
POLLING = 2
data = {}  # Map to save queue for each peer ip
#map_prometheus_request_time = {} #map of last Prometheus requests for each peer ip

reqs = {}

# Flask and Flask-RestPlus configuration
app = Flask(__name__)
api = Api(app, version='1.0', title='5GrForecastingPlatform')
#          description='API to enable the submission of requests to activate forecasting jobs. \n'
#                      'Author: Andrea Sgambelluri')
restApi = api.namespace('', description='input REST API for forecasting requests')
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


'''
nsId.
vnfdId
Performance metric
nsdId
IL
'''


@restApi.route('/Forecasting')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not found')
@restApi.response(410, 'Forecasting job not started')
class _Forecasting(Resource):
    @restApi.doc(description="handling new forecasting requests")
    #put method receives data as payload in json format
    def post(self):
        global active_jobs
        global data
        global reqs

        request_data = request.get_json()
        #input data in the payload in json format
        nsid = request_data['nsId']
        vnfdid = request_data['vnfdId']
        metric = request_data['performanceMetric']
        nsdid = request_data['nsdId']
        il = request_data['IL']
        req_id = "b4338be3-9ec3-11eb-9558-dc7196d747fd"

        #TODO
        #mapping algorithm
        model = "LSTM"
        #TODO
        #download the model form AI/ML platform
        #TODO
        #create kafla topic
        # TODO
        # create scraper job

        #print(f'mon_id: {mon_id} and data_type: {data_type}')
        #req_id = uuid.uuid1()
        fj = ForecastingJob(id, metric, nsdid, model)
        task = Task(str(req_id), metric, 0, fj, data, POLLING)
        t = Thread(target=task.run, args=())
        t.start()
        active_jobs[str(req_id)] = {'thread': t, 'job': fj, 'task': task}
        reqs[str(req_id)] = {'model': model, 'nsId': nsid, 'vnfdId': vnfdid, 'IL': il, 'nsdId': nsdid,
                             'performanceMetric': metric, 'isActive' : True}
        # TODO
        # create Prometheus job pointing to the exporter

        return str(req_id), 200

    def get(self):
        global reqs
        reply = list(reqs.keys())
        print(reply)
        return json.dumps(reply), 200


@restApi.route('/Forecasting/<string:job_id>')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not found')
@restApi.response(410, 'Forecasting job not started')
class _Forecasting1(Resource):
    #@restApi.doc(description="handling new forecasting requests")
    #put method receives data as payload in json format
    def delete(self, job_id):
        global active_jobs
        global reqs
        print(f'job_id: {job_id}')
        if job_id in active_jobs.keys():
            element = active_jobs[job_id]
            task = element.get('task')
            thr = element.get('task')
            task.terminate()
            #thr.join()
            active_jobs.pop(job_id)
            print(active_jobs)
            if job_id in reqs.keys():
                reqs[str(job_id)] = {'isActive': False}
            return 'Forecasting job '+job_id+ ' Successfully stopped', 200
        else:
            return 'Forecasting job not found', 404

    def get(self, job_id):
        global active_jobs
        global reqs

        print(f'job_id: {job_id}')
        if job_id in reqs.keys():
            return reqs[str(job_id)], 200
        else:
            return 'Forecasting job not found', 404


@restApi.route('/Forecasting/<string:job_id>/<string:il>')
@restApi.response(200, 'Success')
@restApi.response(404, 'not found')
class _ForecastingAdd(Resource):
    def put(self, job_id, il):
        if str(job_id) in reqs.keys():
            reqs[str(job_id)] = {'IL': il}
            return 'Instantiation level updated', 200
        else:
            return 'Forecasting job not found', 404

'''


@restApi.route('/Forecasting/adddata/<string:value>/<string:job>')
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


@restApi.route('/Forecasting/control')
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

'''


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

