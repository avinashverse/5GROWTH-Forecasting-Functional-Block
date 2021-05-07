from flask import Flask, make_response, request
from flask_restplus import Resource, Api


import uuid
from multiprocessing import Event, Manager, Queue
from multiprocessing import Process
from time import sleep
from tools.Classes import ForecastingJob
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
manager = None
#active_jobs = None
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
        del object["job"]
        for key, value in object.items():
            if key in self.dict_sum.keys():
                self.dict_sum[key] += value
                self.dict_number[key] += 1
            else:
                self.dict_sum[key].update(value)
                self.dict_number[key].update(1)

    def get_result(self):
        dict_result = {}
        for parameter, value in self.dict_sum.items():
            number = self.dict_number[parameter]
            result = round(value / number, 1)
            dict_result[parameter].update(result)
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

        queue = data[found_key]
        msgs = SummMessages()
        while not queue.empty():
            msgs.add(queue.get())

        result = msgs.get_result()
        metrics = []
        for parameter, value in result.items():
            gmf = GaugeMetricFamily(parameter, "avg")
            gmf.add_metric(value)
            metrics.append(gmf)
        for metric in metrics:
            yield metric

    def set_parameters(self, r):
        self.id = r


cc = CustomCollector()
REGISTRY.register(cc)


@restApi.route('/adddata/<string:value>/<string:job>')
@restApi.response(200, 'Success')
@restApi.response(404, 'not found')
class _ForecastingAdd(Resource):
    @restApi.doc(description="handling new forecasting requests")
    def put(self, value, job):
        global active_jobs
        #print(active_processes)
        f = active_processes[str(job)].get('job')
        a1 = np.array([[value]])
        f.addData(a1)
        print(f.data)
        print(str(f.getForecastingValue()))
        f.setForecasting(True)
        return "ok"


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
        kill_event = Event()
        fj = ForecastingJob(id, mon_id, data_type, model)
        process = newProcess(kill_event, str(req_id), mon_id, data_type, model, 0, fj, data)
        active_processes[str(req_id)] = {'kill_event': kill_event, 'process': process, 'job': fj}
        #print(active_processes)
        return str(req_id)


@restApi.route('/control')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not started')
class _ForecastingCheck(Resource):
    @restApi.doc(description="handling new forecasting requests")
    def get(self):
        global active_jobs
        global data
        print("processes")
        print(active_processes)
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
        if job_id in active_processes.keys():
            element = active_processes[job_id]
            kill_event = element.get('kill_event')
            process = element.get('process')
            if process.is_alive():
                kill_event.set()
                process.join()
            active_processes.pop(job_id)
            print(active_processes)
            return 'Forecasting job '+job_id+ ' Successfully stopped'

        else:
            return 'Forecasting job not found', 404


def newProcess(kill_event, id, mon_id, data_type, model, period, fj, dataset):
    process = Process(target=run, args=(kill_event, id, mon_id, period, fj, dataset))
    process.daemon = True
    process.start()
    return process


def run(kill_event, fid, mon_id, period, fj, queeueData):
    while not kill_event.is_set():
        if period == 1:
            '''
            try:
                msg = queue.get()
                myid = msg.get('id')
                command = msg.get('command')
                reply = spo.run_command(flag, command)
                my_reply_dictionary[myid] = reply
            except Exception as e:
                print spo.name + '\'s configuration process stopped by exception: ' + str(e) + " ------> RESTARTING"
                spo.kill_tcl(flag)
                kill_child_processes()
                continue
            '''
            #print(fj.str())

            value = fj.getForecastingValue()
            return_data = {
                "job": fid,
                mon_id: value
            }
            return_data_str = json.dumps(return_data)
            json_obj2 = json.loads(return_data_str)
            if json_obj2['job'] not in queeueData.keys():
                queeueData[fid] = Queue()
            queeueData[fid].put(json_obj2)
            sleep(POLLING)
        else:
            print("loop")
            if fj.isForecasting():
                print("Forecasting")
                print(fj.data)
                value = fj.getForecastingValue()
                return_data = {
                    "job": fid,
                    mon_id: value
                }
                return_data_str = json.dumps(return_data)
                json_obj2 = json.loads(return_data_str)
                if json_obj2['job'] not in queeueData.keys():
                    queeueData[fid] = Queue()
                queeueData[fid].put(json_obj2)
                print(return_data_str)
                fj.setForecasting(False)

        sleep(POLLING)

    print( f'configuration process stopped')


@prometheusApi.route('/metrics/<string:job_id>')
@prometheusApi.response(200, 'Success')
@prometheusApi.response(404, 'Not found')
class _PrometheusExporter(Resource):
    @prometheusApi.doc(description="handling Prometheus connections")
    def get(self, job_id):
        global data
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
        #response = make_response(data , 200)
        #response.mimetype = "text/plain"
        #return response
        print(dataF)
        return dataF


if __name__ == '__main__':
    #loadConfig()
    #setupConfigProcesses()
    #manager = Manager()
    #active_jobs = manager.dict()
    #active_processes = manager.dict()
    app.run(host='0.0.0.0', port=PORT)

