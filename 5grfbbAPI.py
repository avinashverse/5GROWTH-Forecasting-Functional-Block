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
from flask import Flask, make_response, request
from flask_restplus import Resource, Api, fields
from threading import Thread, Event
import multiprocessing
import uuid
import json
from prometheus_client import REGISTRY, generate_latest
from prometheus_client.metrics_core import GaugeMetricFamily
import configparser
import logging
import time
#######

from tools.Classes import ForecastingJob
from tools.externalConnections import ExternalConnections

# New API implemented
# Start job
# POST http://127.0.0.1:8888/Forecasting/
# input json
'''
{ 
    "nsId" : "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d",
    "vnfdId" : "spr2",
    "performanceMetric" :  "VcpuUsageMean",
    "nsdId" : nsEVS_aiml,
    "IL" : 1
}
'''
# Update IL
# PUT http://127.0.0.1:8888/Forecasting?job_id=job&IL=x
# Get list of active jobs
# GET http://127.0.0.1:8888/Forecasting
# Get details of job_id job
# GET http://127.0.0.1:8888/Forecasting?job_id=job
# stop job
# DELETE http://127.0.0.1:8888/Forecasting?job_id=job


# Add data
# PUT http://127.0.0.1:8888/Forecasting/adddata/<string:value>/<string:job>
# control status
# GET http://127.0.0.1:8888/Forecasting/control

#######

PORT = 8888  # default listening port
active_jobs = {}  # dict to store the active jobs
data = {}  # Map to save queue for each peer ip
reqs = {}  # dict to store all the instantiated jobs
aimlip = "192.168.1.1"
aimlport = 12345
aimlurl = '/aiml'

# Flask and Flask-RestPlus configuration
app = Flask(__name__)
api = Api(app, version='1.0', title='5GrForecastingPlatform')
restApi = api.namespace('', description='input REST API for forecasting requests')
prometheusApi = api.namespace('', description='REST API used by the Prometheus exporter')

# module to load FFB configuration
config = configparser.ConfigParser()
# logging configuration
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO, filename='5grfbb.log')

ec = None

# model definition for the API
model = restApi.model("Model",
                      {
                          "nsId": fields.String,
                          "vnfdId": fields.String,
                          "performanceMetric": fields.String,
                          "nsdId": fields.String,
                          "IL": fields.Integer,
                      }
                      )


# auxiliary class used to preprocess data for the reply
class SummMessages(object):
    def __init__(self):
        self.dict_sum = {}
        self.dict_number = {}

    def add(self, element):
        metric = element.get("metric")
        del element['metric']
        if "cpu" or "CPU" or "Cpu" in metric:
            del element['job']
            name = element.get("name")
            cpu = element.get("cpu")
            mode = element.get("mode")
            host = name + '::' + cpu + '::' + mode
            del element['name']
            del element['cpu']
            del element['mode']

            for key, value in element.items():
                if key in self.dict_sum.keys():
                    if host in self.dict_sum[key].keys():
                        self.dict_sum[key][host] += value
                        self.dict_number[key][host] += 1
                    else:
                        self.dict_sum[key].update({host: value})
                        self.dict_number[key].update({host: 1})
                else:
                    self.dict_sum.update({key: {host: value}})
                    self.dict_number.update({key: {host: 1}})

    def get_result(self):
        dict_result = {}
        for parameter, value in self.dict_sum.items():
            for host, value2 in value.items():
                number = self.dict_number[parameter][host]
                result = round(value2 / number, 1)
                if parameter in dict_result.keys():
                    dict_result[parameter].append({
                        "host": host,
                        "value": result})
                else:
                    dict_result.update({parameter: [{
                        "host": host,
                        "value": result}]})
        return dict_result


# custom Prometheus exporter collector
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
        for parameter, values in result.items():
            if "cpu" or "CPU" or "Cpu" in parameter:
                for value in values:
                    [instance, cpu, mode] = str(value['host']).split('::', 2)
                    label = [cpu, mode, instance]
                    gmf = GaugeMetricFamily(parameter, "avg_" + parameter, labels=['cpu', 'mode', 'instance'])
                    gmf.add_metric(label, value['value'])
                    metrics.append(gmf)
        logging.debug('Prometheus Exporter: New metrics computed ' + str(metrics))
        for metric in metrics:
            yield metric

    def set_parameters(self, r):
        logging.debug('Prometheus Exporter: new job selected' + str(r))
        self.id = r


cc = CustomCollector()
REGISTRY.register(cc)


@restApi.route('/Forecasting')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not found')
@restApi.response(410, 'Forecasting job not started')
class _Forecasting(Resource):
    @restApi.doc(description="handling new forecasting requests")
    @restApi.expect(model, envelope='resource')
    # put method receives data as payload in json format
    def post(self):
        global active_jobs
        global data
        global reqs
        global ec

        request_data = request.get_json()
        logging.info('Forecasting API: new job requested' + str(request_data))
        # input data in the payload in json format
        nsid = request_data['nsId']
        vnfdid = request_data['vnfdId']
        metric = request_data['performanceMetric']
        nsdid = request_data['nsdId']
        il = request_data['IL']
        # dynamic request_id creation
        req_id = uuid.uuid1()
        # static id (only for development purpose)
        #req_id = "1aa0c8e6-c26e-11eb-a8ba-782b46c1eefd"
        reqs[str(req_id)] = {'nsId': nsid, 'vnfdId': vnfdid, 'IL': il, 'count': 2, 'nsdId': nsdid,
                             'performanceMetric': metric, 'isActive': True, 'scraperJob': None,
                             'kafkaTopic': None, 'prometheusJob': None, 'model': None}
        logging.debug('Forecasting API: DB updated with new job ' + str(req_id))

        # create kafka topic and update reqs dict
        topic = ec.createKafkaTopic(nsid)
        if topic != 0:

            logging.info('Forecasting API: topic ' + topic + ' created')
        else:
            topic = nsid + "_forecasting"
        reqs[str(req_id)]['kafkaTopic'] = topic
        # TODO: check scraper job API call is working
        # create scraper job and update the reqs dict
        expression = metric + '{mode=\"idle\",nsId=\"' + nsid + '\",vnfdId=\"' + vnfdid + '\", forecasted=\"no\"'
        # rep = ec.startScraperJob(nsid = nsid, topic = topic, vnfdid = vnfdid, metric = metric,
        #                      expression = expression, period = 15)
        # logging.info('Forecasting API: scraper job '+rep+' created')
        # TODO
        # mapping algorithm
        # model_forecasting = "Test"
        model_forecasting = "lstm"

        reqs[str(req_id)]['model'] = model_forecasting
        logging.debug('Forecasting API: model selected ' + model_forecasting)
        # TODO: connect to AIML
        # download the model form AI/ML platform
        #logging.info('Forecasting API: model ' + model_forecasting + ' downloaded from AIMLP')

        fj = ForecastingJob(req_id, nsdid, model_forecasting, metric, il)
        logging.debug('Forecasting API: forecasting job created ' + fj.str())
        fj.set_model(1, 1, True, 'trainedModels/lstm11.h5')
        event = Event()
        t = Thread(target=fj.run, args=(event, ec.createKafkaConsumer(req_id, topic)))
        t.start()
        active_jobs[str(req_id)] = {'thread': t, 'job': fj, 'kill_event': event}#, 'trained_model': trainedModel}
        # TODO: check prometheus job API call is working
        # create Prometheus job pointing to the exporter
        #ec.startPrometheusJob(vnfdid, nsid, 15, req_id)
        return str(req_id), 200

    @staticmethod
    def get():
        global reqs
        reply = list(reqs.keys())
        print(reply)
        return json.dumps(reply), 200


@restApi.route('/Forecasting/<string:job_id>')
@restApi.response(200, 'Success')
@restApi.response(404, 'Forecasting job not found')
@restApi.response(410, 'Forecasting job not started')
class _Forecasting1(Resource):
    # @restApi.doc(description="handling new forecasting requests")
    # put method receives data as payload in json format
    @staticmethod
    def delete(job_id):
        global active_jobs
        global reqs

        logging.info('Forecasting API: request to stop forecasting job ' + job_id + ' received')
        if job_id in active_jobs.keys():
            element = active_jobs[job_id]
            thread = element.get('thread')
            event = element.get('kill_event')
            event.set()
            thread.join()
            active_jobs.pop(job_id)
            print(active_jobs)
            # TODO
            # delete Prometheus job pointing to the exporter
            logging.info('Forecasting API: deleted Prometheus job')
            # TODO
            # delete scraper job and update the reqs model
            logging.info('Forecasting API: deleted scraper job')

            # delete kafla topic and update reqs dict
            topic = reqs[str(job_id)].get('kafkaTopic')
            if topic is not None:
                ec.deleteKafkaTopic(topic)
            else:
                ec.deleteKafkaTopic(reqs[str(job_id)].get('nsId') + "_forecasting")
            logging.info('Forecasting API: deleted kafka topic')
            if job_id in reqs.keys():
                reqs[str(job_id)] = {'isActive': False}
            return 'Forecasting job ' + job_id + ' Successfully stopped', 200
        else:
            return 'Forecasting job not found', 404

    @staticmethod
    def get(job_id):
        global active_jobs
        global reqs

        if job_id in reqs.keys():
            logging.info('Forecasting API: GET job info, ' + str(reqs[str(job_id)]))
            return reqs[str(job_id)], 200
        else:
            logging.info('Forecasting API: GET job info, job not found ' + job_id)
            return 'Forecasting job not found', 404


@restApi.route('/Forecasting/<string:job_id>/<string:il>')
@restApi.response(200, 'Success')
@restApi.response(404, 'not found')
class _ForecastingAdd(Resource):
    @staticmethod
    def put(job_id, il):
        if str(job_id) in reqs.keys():
            reqs[str(job_id)]['IL'] = il
            logging.info('Forecasting API: IL for job ' + job_id + ' updated to value ' + str(il))
            return 'Instantiation level updated', 200
        else:
            logging.info('Forecasting API: PUT IL, job not found ' + job_id)
            return 'Forecasting job not found', 404


@prometheusApi.route('/metrics/<string:job_id>/<string:vnfd_id>')
@prometheusApi.response(200, 'Success')
@prometheusApi.response(404, 'Not found')
class _PrometheusExporter(Resource):
    @prometheusApi.doc(description="handling Prometheus connections")
    def get(self, job_id, vnfd_id):
        global data
        global active_jobs
        global reqs
        logging.info('Prometeheus Exporter: new metric request for job=' + job_id + ' and vnfdid=' + vnfd_id)

        jobid = job_id
        is_exists = False
        for key in active_jobs.keys():
            if key == jobid:
                is_exists = True
                if reqs[str(job_id)].get('vnfdId') == vnfd_id:
                    break
        if not is_exists:
            logging.info('Prometeheus Exporter: job not found ' + job_id)
            return 'Forecasting job not found', 404
        f = active_jobs[str(job_id)].get('job')
        # get forecasting value
        value = 0
        if f.get_model() == "Test":
            value = f.get_forecasting_value(None)
        elif f.get_model() == "lstm":
            value = f.get_forecasting_value(4)
            #print(str(value))

        metric = reqs[str(job_id)].get('performanceMetric')

        # creating replicas for the average data
        if "cpu" or "CPU" or "Cpu" in metric:
            names = f.get_names()
            #print(names)
            for instance in names.keys():
                for c in range(0, len(names[instance]['cpus'])):
                    cpu = str(names[instance]['cpus'][c])
                    mode = str(names[instance]['modes'][c])
                    return_data = {
                        'job': job_id,
                        'metric': metric,
                        'name': instance,
                        'cpu': cpu,
                        'mode': mode,
                        str(metric): value
                    }
                    return_data_str = json.dumps(return_data)
                    json_obj2 = json.loads(return_data_str)
                    if json_obj2['job'] not in data.keys():
                        data[jobid] = multiprocessing.Queue()
                    #print(return_data_str)
                    data[jobid].put(json_obj2)
                    # print("push")
                    # print(data[id].qsize())
        time.sleep(0.1)
        cc.set_parameters(jobid)
        reply = generate_latest(REGISTRY)
        response = make_response(reply, 200)
        response.mimetype = "text/plain"
        logging.info('Prometheus Exporter: response= ' + str(response))
        return response


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.conf')
    logging.debug('Configuration file parsed and read')

    ec = ExternalConnections('config.conf')
    logging.debug('External connection module initialized')
    if 'local' in config:
        ip = config['local']['localIP']
        port = config['local']['localPort']
    else:
        port = PORT
    if 'AIML' in config:
        aimlip = config['AIML']['aimlIP']
        aimlport = config['AIML']['aimlPort']
        aimlurl = config['AIML']['aimlUrl']
    else:
        port = PORT

    app.run(host='0.0.0.0', port=port)
    logging.info('API server started on port ' + str(port))
