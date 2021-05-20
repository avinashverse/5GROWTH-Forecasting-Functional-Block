from flask import Flask, make_response, request, jsonify
from flask_restplus import Resource, Api
from threading import Thread
import multiprocessing
import uuid
from tools.Classes import ForecastingJob, Task
from tools.externalConnections import ExternalConnections
#######
import json

from prometheus_client import REGISTRY, generate_latest
from prometheus_client.metrics_core import GaugeMetricFamily
import configparser
import logging

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

config = configparser.ConfigParser()
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO, filename='5grfbb.log')
ec = None


class SummMessages(object):
    def __init__(self):
        self.dict_sum = {}
        self.dict_number = {}

    def add(self, object):
        #job = object.get("job")
        #print("insideAdd")
        #print(object)
        del object['job']
        name = object.get("name")
        cpu = object.get("cpu")
        host = name + '::' + cpu
        del object['name']
        del object['cpu']

        for key, value in object.items():
            #self.dict_sum.update({key: {host: value}})
            if key in self.dict_sum.keys():
                if host in self.dict_sum[key].keys():
                    #print("host " + host + " present")
                    self.dict_sum[key][host] += value
                    self.dict_number[key][host] += 1
                else:
                    #print("host " + host + " not present")
                    self.dict_sum[key].update({host: value})
                    self.dict_number[key].update({host: 1})
            else:
                #print("new key " + key + " in dict for " + host )
                self.dict_sum.update({key: {host: value}})
                self.dict_number.update({key: {host: 1}})

    def get_result(self):
        dict_result = {}
        #print(len(self.dict_sum))
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
        # self.dict_sum.clear()
        # self.dict_number.clear()
        return dict_result


class CustomCollector(object):
    def __init__(self):
        self.id = ""

    def collect(self):
        global data
        global logging
        #print("data")
        #print(len(data))
        found_key = ""
        for key in data.keys():
            if self.id == key:
                found_key = key
        if found_key == "":
            return None

        queue = data[found_key]
        #print("queue")
        #print(queue.qsize())
        msgs = SummMessages()
        while not queue.empty():
            msgs.add(queue.get())
        result = msgs.get_result()
        metrics = []
        for parameter, values in result.items():
            for value in values:
                [instance, cpu] = str(value['host']).split('::', 1)
                mode = 'idle'
                label = [cpu, mode, instance]
                gmf = GaugeMetricFamily(parameter, "avg_" + parameter, labels=['cpu', 'mode', 'instance'])
                gmf.add_metric(label, value['value'])
                metrics.append(gmf)
        logging.debug('Prometheus Exporter: New metrics computed'+ str(metrics))
        for metric in metrics:
            yield metric

    def set_parameters(self, r):
        logging.debug('Prometheus Exporter: new job selected'+ str(r))
        self.id = r


cc = CustomCollector()
REGISTRY.register(cc)


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
        global ec

        request_data = request.get_json()
        logging.info('Forecasting API: new job requested' + str(request_data))
        #input data in the payload in json format
        nsid = request_data['nsId']
        vnfdid = request_data['vnfdId']
        metric = request_data['performanceMetric']
        nsdid = request_data['nsdId']
        il = request_data['IL']
        #dynamic request_id creation
        # req_id = uuid.uuid1()
        #static id (only for testing)
        req_id = "b4338be3-9ec3-11eb-9558-dc7196d747fd"
        reqs[str(req_id)] = {'nsId': nsid, 'vnfdId': vnfdid, 'IL': il, 'count': 2, 'nsdId': nsdid,
                             'performanceMetric': metric, 'isActive': True, 'scraperJob': None,
                             'kafkaTopic': None, 'prometheusJob0': None, 'model': None }
        logging.debug('Forecasting API: DB updated with new job '+req_id)

        #TODO
        #mapping algorithm
        model = "LSTM"
        reqs[str(req_id)]['model'] = model
        logging.debug('Forecasting API: model selected '+model)
        #TODO
        #download the model form AI/ML platform
        logging.info('Forecasting API: model '+model+' downloaded from AIMLP')

        #create kafka topic and update reqs model
        #topic = ec.createKafkaTopic(nsid)
        #reqs[str(req_id)]['topic'] = topic
        #logging.info('Forecasting API: topic '+topic+' created')
        # TODO
        # create scraper job and update the reqs model
        expression = metric+'{mode=\"idle\",nsId=\"'+nsid+'\",vnfdId=\"'+vnfdid+'\", forecasted=\"no\"'
        print(expression)
        #rep = ec.startScraperJob(nsid = nsid, topic = topic, vnfdid = vnfdid, metric = metric,
        #                      expression = expression, period = 15)
        #logging.info('Forecasting API: scraper job '+rep+' created')

        fj = ForecastingJob(id, metric, nsdid, model)
        logging.debug('Forecasting API: forecasting job created '+fj.str())
        task = Task(str(req_id), metric, 0, fj, data, POLLING)
        logging.debug('Forecasting API: forecasting job in execution')
        t = Thread(target=task.run, args=())
        t.start()
        active_jobs[str(req_id)] = {'thread': t, 'job': fj, 'task': task}
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

        logging.info('Forecasting API: request to stop forecasting job '+job_id+' received')
        if job_id in active_jobs.keys():
            element = active_jobs[job_id]
            task = element.get('task')
            #thr = element.get('task')
            task.terminate()
            #thr.join()
            active_jobs.pop(job_id)
            print(active_jobs)
            # TODO
            # delete Prometheus job pointing to the exporter
            logging.info('Forecasting API: deleted Prometheus job')
            # TODO
            # delete scraper job and update the reqs model
            logging.info('Forecasting API: deleted scraper job')
            # TODO
            # delete kafla topic and update reqs model
            logging.info('Forecasting API: deleted kafka topic')
            if job_id in reqs.keys():
                reqs[str(job_id)] = {'isActive': False}
            return 'Forecasting job '+job_id+ ' Successfully stopped', 200
        else:
            return 'Forecasting job not found', 404

    def get(self, job_id):
        global active_jobs
        global reqs

        if job_id in reqs.keys():
            logging.info('Forecasting API: GET job info, ' + str(reqs[str(job_id)]))
            return reqs[str(job_id)], 200
        else:
            logging.info('Forecasting API: GET job info, job not found '+job_id)
            return 'Forecasting job not found', 404



@restApi.route('/Forecasting/<string:job_id>/<string:il>')
@restApi.response(200, 'Success')
@restApi.response(404, 'not found')
class _ForecastingAdd(Resource):
    def put(self, job_id, il):
        if str(job_id) in reqs.keys():
            reqs[str(job_id)]['IL'] = il
            logging.info('Forecasting API: IL for job ' + job_id + ' updated to value ' + str(il))
            return 'Instantiation level updated', 200
        else:
            logging.info('Forecasting API: PUT IL, job not found '+job_id)
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


@prometheusApi.route('/metrics/<string:job_id>/<string:vnfd_id>')
@prometheusApi.response(200, 'Success')
@prometheusApi.response(404, 'Not found')
class _PrometheusExporter(Resource):
    @prometheusApi.doc(description="handling Prometheus connections")
    def get(self, job_id, vnfd_id):
        global data
        global active_jobs
        global reqs
        logging.info('Prometeheus Exporter: new metric request for job='+job_id+' and vnfdid='+vnfd_id)

        id = job_id
        isExists = False
        for key in active_jobs.keys():
            if key == id:
                isExists = True
                if reqs[str(job_id)].get('vnfdId') == vnfd_id:
                    break
        if not isExists:
            logging.info('Prometeheus Exporter: job not found ' + job_id)
            return 'Forecasting job not found', 404
        f = active_jobs[str(job_id)].get('job')
        value = f.getForecastingValue()
        #print(str(value))
        metric = reqs[str(job_id)].get('performanceMetric')

        il = reqs[str(job_id)].get('IL')
        count = reqs[str(job_id)].get('count')
        for i in range (0, il):
            vnf_name = vnfd_id + '-' + str(i + 1)
            for c in range(0, count):
                cpu = str(c)
                return_data = {
                    'job': job_id,
                    'name': vnf_name,
                    'cpu': cpu,
                    str(metric): value
                }
                return_data_str = json.dumps(return_data)
                json_obj2 = json.loads(return_data_str)
                if json_obj2['job'] not in data.keys():
                    data[id] = multiprocessing.Queue()
                #print(return_data_str)
                data[id].put(json_obj2)
                #print("push")
                #print(data[id].qsize())


        cc.set_parameters(id)
        dataF = generate_latest(REGISTRY)
        response = make_response(dataF, 200)
        response.mimetype = "text/plain"
        logging.info('Prometeheus Exporter: response= ' + str(response))
        return response


if __name__ == '__main__':
    #loadConfig()
    #setupConfigProcesses()
    #manager = Manager()
    #active_jobs = manager.dict()
    #active_processes = manager.dict()
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
    app.run(host='0.0.0.0', port=port)
    logging.info('API server started on port '+ str(port))

