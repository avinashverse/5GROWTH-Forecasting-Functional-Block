import configparser
from http.client import HTTPConnection
from json import dumps, loads
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer
from confluent_kafka.cimpl import NewTopic


class ExternalConnections:
    def __init__(self, configfile):
        self.monIp = "192.168.1.1"
        self.monPort = 9999
        self.monUrl = "/monitoring"

        self.kIp = "192.168.1.1"
        self.kPort = 9999
        self.kUrl = "/kafka"

        self.localIp = "192.168.1.13"
        self.localPort = 6666

        config = configparser.ConfigParser()
        if configfile is None:
            config.read('default.config')
        else:
            config.read(configfile)
        if 'monitoring' in config:
            self.monIp = config['monitoring']['monitoringIP']
            self.monPort = config['monitoring']['monitoringPort']
            self.monUrl = config['monitoring']['monitoringUrl']
        if 'kafka' in config:
            self.kIp = config['kafka']['kafkaIP']
            self.kPort = config['kafka']['kafkaPort']
            #self.kUrl = config['kafka']['kafkaUrl']
        if 'local' in config:
            self.localIp = config['local']['localIP']
            self.localPort = config['local']['localPort']

    ### KAFKA APIs ###

    #create a new kafka topic
    def createKafkaTopic(self, ns_id):
        new_topics = []
        broker = self.kIp + ":" + self.kPort
        client = AdminClient({'bootstrap.servers' : broker})
        topic = ns_id + "_forecasting"
        new_topics.append(NewTopic(topic, 1, 1))
        fs = client.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic_elem, f in fs.items():
            try:
                f.result()
                print("Topic {} created".format(topic_elem))
                return topic
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic_elem, e))
                return 0

    #create a kafka topic
    def deleteKafkaTopic(self, topic):
        del_topics = []
        broker = self.kIp + ":" + self.kPort
        client = AdminClient({'bootstrap.servers' : broker})
        del_topics.append(topic)
        fs = client.delete_topics(del_topics)
        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
                return topic
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))
                return 0

    def createKafkaConsumer(self, id, topic):
        consumer = Consumer({
            'bootstrap.servers':  self.kIp + ":" + self.kPort,
            'group.id': id,
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([topic])
        return consumer

    def startPrometheusJob(self, name, vnfdId, nsId, period, job_id):
        # job_id = str(uuid4())
        header = {'Accept': 'application/json',
                  'Content-Type': 'application/json'
                  }
        # create the exporter for the job
        monitoring_uri = "http://" + self.monIp + ":" + self.monPort + self.monUrl + "/exporter"

        body = {"name": name,
                "endpoint": [ {"address": self.localIp,
                               "port": self.localPort}
                            ],
                "vnfdId": vnfdId,
                "nsId": nsId,
                "instance": name,
                "collectionPeriod": period,
                "metrics_path": "/metrics/" + job_id + "/" + vnfdId
               }
        try:
            conn = HTTPConnection(self.monIp, self.monPort)
            conn.request("POST", monitoring_uri, body = dumps(body), headers = header)
            rsp = conn.getresponse()
            exporterInfo = rsp.read()
            exporterInfo = exporterInfo.decode("utf-8")
            exporterInfo = loads(exporterInfo)
        except ConnectionRefusedError:
            print("Error, connection refused")
        return exporterInfo

    #stop a running monitoring job
    def stopPrometheusJob(self, jobId):
        header = {'Content-Type': 'application/json',
                  'Accept': 'application/json'}
        # create the exporter for the job
        path = "http://" + self.monIp + ":" + self.monPort + self.monUrl + "/exporter"
        try:
            conn = HTTPConnection(self.monIp, self.monPort)
            conn.request("DELETE", path + "/" + jobId, None, header)
            rsp = conn.getresponse()
        except ConnectionRefusedError:
            print("Error, connection refused)")

    ### SCRAPER APIs ###
    #create scraper job
    def startScraperJob(self, nsid, topic, vnfdid, metric, expression, period):
            header = {'Accept': 'application/json',
                      'Content-Type': 'application/json'
                      }
            '''
            example
            {
                "nsid": "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d",
                "vnfid": "spr2",
                "interval": 15,
                "performanceMetric": "VcpuUsageMean",
                "kafkaTopic": "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d_forecasting",
                "expression": "node_cpu_seconds_total{mode=\"idle\",nsId=\"fgt-82f4710-3d04-429a-8243-  5a2ac741fd4d\",vnfdId=\"spr2\", forecasted=\"no\"}"
            }
            '''

            body = {
                "nsid": nsid,
                "vnfid": vnfdid,
                "interval": period,
                "performanceMetric": metric,
                "kafkaTopic": topic,
                "expression": expression
            }
            path = "http://" + self.monIp + ":" + self.monPort + self.monUrl + "/prometheus_scraper"
            try:
                conn = HTTPConnection(self.monIp, self.monPort)
                conn.request("POST", path, body=dumps(body), headers=header)
                re = conn.getresponse()
                data = re.read()
                reply8 = data.decode("utf-8")
                reply = loads(reply8)
                conn.close()
            except ConnectionRefusedError:
                print("Error, connection refused")
                # the Config Manager is not running or the connection configuration is wrong
            return reply

    #delete scraper job
    def stopScraperJob(self, job_id):
            header = {'Accept': 'application/json'
                      }
            path = "http://" + self.monIp + ":" + self.monPort + self.monUrl + "/prometheus_scraper"+ str(job_id)
            try:
                conn = HTTPConnection(self.monIp, self.monPort)
                conn.request("DELETE", path, headers=header)
                rsp = conn.getresponse()
                resources = rsp.read()
                resp_alert = resources.decode("utf-8")
                resp_alert = loads(resp_alert)
                conn.close()
                return 1
            except ConnectionRefusedError:
                return 0
