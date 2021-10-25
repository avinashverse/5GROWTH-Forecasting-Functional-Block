
#from tools.externalConnections import ExternalConnections
from externalConnections import ExternalConnections


createKafka = 0
deleteKafka = 1

startScraper = 0
stopScraper = 1
stopProm = 1

'''
example
{ 
        "nsId" : "fgt-8b20af7-ebc5-4fbb-9ce4-ec6b136eb6b8",
        "vnfdId" : "dtdtvvnf",
        "performanceMetric" :  "node_cpu_seconds_total",
        "nsdId" : "DTwin",
        "IL" : 1
}
'''

sj="4cab84ab-6b3b-464a-9bfa-f8499009be6a"
pj="ef480b3f-92da-4d4b-b219-6a6b85afe0c3"

nsId = "fgt-6e44566-121b-4b8a-ba59-7cd0be562d4f"
vnfdId =  "dtdtvvnf"
performanceMetric =  "node_cpu_seconds_total"
nsdId = "DTwin"
il = 1

#expression = "avg((1 - avg by(instance) (irate("+performanceMetric+"{mode=\"idle\",nsId=\""+nsId+"\",vnfdId=\""+vnfdId+"\"}[1m]))) * 100)"
expression = performanceMetric+"{nsId=\""+nsId+"\", vnfdId=\""+vnfdId+"\" forecasted=\"no\"}"
topic = nsId + "_forecasting"


ec = ExternalConnections('../config.conf')

if deleteKafka:
    ec.deleteKafkaTopic(topic)

if createKafka:
    ec.createKafkaTopic(nsId)

if startScraper:
    sId = ec.startScraperJob(nsid = nsId, topic = topic, vnfdid = vnfdid, metric = performanceMetric,
                              expression = expression, period = 15)
    print(sId)


if stopScraper:
    sId = ec.stopScraperJob(sj)
    print(sId)

if stopProm:
    sId = ec.stopPrometheusJob(pj)
    print(sId)
