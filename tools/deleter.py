
#from tools.externalConnections import ExternalConnections
from externalConnections import ExternalConnections


createKafka = 0
deleteKafka = 1

startScraper = 0
stopScraper = 0
stopProm = 0

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

sj="0a34ef28-3bb6-4223-a3f7-289a1be92a7a"
pj="51cb0cfe-0002-4a65-99e9-1635b219c861"

nsId = "fgt-4f61c57-9ce2-441e-9919-7674dda57c9d"
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


