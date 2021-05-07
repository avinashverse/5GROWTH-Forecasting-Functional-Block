##########################################
# Unidirectional link delay probe implementation for distributed system
# The code has been developed by Andrea Sgambelluri
# from Scuola Superiore Sant'Anna, Pisa, Italy
#
# please cite the paper
# A. Sgambelluri, X. C. Moreno, S. Spadaro and P. Monti, "Evaluating Link Latency in Distributed SDN-Based Control Plane Architectures," ICC 2019 - 2019 IEEE International Conference on Communications (ICC), Shanghai, China, 2019, pp. 1-6, doi: 10.1109/ICC.2019.8761961
##########################################
#
# to run the Prometheus exporter: python exporter_one_way_latency.py
# This Prometheus exporter works in mode DST probe default.
# Run request that you see below to start an instance probe in SRC mode on the exporter.
##########################################
####### API  examples ####################
##########################################
# Activating a SRC probe instance
#curl --location --request GET 'http://192.168.122.134:15000/metrics?ip=192.168.122.1&polling=2'
# where parameter:
# ip=192.168.122.1 is IP address of peer to which will be done measurement
# polling=2 is latency evaluation period, unit seconds
# The Prometheus exporter stops measurement if it don’t receive request from Prometheus server during last 300 seconds.
#
##########################################
import multiprocessing
import traceback
from json import JSONDecodeError

from flask import Flask, make_response
from flask import request
import socket
import time
import json
import threading

from prometheus_client import REGISTRY, generate_latest
from prometheus_client.metrics_core import GaugeMetricFamily

PROBERESTPORT = 15000

# default values
IP = "192.168.122.1"  # default remote IP
SRCOFFPORT = 14999  # default port of the socket for the offset traffic
DSTPROBEPORT = 14998  # defaukt port of the socket for the probe traffic
POLLING = 2  # default latency evaluation period
limitCounter = 15000  # tag to limit the counter ID
quit = {}  # dict used to store the tags to stop the execution of a probe
poll = {}  # dict storing the latency evaluation period of each probe
offset = {}  # dict storing the evaluated offset for each probe
loss = {}  # dict storing the packet loss values for each probe
latency = {}  # dict storing the latency values for eache probe
lastCount = {}  # dict storing the last value of the received count
lossPercent = {}  # dict storing the packet loss values for each probe
totalCount = {}  # dict storing the total count fof the received packets
MAXNUMBEROFCONNECTIONS = 100 #Maximum number of  DST probe and SRC off Set Servers
PROMETHEUS_REQUEST_TIMEOUT = 300 # SECONDS When this time out is expired exporters stops sending messages
map_of_queue = {} #Map to save queue for each peer ip
is_srcoffset_server_started = False # Helps to run Server srcoffset one time
map_prometheus_request_time = {} #map of last Prometheus requests for each peer ip
map_sockets = {} #Map to save sockets for each peer ip
app = Flask(__name__)

######Destination probe funcitons#########################

# function to handle offset traffic at the destination node
def DSToffSet(ip, port, xID, laddr, raddr):
    global offset
    global quit
    quit[xID] = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connected = 0
    print("Waiting to connect the remote Offset server " + str(xID) + "...")
    while not connected:
        try:
            sock.connect((ip, port))
            laddr =  sock.getsockname()
            raddr =  sock.getpeername()
            connected = 1
            print("Connection established with the remote offset server for instance " + str(xID))
        except Exception:
            print("Connection not established " + ip + " " + str(port))
            time.sleep(5)
            connected = 0
    offset[xID] = 0
    while not quit[xID]:
        t2 = int(round(time.time() * 1000))
        Message = "{ \"id\": \"" + str(xID) + "\", \"tTX\": " + str(t2) + ", \"tRX\": 0 }"
        try:
            sock.sendall(Message.encode('utf-8'))
        except BrokenPipeError:
            print("DSToffSet: Connection from " + raddr[0] + " port: " + str(raddr[1]))
            print("DSToffSet: to " + laddr[0] + " port: " + str(laddr[1]) + " is broken")
            break
        received = False
        # waiting for the reply
        while not received:
            try:
                datax = sock.recv(1024).decode('utf-8')
                t4 = int(round(time.time() * 1000))
                json_obj2 = json.loads(datax)
            except JSONDecodeError:
                time.sleep(1)
                print(traceback.format_exc())

            rxID = json_obj2.get('id')
            tTX = int(json_obj2.get('tTX'))
            tRX = int(json_obj2.get('tRX'))
            offset[xID] = tRX - tTX - (t4 - tTX)/2
            # print ("Offset="+str(offset))
            received = True
        # time.sleep(POLLING)
    sock.close()
    del offset[xID]




#############Source probe funcitons#########################




# function to handle probe traffic at the source node
def SRCprobe(ip, port, laddr, raddr):
    global quit
    global poll
    global log
    global limitCounter
    global map_sockets
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connected = 5
    print("Connecting remote probe server " + ip + "....")
    while True:
        try:
            sock2.connect((ip, port))
            laddr =  sock2.getsockname()
            raddr =  sock2.getpeername()
            xID = laddr[0] + "_" + raddr[0]
            quit[xID] = False
            poll[xID] = POLLING
            if xID in map_sockets.keys():
                map_sockets[xID].append(sock2)
            else:
                map_sockets.update({xID: [sock2]})
            print("Connection with remote probe server " + str(xID) + " established")
            threading.Thread(target=recv_forward_data, kwargs={"sock": sock2,
                                                               "laddr": laddr,
                                                               "raddr": raddr}).start()
            break
        except Exception:
            print(traceback.format_exc())
            print("Connection with remote probe server not established :" + ip + " port: " + str(port))
            time.sleep(5)
            connected = connected - 1
            if connected == 0:
                return

    counter = 1
    while not quit[xID]:
        t0 = int(round(time.time() * 1000))
        Message = "{ \"id\": \"" + xID + "\", \"count\": " + str(counter) + ", \"t0\": " + str(t0) + " }"
        if counter == limitCounter:
            counter = 1
        else:
            counter = counter + 1
        try:
            sock2.sendall(Message.encode('utf-8'))
        except BrokenPipeError:
            print("SRCprobe: Connection from " + laddr[0] + " port: " + str(laddr[1]))
            print("SRCprobe: to " + raddr[0] + " port: " + str(raddr[1]) + " is broken")
            del map_of_queue[xID]
            break
        time.sleep(poll[xID])
        if time.time() - map_prometheus_request_time[ip] > PROMETHEUS_REQUEST_TIMEOUT:
            print("Prometheus request timeout for xID: " + xID)

            if xID in map_sockets.keys():
                for local_sock in map_sockets[xID]:
                    local_sock.close()
            del map_of_queue[xID]
            print("Connection from " + laddr[0] + " to " + raddr[0] + " is closed")
            break
    sock2.close()


def recv_forward_data(sock, laddr, raddr):
    global map_of_queue
    json_obj2 = ""
    while True:
        try:
            datax = sock.recv(1024).decode('utf-8')
        except (BrokenPipeError, OSError):
            print("SRCprobe: Connection from " + laddr[0] + " port: " + str(laddr[1]))
            print("SRCprobe: to " + raddr[0] + " port: " + str(raddr[1]) + " is broken")
            break
        try:
            json_obj2 = json.loads(datax)
            id = json_obj2['probe']
            if json_obj2['probe'] not in map_of_queue.keys():
                map_of_queue[id] = multiprocessing.Queue()
            map_of_queue[id].put(json_obj2)
        except JSONDecodeError:
            time.sleep(1)
            print(traceback.format_exc())
        print("forward data: " + str(json_obj2))


def DSTprobe_server_thread(con, xID, laddr, raddr):
    global offset
    global quit
    global lastCount
    global loss
    global lossPercent
    global latency
    global limitCounter
    previos_linklatency = None
    while True:
        rxId = ""
        json_obj = {}
        received = con.recv(1024)
        tRX = int(round(time.time() * 1000))

        if not received:
            con.close()
            print("Connection from " + raddr[0] + " to " + laddr[0] + "is closed")
            break
        try:
            json_obj = json.loads(received.decode('utf-8'))
            rxId = json_obj.get('id')
        except JSONDecodeError:
            time.sleep(1)
            print(traceback.format_exc())
            continue

        if rxId in offset.keys():
            tTX = int(json_obj.get('t0'))
            count = int(json_obj.get('count'))

            latency[xID] = tRX - tTX + offset[xID]
            # fix to not return negative latency values
            print(latency[xID])
            if latency[xID] < 0:
                latency[xID] = 0
            # samples out of sequence
            if count != lastCount[xID] + 1:
                # received count is 1
                if count == 1:
                    # loss of packet(s) at the end of the series
                    if lastCount[xID] != limitCounter:
                        # print "losses"
                        totalCount[xID] = totalCount[xID] - lastCount[xID] + limitCounter + count
                        loss[xID] = loss[xID] + (limitCounter - lastCount[xID])
                    # counter value restarted
                    else:
                        # print "restart numbering"
                        totalCount[xID] = totalCount[xID] + count
                else:
                    totalCount[xID] = totalCount[xID] - lastCount[xID] + count
                    loss[xID] = loss[xID] + (count - lastCount[xID])
            else:
                totalCount[xID] = totalCount[xID] + 1
            lossPercent[xID] = round(loss[xID] / totalCount[xID], 3)
            lastCount[xID] = count
            if previos_linklatency == None:
                previos_linklatency = latency[xID]
            jitter = abs(previos_linklatency - latency[xID])
            previos_linklatency = latency[xID]
            return_data = {
                "probe": rxId,
                "linklatency": latency[xID],
                "jitter": jitter,
                "packetLoss_percent": lossPercent[xID],
                "packetLoss": loss[xID],
                "totalTXPackets": totalCount[xID]
            }

            return_data_str = json.dumps(return_data)
            try:
                con.sendall(return_data_str.encode('utf-8'))
            except BrokenPipeError:
                local = con.getsockname()
                print("DSTprobe_server_thread: Connection from " + raddr[0] + " port: " + str(raddr[1]))
                print("DSTprobe_server_thread: to " + laddr[0] + " port: " + str(laddr[1]) + " is broken")

                break
            print("Probe " + str(rxId) + ": linkLatency=" + str(latency[xID]) + "\tpacketLoss=" + str(
                loss[xID]) + "\tpacketLoss_percent=" + str(lossPercent[xID]) + "\ttotalTXPackets=" + str(totalCount[xID]))
        else:
            print("Activating probe traffic handler for instance " + str(rxId))
            count = int(json_obj.get('count'))
            totalCount[xID] = totalCount[xID] - lastCount[xID] + count
            lastCount[xID] = count


def SRCoffSet_server_thread(con, laddr, raddr):
    global map_sockets
    while True:
        try:
            received = con.recv(1024)
            try:
                json_obj = json.loads(received.decode('utf-8'))

            except JSONDecodeError:
                time.sleep(1)
                continue
            xId = json_obj.get('id')
            tTX = int(json_obj.get('tTX'))
            tRX = int(round(time.time() * 1000))
            Message = "{ \"id\": \"" + xId + "\", \"tTX\": " + str(tTX) + ", \"tRX\": " + str(tRX) + " }"
            con.send(Message.encode('utf-8'))
            if xId in map_sockets.keys():
                map_sockets[xId].append(con)
            else:
                map_sockets.update({xId: [con]})
        except BrokenPipeError:
            print("SRCoffSet_server_thread: Connection from " + laddr[0] + " port: " + str(laddr[1]))
            print("SRCoffSet_server_thread: to " + raddr[0] + " port: " + str(raddr[1]) + " is broken")
            break

def dst_server_main_thread():
    global offset
    global quit
    global lastCount
    global loss
    global lossPercent
    global latency
    global limitCounter
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock2.bind(("", DSTPROBEPORT))

    print("Dst Probe server instance started on port " + str(DSTPROBEPORT))
    sock2.listen(MAXNUMBEROFCONNECTIONS)
    while True:
        con, cliente = sock2.accept()
        laddr =  con.getsockname()
        raddr = con.getpeername()
        xID = raddr[0] + "_" + laddr[0]
        lastCount[xID] = 0
        totalCount[xID] = 0
        loss[xID] = 0
        lossPercent[xID] = 0
        latency[xID] = 0

        threading.Thread(target=DSTprobe_server_thread, kwargs={"con": con,
                                                                "xID": xID,
                                                               "laddr": con.getsockname(),
                                                               "raddr": con.getpeername()
                                                                }).start()
        threading.Thread(target=DSToffSet, kwargs={"ip": cliente[0],
                                                   "port": SRCOFFPORT,
                                                   "xID": xID,
                                                   "laddr": con.getsockname(),
                                                   "raddr": con.getpeername()}).start()

def srcoffset_server_main_thread():
    global is_srcoffset_server_started
    if is_srcoffset_server_started == False:
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock2.bind(("", SRCOFFPORT))

        print("Src Probe server instance started on port " + str(SRCOFFPORT))
        sock2.listen(MAXNUMBEROFCONNECTIONS)
        is_srcoffset_server_started = True
        while True:
            con, cliente = sock2.accept()
            threading.Thread(target=SRCoffSet_server_thread, kwargs={"con": con,
                                                               "laddr": con.getsockname(),
                                                               "raddr": con.getpeername()}).start()

class SummMessages(object):
    def __init__(self):
        self.dict_sum = {}
        self.dict_number = {}

    def add(self, object):
        session_id = ""
        host = object.get("probe")
        host = host.split("_")[1]
        del object["probe"]
        previos_linklatency = None
        for key, value in object.items():
            if key == 'totalTXPackets':
                self.dict_sum.update({key: {host: value}})
                continue
            if key == 'packetLoss':
                self.dict_sum.update({key: {host: value}})
                continue
            if key == 'packetLoss_percent':
                self.dict_sum.update({key: {host: value}})
                continue
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
                if parameter == 'totalTXPackets':
                    dict_result.update({parameter: [{
                                                    "host": host,
                                                    "value": value2}]})
                    continue
                if parameter == 'packetLoss':
                    dict_result.update({parameter: [{
                                                    "host": host,
                                                    "value": value2}]})
                    continue
                if parameter == 'packetLoss_percent':
                    dict_result.update({parameter: [{
                                                    "host": host,
                                                    "value": value2}]})
                    continue


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
        return  dict_result



class CustomCollector(object):
    def __init__(self):
        self.ip = ""
        self.polling = ""

    def collect(self):
        global map_of_queue
        found_key = ""
        for key in map_of_queue.keys():
            if self.ip == key.split("_")[1]:
                found_key = key
        if found_key == "":
            return None

        queue = map_of_queue[found_key]
        summ_m = SummMessages()
        while not queue.empty():
            summ_m.add(queue.get())

        result = summ_m.get_result()
        metrics = []
        for parameter, values in result.items():
            if parameter == "linklatency":
                gmf = GaugeMetricFamily(parameter, "avg_linklatency", labels=['host'])
            elif parameter == "jitter":
                gmf = GaugeMetricFamily(parameter, "avg_jitter", labels=['host'])
            else:
                gmf = GaugeMetricFamily(parameter, parameter, labels=['host'])
            for value in values:
                gmf.add_metric([value['host']], value['value'])
            metrics.append(gmf)
        for metric in metrics:
            yield metric

    def set_parameters(self, r):
        self.ip = r["ip"]
        self.polling = r["polling"]


cc = CustomCollector()
REGISTRY.register(cc)



@app.route('/metrics', methods = ['GET'])
def metrics():
    global POLLING
    params = request.args
    ip = params['ip']
    POLLING = int(params['polling'])
    map_prometheus_request_time.update({ip: time.time()})
    is_exists = False
    for key in map_of_queue.keys():
        if key.split("_")[1] == ip:
            is_exists = True
            break

    if is_exists == False:
        thread1 = threading.Thread(target=srcoffset_server_main_thread)
        thread1.start()
        thread2 = threading.Thread(target=SRCprobe, kwargs={"ip": ip,
                                                            "port": DSTPROBEPORT,
                                                            "laddr": {},
                                                            "raddr": {}})
        thread2.start()
        response = make_response("no data", 404)
        response.mimetype = "text/plain"
        return response

    cc.set_parameters(params)
    data = generate_latest(REGISTRY)
    response = make_response(data , 200)
    response.mimetype = "text/plain"
    return response


def start_one_way_latency_server():
    threading.Thread(target=dst_server_main_thread).start()

# main command
if __name__ == "__main__":
    start_one_way_latency_server()
    app.run(host='0.0.0.0', port=PROBERESTPORT)
