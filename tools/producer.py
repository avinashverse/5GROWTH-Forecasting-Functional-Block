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

# python imports

from confluent_kafka import Producer
import sys
kafkaIP = "10.30.2.118"
kafkaPort = 9092

topic = "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d_forecasting"
#topic = "Test"
conf = {'bootstrap.servers': kafkaIP + ":" + str(kafkaPort)}

p = Producer(**conf)

# Producer configuration
message = """
[
    {
        "metric": {
            "__name__": "node_cpu_seconds_total",
            "cpu": "0",
            "exporter": "node_exporter",
            "instance": "spr2-1",
            "job": "58d10e62-bdf5-44a9-9b35-0ba4ddd14b74",
            "mode": "idle",
            "nsId": "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d",
            "vnfdId": "spr2",
            "forecasted": "no"
        },
        "value": [
            1618383722.708,
            "100"
        ],
        "type_message": "metric"
    }
]
"""
'''
    , 
    {
        "metric": {
            "__name__": "node_cpu_seconds_total",
            "cpu": "1",
            "exporter": "node_exporter",
            "instance": "spr2-1",
            "job": "58d10e62-bdf5-44a9-9b35-0ba4ddd14b74",
            "mode": "idle",
            "nsId": "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d",
            "vnfdId": "spr2",
            "forecasted": "no"
        },
        "value": [
            1618383722.708,
            "9.05"
        ],
        "type_message": "metric"
    }
]
"""
'''


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))


#p.produce(topic, key="metric", value=message, callback=delivery_callback)
p.produce(topic, value=message, callback=delivery_callback)
p.flush()

p.poll(1)
'''
import sys

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)



    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).

    # Read lines from stdin, produce each line to Kafka
    for line in sys.stdin:
        try:
            # Produce line (without newline)
            p.produce(topic, line.rstrip(), callback=delivery_callback)

        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

'''