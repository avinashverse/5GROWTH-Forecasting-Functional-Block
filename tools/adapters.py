import time
from confluent_kafka import KafkaError, KafkaException
import json
from confluent_kafka.error import ConsumeError


def mconverter(value):
    """
     :param value: the input metric to be converted
     """
    if value == "VcpuUsageMean":
        metric = "node_cpu_seconds_total"
    elif value == "latency":
        metric = "app_latency"
    else:
        metric = None
    return metric


def parsermsg(json_data, log):
    """
    :param json_data: the message received
    :param log: message logger
    """

    loaded_json = json.loads(json_data)
    log.debug("Instance CHeck: received data: \n{}".format(loaded_json))
    keys = []
    for element in loaded_json:
         mtype = element['type_message']
         if mtype == "metric":
             name = element['metric']['__name__']
             if "node_cpu_seconds_total" in name:
                 instance = element['metric']['instance']
                 if instance not in keys:
                    keys.append(instance)
    return keys


def instancecheck(consumer, duration, log):
    """
    :param consumer: the Kafka consumer, reading from the topic
    :param duration: value to set the timeout of the detection process
    :param log: message logger
    """
    log.debug("Instance CHeck: Starting the Kafka Consumer")
    timeout = time.time() + duration
    instances = []
    while True:
        if time.time() > timeout:
            consumer.close()
            break
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    log.error('Instance CHeck: %% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                insts = parsermsg(msg.value(), log)
                for inst in insts:
                    if inst not in instances:
                        instances.append(inst)

        except ConsumeError as e:
            log.error("Instance CHeck: Consumer error: {}".format(str(e)))
            consumer.close()
    return instances
