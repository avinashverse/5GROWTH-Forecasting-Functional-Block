

def metricConverter(value):
    if value == "vcpuUsageMean":
        metric = "node_cpu_seconds_total"
    elif value == "latency":
        metric = "app_latency"
    else:
        metric = None
    return metric

