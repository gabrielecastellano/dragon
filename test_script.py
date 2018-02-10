import hashlib
import json
import pprint
import shutil
import subprocess

import itertools
# from numpy import random
from collections import OrderedDict

from subprocess import TimeoutExpired

from config.configuration import Configuration
from network_plotter import NetworkPlotter
from resource_allocation.resoruce_allocation_problem import ResourceAllocationProblem

from scripts import purge_rabbit

p_list = list()

# [ Configuration ]
print("SDO_NUMBER:           " + str(Configuration.SDO_NUMBER))
print("NEIGHBOR_PROBABILITY: " + str(Configuration.NEIGHBOR_PROBABILITY))
print("NODE_NUMBER:          " + str(Configuration.NODE_NUMBER))
print("BUNDLE_PERCENTAGE:    " + str(Configuration.BUNDLE_PERCENTAGE))

# [ RAP instance ]
rap = ResourceAllocationProblem()
with open(Configuration.RAP_INSTANCE, mode="r") as rap_file:
    rap.parse_dict(json.loads(rap_file.read()))
sdos = ["sdo"+str(n) for n in range(Configuration.SDO_NUMBER)]
nodes = ["node" + str(n) for n in range(Configuration.NODE_NUMBER)]
'''
services = ["s1", "s2", "s3", "s4", "s5", "s6"]
functions = ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"]
resources = ["cpu", "memory", "bandwidth"]
consumption = dict()
for function in functions:
    consumption[function] = dict()
    consumption[function]["cpu"] = random.randint(1, 4)
    consumption[function]["memory"] = int(random.triangular(1, 1024, 4096))
    consumption[function]["bandwidth"] = int(random.triangular(1, 256, 1024))
# manual one, expensive function
# consumption["f1"]["cpu"] = 12
# consumption["f1"]["memory"] = 6*1024
# consumption["f1"]["bandwidth"] = 3*512

consumption = {'f1': {'bandwidth': 393, 'cpu': 3, 'memory': 1737},
               'f2': {'bandwidth': 970, 'cpu': 4, 'memory': 3299},
               'f3': {'bandwidth': 970, 'cpu': 3, 'memory': 1093},
               'f4': {'bandwidth': 422, 'cpu': 3, 'memory': 2014},
               'f5': {'bandwidth': 182, 'cpu': 4, 'memory': 295},
               'f6': {'bandwidth': 247, 'cpu': 1, 'memory': 3610},
               'f7': {'bandwidth': 868, 'cpu': 4, 'memory': 3294},
               'f8': {'bandwidth': 361, 'cpu': 4, 'memory': 3299},
               'f9': {'bandwidth': 275, 'cpu': 1, 'memory': 1404}}

available_resources = {"cpu": 25, "memory": 20*1024, "bandwidth": 4*1024}
implementation = {
    "s1": ["f1", "f2", "f3"],
    "s2": ["f3", "f4"],
    "s3": ["f2", "f4", "f5"],
    "s4": ["f2", "f3"],
    "s5": ["f4", "f5", "f6"],
    "s6": ["f1", "f6"]
}
'''
rap.sdos = sdos
rap.nodes = nodes
with open(Configuration.RAP_INSTANCE, mode="w") as rap_file:
    rap_file.write(json.dumps(rap.to_dict(), indent=4))

# purge rabbitmq queues
purge_rabbit.purge_queues(sdos)

# clean result directory
shutil.rmtree(Configuration.RESULTS_FOLDER, ignore_errors=True)

# plot the topology
# NetworkPlotter(rap.sdos).graphical_plot()
NetworkPlotter(rap.sdos).print_topology()

for i in range(Configuration.SDO_NUMBER):
    sdo_name = "sdo" + str(i)
    service_bundle = [s for s in rap.services
                      if int(str(int(hashlib.sha256((sdo_name+s).encode()).hexdigest(), 16))[-2:]) < Configuration.BUNDLE_PERCENTAGE]
    if len(service_bundle) == 0:
        service_bundle.append(rap.services[0])
    print(sdo_name + " : " + str(service_bundle))
    # call("python3 main.py " + sdo_name + " " + ''.join(service_bundle) + " -l VERBOSE --log-on-file &", shell=True)
    #if sdo_name != "sdo0":
    p = subprocess.Popen(["python3", "main.py", sdo_name] + service_bundle + ["-l", Configuration.LOG_LEVEL, "-o"])
    #else:
    #    p = subprocess.Popen(["python3", "main.py", sdo_name] + service_bundle + ["-l", Configuration.LOG_LEVEL])
    p_list.append(p)

try:
    for p in p_list:
        p.wait(timeout=200)
    placements = dict()
    message_rates = dict()
    for i in range(Configuration.SDO_NUMBER):
        sdo_name = "sdo" + str(i)
        placement_file = Configuration.RESULTS_FOLDER + "/placement_" + sdo_name + ".json"
        rates_file = Configuration.RESULTS_FOLDER + "/rates_" + sdo_name + ".json"
        try:
            with open(placement_file, "r") as f:
                placement = json.loads(f.read())
                placements[sdo_name] = placement
            with open(rates_file, "r") as f:
                rates = OrderedDict(json.loads(f.read()))
                message_rates[sdo_name] = rates
        except FileNotFoundError:
            continue

    placement_file = Configuration.RESULTS_FOLDER + "/results.json"
    with open(placement_file, "w") as f:
        f.write(json.dumps(placements, indent=4))
    residual_resources = dict(rap.available_resources)
    for service, function, node in list(itertools.chain(*placements.values())):
        residual_resources[node] = rap.sub_resources(residual_resources[node], rap.consumption[function])
    print("Allocation: \n" + pprint.pformat(placements))
    print("Residual resources: \n" + pprint.pformat(residual_resources))

    begin_time = min([float(next(iter(message_rates[sdo])).split(":")[0]) for sdo in message_rates])
    next_begin_time = begin_time
    global_rates = OrderedDict()
    while len(message_rates) > 0:
        # next_begin_time = min([float(next(iter(message_rates[sdo])).split(":")[0]) for sdo in message_rates])
        # next_end_time = max([float(next(iter(message_rates[sdo])).split(":")[1]) for sdo in message_rates])
        next_end_time = next_begin_time+Configuration.SAMPLE_FREQUENCY
        in_range_counter = 0
        for sdo in message_rates:
            if len(message_rates[sdo]) > 0:
                # in_range_keys = [k for k in message_rates[sdo] if float(k.split(":")[0]) >= next_begin_time and float(k.split(":")[1]) <= next_end_time]
                in_range_keys = [k for k in message_rates[sdo] if float(k.split(":")[1]) <= next_end_time]
                in_range_counter += sum([message_rates[sdo][k] for k in in_range_keys])
                for k in in_range_keys:
                    del message_rates[sdo][k]
        for sdo in dict(message_rates):
            if len(message_rates[sdo]) == 0:
                del message_rates[sdo]
        global_rates[float("{0:.3f}".format(next_end_time-begin_time))] = in_range_counter/(next_end_time-next_begin_time)
        next_begin_time = next_end_time

    print("Message rates: \n" + pprint.pformat(global_rates))

except TimeoutExpired:
    for p in p_list:
        p.kill()

# purge rabbitmq queues
purge_rabbit.purge_queues(sdos)
