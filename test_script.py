import hashlib
import json
import pprint
import shutil
import subprocess

import itertools
# from numpy import random

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
    results = dict()
    for i in range(Configuration.SDO_NUMBER):
        sdo_name = "sdo" + str(i)
        results_file = Configuration.RESULTS_FOLDER + "/" + sdo_name + ".json"
        try:
            with open(results_file, "r") as f:
                placement = json.loads(f.read())
                results[sdo_name] = placement
        except FileNotFoundError:
            continue
    results_file = Configuration.RESULTS_FOLDER + "/results.json"
    with open(results_file, "w") as f:
        f.write(json.dumps(results, indent=4))
    residual_resources = dict(rap.available_resources)
    for service, function, node in list(itertools.chain(*results.values())):
        residual_resources[node] = rap.sub_resources(residual_resources[node], rap.consumption[function])
    print("Allocation: \n" + pprint.pformat(results))
    print("Residual resources: \n" + pprint.pformat(residual_resources))

except TimeoutExpired:
    for p in p_list:
        p.kill()

# purge rabbitmq queues
purge_rabbit.purge_queues(sdos)
