import hashlib
import json
import pprint
import shutil
import subprocess

import itertools

from subprocess import TimeoutExpired

from config.config import Configuration
from resource_assignment.resource_assignment_problem import ResourceAllocationProblem


p_list = list()

# [ Configuration ]
CONF_FILE = 'default-config.ini'
configuration = Configuration(CONF_FILE)
print("SDO_NUMBER:           " + str(configuration.SDO_NUMBER))
print("NEIGHBOR_PROBABILITY: " + str(configuration.NEIGHBOR_PROBABILITY))
print("NODE_NUMBER:          " + str(configuration.NODE_NUMBER))
print("BUNDLE_PERCENTAGE:    " + str(configuration.BUNDLE_PERCENTAGE))

# [ RAP instance ]
rap = ResourceAllocationProblem()
with open(configuration.RAP_INSTANCE, mode="r") as rap_file:
    rap.parse_dict(json.loads(rap_file.read()))
sdos = ["sdo"+str(n) for n in range(configuration.SDO_NUMBER)]
nodes = ["node" + str(n) for n in range(configuration.NODE_NUMBER)]
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
with open(configuration.RAP_INSTANCE, mode="w") as rap_file:
    rap_file.write(json.dumps(rap.to_dict(), indent=4))

# clean result directory
shutil.rmtree(configuration.RESULTS_FOLDER, ignore_errors=True)

# print total resources
total_resources = rap.get_total_resources_amount()
average_resource_per_function = {r: sum([rap.get_function_resource_consumption(f)[r] for f in rap.functions])/len(rap.functions) for r in rap.resources}
average_resource_percentage_per_function = sum([average_resource_per_function[r]/total_resources[r] for r in rap.resources])/len(rap.resources)
statistical_bundle_len = len(rap.services)*(configuration.BUNDLE_PERCENTAGE/100)
average_resource_demand = statistical_bundle_len*average_resource_percentage_per_function
print("- Resources Statistics - ")
print("Total resources: \n" + pprint.pformat(total_resources))
print("Average resources per function: \n" + pprint.pformat(average_resource_per_function))
print("Average demand percentage per function: " + str(round(average_resource_percentage_per_function, 3)))
print("Statistical bundle len: " + str(round(statistical_bundle_len, 2)))
print("Statistical average demand percentage per bundle: " + str(round(average_resource_demand, 3)))
print("Statistical total demand percentage: " + str(round(average_resource_demand*configuration.SDO_NUMBER, 3)))
print("- -------------------- - ")

print("- Run Orchestration - ")
bundle_arg = []
for i in range(configuration.SDO_NUMBER):
    sdo_name = "sdo" + str(i)
    service_bundle = [s for s in rap.services
                      if int(str(int(hashlib.sha256((sdo_name+s).encode()).hexdigest(), 16))[-2:]) < configuration.BUNDLE_PERCENTAGE]
    if sdo_name == "sdo9":
        service_bundle = [s for s in rap.services
                          if int(str(int(hashlib.sha256(("sdo10"+s).encode()).hexdigest(), 16))[-2:]) < configuration.BUNDLE_PERCENTAGE]
    elif sdo_name == "sdo15":
        service_bundle = [s for s in rap.services
                          if int(str(int(hashlib.sha256(("sdo14"+s).encode()).hexdigest(), 16))[-2:]) < configuration.BUNDLE_PERCENTAGE]
    elif sdo_name == "sdo19":
        service_bundle = service_bundle[:3]
    if len(service_bundle) == 0:
        service_bundle.append(rap.services[0])
    print(sdo_name + " : " + str(service_bundle))
    bundle_arg += [sdo_name] + service_bundle + [","]

bundle_arg = bundle_arg[:-1]
print(" ".join(bundle_arg))

p = subprocess.Popen(["python3", "centralized_main.py"] + bundle_arg + ["-l", configuration.LOG_LEVEL, "-d", CONF_FILE, "-o"])

try:
    p.wait(timeout=30)
except TimeoutExpired:
    p.kill()

print(" - Collect Results - ")
# fetch post process information
placements = dict()
message_rates = dict()
private_utilities = list()
for i in range(configuration.SDO_NUMBER):
    sdo_name = "sdo" + str(i)
    utility_file = configuration.RESULTS_FOLDER + "/utility_" + sdo_name + ".json"
    placement_file = configuration.RESULTS_FOLDER + "/placement_" + sdo_name + ".json"

    try:
        with open(utility_file, "r") as f:
            utility = int(f.read())
            private_utilities.append(utility)
        with open(placement_file, "r") as f:
            placement = json.loads(f.read())
            placements[sdo_name] = placement
    except FileNotFoundError:
        continue

# sum of private utilities
print("Sum of private utilities: " + str(sum(private_utilities)))

# print assignment info
placement_file = configuration.RESULTS_FOLDER + "/results.json"
with open(placement_file, "w") as f:
    f.write(json.dumps(placements, indent=4))
residual_resources = dict(rap.available_resources)
for service, function, node in list(itertools.chain(*placements.values())):
    residual_resources[node] = rap.sub_resources(residual_resources[node], rap.consumption[function])
total_residual_resources = {r: sum([residual_resources[n][r] for n in rap.nodes]) for r in rap.resources}
total_residual_resources_percentage = sum([total_residual_resources[r]/total_resources[r] for r in rap.resources])/len(rap.resources)
used_resources_percentage = 1 - total_residual_resources_percentage
print("Allocation: \n" + pprint.pformat(placements))
print("Residual resources: \n" + pprint.pformat(residual_resources))
print("Percentage of assigned resources: " + str(round(used_resources_percentage, 3)))
print("Percentage of successfully allocated bundles: " + str(round(len([u for u in private_utilities
                                                                        if u > 0]), 3)/configuration.SDO_NUMBER))