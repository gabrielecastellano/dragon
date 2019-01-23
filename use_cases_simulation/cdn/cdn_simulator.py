import json
import os
import shutil
import subprocess

# from numpy import random
from collections import OrderedDict
from numpy import random

from subprocess import TimeoutExpired

from config.config import Configuration
from resource_assignment.resoruce_allocation_problem import ResourceAllocationProblem


def herfindahl_index(users_per_node):

    return sum([(users_per_node[n]/sum([users_per_node[n] for n in users_per_node])*100)**2 for n in users_per_node])


def gini_index(users_per_node):

    gini = 0
    for node in users_per_node:
        q_i, p_i = gini_term(users_per_node, node)
        gini += (p_i - q_i)
    return 2/(len(users_per_node) - 1)*gini


def gini_term(users_per_node, node):

    od = OrderedDict(sorted(users_per_node.items(), key=lambda k: k[1]))
    i = list(od.keys()).index(node)
    total_users = sum([users_per_node[n] for n in users_per_node])
    scattered_users = sum(list(od.values())[:i+1])
    # scattered_users = sum([users_per_node[n] for n in users_per_node if users_per_node[n] <= users_per_node[node]])
    q_i = scattered_users/total_users
    # p_i = len([users_per_node[n] for n in users_per_node if users_per_node[n] <= users_per_node[node]])/len(users_per_node)
    p_i = len(list(od.values())[:i+1]) / len(users_per_node)
    return q_i, p_i


def variation_index(users_per_node_before, users_per_node_after):

    # return max([abs(users_per_node_before[n] - users_per_node_after[n]) for n in users_per_node_before])/100
    return sum([max((users_per_node_after[n] - users_per_node_before[n]), 0) for n in users_per_node_before]) / (sum(users_per_node_after.values()))


def users_range_size(data, user_a, user_b):

    return sum([size for i, size in enumerate(data) if user_a <= i <= user_b])


CONF_FILE = 'config/cdn_config.ini'
configuration = Configuration(CONF_FILE)

# [ RAP instance ]
rap = ResourceAllocationProblem()
with open(configuration.RAP_INSTANCE, mode="r") as rap_file:
    rap.parse_dict(json.loads(rap_file.read()))
sdos = ["sdo"+str(n) for n in range(configuration.SDO_NUMBER)]
nodes = ["node" + str(n) for n in range(10)]

rap.sdos = sdos
rap.nodes = nodes
with open(configuration.RAP_INSTANCE, mode="w") as rap_file:
    rap_file.write(json.dumps(rap.to_dict(), indent=4))

# clean result directory
shutil.rmtree(configuration.RESULTS_FOLDER, ignore_errors=True)

stationary_iterations = [2, 1, 2, 2, 1, 2, 1, 1, 1, 1, 2, 1, 3, 3, 2, 3, 2, 3, 1, 2]
# users_data = [188, 465, 261, 344, 287, 337, 233, 400, 189, 43, 466, 146, 14, 428, 24, 473, 219, 367, 186, 447, 147, 156, 17, 132, 307, 111, 388, 219, 107, 6, 300, 79, 270, 440, 222, 148, 236, 171, 94, 227, 87, 276, 457, 131, 175, 36, 491, 85, 139, 67, 84, 190, 285, 456, 263, 443, 11, 143, 198, 479, 164, 37, 127, 360, 185, 484, 320, 275, 277, 506, 22, 407, 3, 330, 253, 440, 217, 289, 247, 258, 170, 160, 2, 41, 123, 155, 357, 162, 286, 178, 215, 394, 219, 26, 491, 52, 384, 261, 183, 463]
# users_data = [187, 181, 187, 189, 183, 181, 189, 180, 190, 189, 93, 95, 95, 99, 95, 96, 100, 95, 90, 96, 73, 73, 70, 80, 73, 74, 70, 78, 80, 80, 504, 500, 500, 504, 508, 503, 510, 504, 502, 509, 340, 342, 347, 348, 342, 343, 348, 346, 347, 340, 22, 30, 30, 30, 26, 29, 20, 22, 30, 25, 267, 263, 264, 260, 264, 267, 260, 262, 269, 260, 212, 219, 218, 210, 217, 213, 216, 214, 210, 220, 101, 108, 102, 104, 103, 105, 103, 102, 100, 106, 241, 250, 243, 240, 241, 246, 245, 241, 250, 244]
users_data = [504, 500, 500, 504, 508, 503, 510, 504, 502, 509, 93, 95, 95, 99, 95, 96, 100, 95, 90, 96, 73, 73, 70, 80, 73, 74, 70, 78, 80, 80, 504, 500, 500, 504, 508, 503, 510, 504, 502, 509, 340, 342, 347, 348, 342, 343, 348, 346, 347, 340, 22, 30, 30, 30, 26, 29, 20, 22, 30, 25, 22, 30, 30, 30, 26, 29, 20, 22, 30, 25, 504, 500, 500, 504, 508, 503, 510, 504, 502, 509, 101, 108, 102, 104, 103, 105, 103, 102, 100, 106, 241, 250, 243, 240, 241, 246, 245, 241, 250, 244]
# users_traffic = [53, 108, 118, 107, 178, 93, 179, 178, 90, 62, 24, 22, 36, 129, 153, 193, 48, 101, 168, 30, 12, 29, 168, 69, 11, 109, 188, 120, 165, 17, 46, 47, 69, 14, 188, 151, 54, 28, 141, 110, 39, 152, 157, 194, 48, 17, 142, 88, 120, 164, 92, 30, 27, 16, 130, 192, 133, 80, 100, 106, 45, 137, 50, 124, 13, 53, 142, 117, 108, 121, 193, 129, 194, 186, 83, 194, 146, 143, 125, 129, 166, 137, 71, 57, 17, 195, 134, 85, 131, 114, 184, 123, 111, 180, 66, 175, 185, 85, 167, 40]
# users_traffic = [99, 100, 100, 99, 99, 100, 99, 100, 100, 100, 100, 99, 100, 99, 100, 99, 100, 100, 99, 100, 100, 99, 100, 100, 99, 100, 99, 99, 100, 99, 99, 99, 100, 99, 100, 99, 99, 99, 99, 99, 99, 99, 100, 99, 100, 99, 100, 99, 100, 100, 100, 99, 99, 99, 100, 99, 99, 99, 99, 99, 99, 99, 99, 100, 100, 99, 100, 100, 99, 99, 99, 100, 99, 99, 99, 99, 99, 100, 99, 99, 99, 99, 99, 99, 99, 100, 99, 99, 100, 99, 100, 100, 100, 100, 99, 100, 99, 100, 99, 100]
# users_traffic = [90, 137, 88, 97, 149, 100, 90, 84, 128, 105, 89, 133, 59, 71, 73, 105, 74, 51, 70, 90, 122, 131, 129, 133, 103, 131, 73, 106, 64, 118, 109, 140, 91, 121, 148, 80, 110, 98, 58, 115, 92, 87, 106, 59, 145, 114, 102, 141, 63, 92, 96, 95, 117, 111, 120, 53, 59, 122, 87, 99, 71, 70, 52, 148, 95, 54, 125, 90, 117, 89, 112, 85, 144, 145, 65, 83, 78, 148, 113, 150, 50, 114, 50, 74, 89, 88, 125, 110, 118, 59, 53, 123, 114, 71, 85, 69, 87, 108, 82, 76]
# users_traffic = [50, 98, 148, 16, 170, 111, 118, 63, 179, 136, 37, 109, 0, 119, 90, 198, 151, 172, 66, 31, 77, 17, 89, 41, 109, 186, 50, 174, 52, 180, 78, 192, 193, 20, 167, 87, 166, 191, 119, 179, 76, 198, 113, 168, 137, 74, 0, 59, 118, 101, 52, 173, 28, 118, 190, 162, 66, 148, 82, 17, 98, 138, 197, 103, 104, 200, 43, 168, 54, 139, 123, 191, 102, 10, 155, 12, 139, 56, 120, 119, 7, 38, 154, 62, 85, 183, 90, 158, 33, 105, 192, 83, 25, 120, 154, 83, 170, 67, 196, 14]
# users_traffic = [462, 595, 337, 396, 316, 682, 672, 374, 452, 569, 391, 656, 613, 528, 573, 300, 453, 575, 419, 582, 620, 546, 613, 520, 456, 623, 700, 672, 613, 382, 391, 338, 601, 665, 579, 570, 523, 626, 655, 656, 451, 515, 586, 509, 526, 581, 447, 502, 548, 657, 354, 659, 659, 560, 499, 423, 345, 499, 434, 359, 587, 466, 306, 676, 620, 439, 364, 307, 490, 696, 520, 413, 458, 348, 509, 370, 425, 348, 524, 556, 407, 404, 468, 636, 321, 366, 349, 685, 564, 310, 414, 353, 509, 416, 521, 522, 624, 326, 483, 449]
# users_traffic = [654, 125, 635, 588, 617, 575, 167, 676, 151, 370, 782, 749, 502, 229, 406, 213, 275, 688, 110, 573, 483, 280, 254, 621, 549, 592, 270, 216, 121, 683, 289, 282, 420, 252, 600, 742, 768, 118, 454, 280, 392, 222, 399, 524, 343, 742, 775, 638, 164, 367, 759, 840, 504, 591, 259, 892, 871, 377, 223, 858, 173, 834, 542, 451, 311, 772, 836, 139, 522, 593, 385, 478, 531, 788, 231, 429, 802, 126, 619, 784, 797, 761, 489, 554, 755, 392, 318, 211, 508, 226, 672, 835, 199, 190, 452, 147, 867, 157, 329, 499]
users_traffic = [139, 150, 187, 140, 142, 173, 127, 145, 200, 195, 104, 161, 123, 147, 199, 162, 129, 200, 194, 138, 112, 185, 121, 136, 121, 200, 150, 130, 152, 185, 768, 742, 727, 799, 778, 751, 706, 795, 784, 706, 740, 750, 774, 777, 719, 719, 794, 800, 797, 712, 712, 756, 747, 737, 770, 764, 723, 727, 790, 708, 423, 402, 486, 402, 446, 418, 490, 417, 458, 433, 416, 476, 417, 472, 478, 411, 412, 411, 431, 450, 104, 126, 138, 139, 140, 133, 137, 120, 104, 143, 135, 118, 116, 105, 141, 131, 114, 103, 139, 102]

users_per_node_list_b = [
    {"node0": 3, "node1": 6, "node2": 4, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},           # 0
    {"node0": 8, "node1": 6, "node2": 7, "node3": 10, "node4": 6, "node5": 5, "node6": 5, "node7": 3, "node8": 3, "node9": 4},          # 1
    {"node0": 12, "node1": 10, "node2": 8, "node3": 9, "node4": 11, "node5": 8, "node6": 14, "node7": 9, "node8": 7, "node9": 10},      # 2
    {"node0": 12, "node1": 20, "node2": 15, "node3": 17, "node4": 23, "node5": 3, "node6": 5, "node7": 5, "node8": 0, "node9": 0},      # 3
    {"node0": 29, "node1": 27, "node2": 26, "node3": 2, "node4": 3, "node5": 3, "node6": 2, "node7": 3, "node8": 3, "node9": 2},        # 4
    {"node0": 3, "node1": 45, "node2": 35, "node3": 5, "node4": 2, "node5": 1, "node6": 2, "node7": 2, "node8": 3, "node9": 2},         # 5
    {"node0": 0, "node1": 61, "node2": 25, "node3": 6, "node4": 5, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 6
    {"node0": 0, "node1": 55, "node2": 36, "node3": 6, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 7
    {"node0": 0, "node1": 49, "node2": 51, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 8
    {"node0": 0, "node1": 83, "node2": 17, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 9
    {"node0": 0, "node1": 99, "node2": 1, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},          # 10
    {"node0": 0, "node1": 55, "node2": 42, "node3": 3, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 11
    {"node0": 3, "node1": 46, "node2": 36, "node3": 5, "node4": 2, "node5": 2, "node6": 0, "node7": 3, "node8": 3, "node9": 0},         # 12
    {"node0": 22, "node1": 30, "node2": 27, "node3": 2, "node4": 6, "node5": 3, "node6": 2, "node7": 3, "node8": 3, "node9": 2},        # 13
    {"node0": 0, "node1": 61, "node2": 25, "node3": 6, "node4": 5, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 14
    {"node0": 0, "node1": 57, "node2": 31, "node3": 9, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 15
    {"node0": 29, "node1": 27, "node2": 26, "node3": 2, "node4": 3, "node5": 3, "node6": 2, "node7": 3, "node8": 3, "node9": 2},        # 16
    {"node0": 10, "node1": 24, "node2": 15, "node3": 19, "node4": 19, "node5": 3, "node6": 5, "node7": 5, "node8": 0, "node9": 0},      # 17
    {"node0": 12, "node1": 10, "node2": 11, "node3": 9, "node4": 11, "node5": 6, "node6": 16, "node7": 9, "node8": 7, "node9": 9},      # 18
    {"node0": 10, "node1": 10, "node2": 10, "node3": 10, "node4": 10, "node5": 10, "node6": 10, "node7": 10, "node8": 12, "node9": 8},  # 19
]

users_per_node_list = [
    {"node0": 3, "node1": 6, "node2": 4, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},           # 0
    {"node0": 8, "node1": 6, "node2": 7, "node3": 10, "node4": 6, "node5": 5, "node6": 5, "node7": 3, "node8": 3, "node9": 4},          # 1
    {"node0": 12, "node1": 10, "node2": 8, "node3": 9, "node4": 11, "node5": 8, "node6": 14, "node7": 9, "node8": 7, "node9": 10},      # 2
    {"node0": 12, "node1": 14, "node2": 10, "node3": 11, "node4": 11, "node5": 8, "node6": 14, "node7": 7, "node8": 5, "node9": 6},     # 3
    {"node0": 12, "node1": 20, "node2": 15, "node3": 17, "node4": 15, "node5": 3, "node6": 5, "node7": 5, "node8": 5, "node9": 3},      # 4
    {"node0": 12, "node1": 20, "node2": 15, "node3": 17, "node4": 23, "node5": 3, "node6": 5, "node7": 5, "node8": 0, "node9": 0},      # 5     - 0.50
    {"node0": 29, "node1": 27, "node2": 26, "node3": 2, "node4": 3, "node5": 3, "node6": 2, "node7": 3, "node8": 3, "node9": 2},        # 6
    {"node0": 0, "node1": 55, "node2": 36, "node3": 6, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 7
    {"node0": 0, "node1": 49, "node2": 51, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 8
    {"node0": 0, "node1": 55, "node2": 42, "node3": 3, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 9
    {"node0": 3, "node1": 46, "node2": 36, "node3": 5, "node4": 2, "node5": 2, "node6": 0, "node7": 3, "node8": 3, "node9": 0},         # 10
    {"node0": 22, "node1": 30, "node2": 27, "node3": 2, "node4": 6, "node5": 3, "node6": 2, "node7": 3, "node8": 3, "node9": 2},        # 11
    {"node0": 29, "node1": 27, "node2": 26, "node3": 7, "node4": 3, "node5": 3, "node6": 2, "node7": 3, "node8": 0, "node9": 0},        # 12
    {"node0": 24, "node1": 27, "node2": 24, "node3": 9, "node4": 6, "node5": 5, "node6": 2, "node7": 3, "node8": 0, "node9": 0},        # 13
    {"node0": 0, "node1": 61, "node2": 25, "node3": 6, "node4": 5, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 14
    {"node0": 0, "node1": 57, "node2": 31, "node3": 9, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 3, "node9": 0},         # 15
    {"node0": 29, "node1": 27, "node2": 26, "node3": 2, "node4": 3, "node5": 3, "node6": 2, "node7": 3, "node8": 3, "node9": 2},        # 16
    {"node0": 10, "node1": 24, "node2": 15, "node3": 19, "node4": 19, "node5": 3, "node6": 5, "node7": 5, "node8": 0, "node9": 0},      # 17
    {"node0": 12, "node1": 10, "node2": 11, "node3": 9, "node4": 11, "node5": 6, "node6": 16, "node7": 9, "node8": 7, "node9": 9},      # 18
    {"node0": 10, "node1": 10, "node2": 10, "node3": 10, "node4": 10, "node5": 10, "node6": 10, "node7": 10, "node8": 12, "node9": 8},  # 19
]

stat_filename = "config/use_case_stat/cache_statistics.json"
init_stats = {'misses-ratio': {'node0': 1, 'node1': 1, 'node2': 1, 'node3': 1, 'node4': 1, 'node5': 1, 'node6': 1, 'node7': 1, 'node8': 1, 'node9': 1}, 'max-caches': 31500, 'current-caches-storage': {'node0': 0, 'node1': 0, 'node2': 0, 'node3': 0, 'node4': 0, 'node5': 0, 'node6': 0, 'node7': 0, 'node8': 0, 'node9': 0}, 'traffic': {'node0': 3, 'node1': 6, 'node2': 4, 'node3': 0, 'node4': 0, 'node5': 0, 'node6': 0, 'node7': 0, 'node8': 0, 'node9': 0}}

with open(stat_filename, "w") as f:
    f.write(json.dumps(init_stats, indent=4))

USER_AVG_STORAGE = 256

current_distribution = 0
stationary_iteration = 0
sdo_name = "sdo0"
service_bundle = ["s7"]*35
print(sdo_name + " : " + str(service_bundle))
placement_filename = configuration.RESULTS_FOLDER + "/placement_" + sdo_name + ".json"

try:
    os.remove(placement_filename)
except OSError as err:
    pass


while current_distribution < len(users_per_node_list):
    with open(stat_filename, "r") as stat_file:
        stats = json.loads(stat_file.read())

    try:
        with open(placement_filename, "r") as placement_file:
            placement = json.loads(placement_file.read())
    except:
        placement = []

    tot_users = 0
    normal_miss = random.randint(3, 7)/100
    for i, node in enumerate(rap.nodes):
        tot_cache = stats["current-caches-storage"][node]
        n_users = users_per_node_list[current_distribution][node]
        if n_users == 0:
            misses_ratio = 0
            stats["traffic"][node] = 0
        else:
            # misses_ratio = max(1 - tot_cache/(n_users*USER_AVG_STORAGE), normal_miss)
            misses_ratio = max(1 - tot_cache / (users_range_size(users_data, tot_users, tot_users+n_users)), normal_miss)
            stats["traffic"][node] = users_range_size(users_traffic, tot_users, tot_users + n_users)
            tot_users += n_users
        stats["misses-ratio"][node] = round(misses_ratio, 2)

    with open(stat_filename, "w") as f:
        f.write(json.dumps(stats, indent=4))

    gini = gini_index(users_per_node_list[current_distribution])
    herfindahl = herfindahl_index(users_per_node_list[current_distribution])
    if stationary_iteration > 0 or current_distribution == 0:
        variation = 0
    else:
        variation = variation_index(users_per_node_list[current_distribution-1], users_per_node_list[current_distribution])

    print("Distribution: " + str(current_distribution).rjust(2) +
          " | Stationary: " + str(stationary_iteration) +
          " | Gini Index: " + str(gini)[:4].ljust(4) +
          " | Herfindahl Index: " + str(round(herfindahl)).rjust(5) +
          " | variation: " + str(variation)[:4].ljust(4) +
          " | Cache misses: " + str(sum([stats["misses-ratio"][n]*users_per_node_list[current_distribution][n] for n in stats["misses-ratio"]])/sum([users_per_node_list[current_distribution][n] for n in stats["misses-ratio"]]))[:4].ljust(4) +
          " | Suffering nodes: " + str(len([n for n in stats["misses-ratio"] if stats["misses-ratio"][n] > 0.07])) +
          " | Allocated: " + str(sum(stats["current-caches-storage"].values())).rjust(5) +
          " |             Placement: " + str(stats["current-caches-storage"]).ljust(155) +
          " |             Misses: " + str(stats["misses-ratio"]).ljust(155) +
          " |             Empty slots: " + str(len([e for e in placement if e[1] == 'f11'])) + " | f10: " + str(len([e for e in placement if e[1] == 'f10'])) + " | f9: " + str(len([e for e in placement if e[1] == 'f9'])) +
          " |")

    caches_ok = len([n for n in stats["misses-ratio"] if stats["misses-ratio"][n] > 0.07]) == 0

    if stationary_iteration < stationary_iterations[current_distribution]:
        stationary_iteration += 1
    else:
        stationary_iteration = 0
        current_distribution += 1

    # if current_distribution == 3 and stationary_iteration == 2:
    #     print("debug")

    if not caches_ok:
        p = None
        try:
            log_file = "sdo0_" + str(current_distribution) + "-" + str(stationary_iteration) + ".log"
            p = subprocess.Popen(["python3", "main.py", sdo_name] + service_bundle + ["-l", configuration.LOG_LEVEL, "-d", CONF_FILE, "-a", "CDN", "-o", "-z", "-f", log_file])
            #p = subprocess.Popen(["python3", "main_skipagreement.py", sdo_name] + service_bundle + ["-l", configuration.LOG_LEVEL, "-d", CONF_FILE, "-o"])
            p.wait()
        except TimeoutExpired:
            p.kill()

with open(stat_filename, "w") as f:
    f.write(json.dumps(init_stats, indent=4))

try:
    os.remove("sdo_instances/sdo0_frozen")
except OSError as err:
    pass

try:
    os.remove(placement_filename)
except OSError as err:
    pass
