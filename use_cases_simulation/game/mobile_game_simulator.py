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


LATENCY_EXCL_THRESHOLD = 60
LATENCY_GOOD_THRESHOLD = 100
LATENCY_FAIR_THRESHOLD = 150
LATENCY_POOR_THRESHOLD = 250

MIGRATION_TIME = 2
DEELAY_TOLERANCE_INTERVAL = 3


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


def get_latency(topology, node_a, node_b):

    shortest_path = get_path(topology, node_a, node_b)

    latency = 0
    src = node_a
    for dst in shortest_path[1:]:
        latency += topology[src+':'+dst]
        src = dst
    return latency


def get_path(topology, node_a, node_b):

    if node_a == node_b:
        return []

    a_tree = dict()
    a_tree[node_a] = _get_tree_to_domain(topology, node_a, node_b, len(rap.nodes) - 2)
    a_paths = _get_path_list(a_tree, [])
    a_paths = [path for path in a_paths if path[-1] == node_b]

    if len(a_paths) == 0:
        return None
    shortest_path = a_paths[0]
    for path in a_paths:
        if len(path) < len(shortest_path):
            shortest_path = path
    return shortest_path


def _get_path_list(tree, prefix):
    paths = []
    for domain in tree:
        p = []
        p.extend(prefix)
        p.append(domain)
        if len(tree[domain]) > 0:
            paths.extend(_get_path_list(tree[domain], p))
        else:
            prefix.append(domain)
            paths.append(prefix)
    return paths


def _get_tree_to_domain(topology, root_node, leaf_node, deep):
    tree = {}
    for link in [l for l in topology if l.split(':')[0] == root_node]:
        if link.split(':')[1] != leaf_node and deep > 0:
            tree[link.split(':')[1]] = _get_tree_to_domain(topology, link.split(':')[1], leaf_node, deep-1)
        else:
            tree[link.split(':')[1]] = {}
    return tree


def compute_que(l):

    if l < LATENCY_EXCL_THRESHOLD:
        return "EXCELLENT"
    elif l < LATENCY_GOOD_THRESHOLD:
        return "GOOD"
    elif l < LATENCY_FAIR_THRESHOLD:
        return "FAIR"
    elif l < LATENCY_POOR_THRESHOLD:
        return "POOR"
    else:
        return "BAD"


def get_qoe(index):

    l = ['BAD', 'POOR', 'FAIR', 'GOOD', 'EXCELLENT']
    return l[index-1]


def get_qoe_index(qoe):

    l = ['BAD', 'POOR', 'FAIR', 'GOOD', 'EXCELLENT']
    return l.index(qoe) + 1


def quality_factor(f):

    if f == 'f3':
        return 1
    elif f == 'f9':
        return 2
    elif f == 'f10':
        return 5


def migrate(sdo_name, service_bundle):
    p = None
    try:
        log_file = "sdo0_" + str(current_distribution) + "-" + str(stationary_iteration) + ".log"
        p = subprocess.Popen(["python3", "main.py", sdo_name] + service_bundle + ["-l", configuration.LOG_LEVEL, "-d", CONF_FILE, "-a", "GAME", "-o", "-z", "-f", log_file])
        # p = subprocess.Popen(["python3", "main_skipagreement.py", sdo_name] + service_bundle + ["-l", configuration.LOG_LEVEL, "-d", CONF_FILE, "-a", "GAME", "-o"])
        p.wait()
    except TimeoutExpired:
        p.kill()


if __name__ == "__main__":

    CONF_FILE = 'config/game_config.ini'
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

    stationary_iterations_b = [2, 1, 2, 2, 1, 2, 1, 1, 1, 1, 2, 1, 3, 3, 2, 3, 2, 3, 1, 2]
    stationary_iterations = [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]

    # edge host nodes topology

    edge_topology = {
        "node0:node4": 50,
        "node1:node2": 50,
        "node1:node4": 50,
        "node3:node4": 50,
        "node4:node5": 50,
        "node4:node7": 50,
        "node5:node9": 50,
        "node6:node9": 50,
        "node8:node9": 50,
        "node4:node0": 50,
        "node2:node1": 50,
        "node4:node1": 50,
        "node4:node3": 50,
        "node5:node4": 50,
        "node7:node4": 50,
        "node9:node5": 50,
        "node9:node6": 50,
        "node9:node8": 50
    }

    game_time_intervals = [2, 6, 2, 9, 2, 5, 3, 7, 2, 7, 2, 6, 3, 9]

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

    users_per_node_list_a = [
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

    users_per_node_list = [
        {"node0": 1, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},           # 0
        {"node0": 0, "node1": 1, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},           # 1
        {"node0": 0, "node1": 0, "node2": 1, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},           # 2
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 1, "node6": 0, "node7": 0, "node8": 0, "node9": 0},     # 3
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 1, "node7": 0, "node8": 0, "node9": 0},      # 4
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 1},      # 5     - 0.50
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 1, "node9": 0},        # 6
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 1, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 7
        {"node0": 1, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 8
        {"node0": 0, "node1": 0, "node2": 0, "node3": 1, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 9
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 1, "node5": 0, "node6": 0, "node7": 0, "node8": 0, "node9": 0},         # 10
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 1, "node8": 0, "node9": 0},        # 11
        {"node0": 0, "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "node6": 0, "node7": 0, "node8": 1, "node9": 0}        # 12
    ]

    get_latency(edge_topology, "node0", "node1")

    stat_filename = "config/use_case_stat/game_statistics.json"
    init_stats = {'users': {'node0': 1, 'node1': 0, 'node2': 0, 'node3': 0, 'node4': 0, 'node5': 0, 'node6': 0, 'node7': 0, 'node8': 0, 'node9': 0}, 'max-copies': 4, 'current-copies': {'node0': 1, 'node1': 0, 'node2': 0, 'node3': 0, 'node4': 0, 'node5': 0, 'node6': 0, 'node7': 0, 'node8': 0, 'node9': 0}, 'traffic': {'node0': 3, 'node1': 6, 'node2': 4, 'node3': 0, 'node4': 0, 'node5': 0, 'node6': 0, 'node7': 0, 'node8': 0, 'node9': 0}, 'function': 'f10', 'topology': edge_topology, 'max-latency': 210}
    migrating_stats = dict(init_stats)

    with open(stat_filename, "w") as f:
        f.write(json.dumps(init_stats, indent=4))

    current_distribution = 0
    stationary_iteration = 0
    sdo_name = "sdo0"
    service_bundle = ["s7"]
    print(sdo_name + " : " + str(service_bundle))
    placement_filename = configuration.RESULTS_FOLDER + "/placement_" + sdo_name + ".json"
    migrating = 0
    last_r_qoe = "POOR"

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

        stats['users'] = users_per_node_list[current_distribution]

        base_latency = random.randint(30, 50)       # latency with the first-hop node
        user_node = [node for node in stats['users'] if stats['users'][node] == 1][0]
        game_node = [node for node in stats['current-copies'] if stats['current-copies'][node] == 1][0]
        if migrating > 0:
            game_node = [node for node in migrating_stats['current-copies'] if migrating_stats['current-copies'][node] == 1][0]
        latency = (base_latency + get_latency(edge_topology, user_node, game_node))*quality_factor(stats['function'])

        qoe = compute_que(latency)
        allow_migration = False

        # get current game status
        time_instant = sum(stationary_iterations[:current_distribution]) + len(stationary_iterations[:current_distribution]) + stationary_iteration + 1
        playing = True
        i = 0
        for i, delta_t in enumerate(game_time_intervals):
            playing = not playing
            if time_instant <= sum(game_time_intervals[:i + 1]):
                break
        # game status
        #print(str(time_instant) + "\t " + str(current_distribution) + "." + str(stationary_iteration) + "\t " + str(playing))

        #if current_distribution == 8 and stationary_iteration == 4:
        #    print("debug")

        if migrating == 0 and latency > LATENCY_GOOD_THRESHOLD:

            allow_migration = True
            '''
            if not playing:

                idle_delta_t = 1 + sum(game_time_intervals[:i+1]) - time_instant
                extra_time = idle_delta_t - MIGRATION_TIME

                if extra_time >= 0:
                    allow_migration = True
                elif idle_delta_t > MIGRATION_TIME/2 and latency > LATENCY_FAIR_THRESHOLD:
                    allow_migration = True
                elif latency > LATENCY_POOR_THRESHOLD:
                    allow_migration = True
            elif latency > LATENCY_POOR_THRESHOLD:
                allow_migration = True
            elif latency > LATENCY_FAIR_THRESHOLD:
                if sum(game_time_intervals[:i+1]) - time_instant > DEELAY_TOLERANCE_INTERVAL:
                    allow_migration = True
                elif i < len(game_time_intervals) - 1 and game_time_intervals[i+1] < MIGRATION_TIME:
                    allow_migration = True
            '''
        if allow_migration:
            migrating = MIGRATION_TIME
            migrating_stats = dict(stats)

        if not allow_migration and migrating:
            qoe = "MIGRATING"

        r_qoe = qoe
        if not playing:
            if last_r_qoe == "GOOD":
                r_qoe = "GOOD"
            elif last_r_qoe == "EXCELLENT":
                r_qoe = "EXCELLENT"
            else:
                improved = get_qoe_index(last_r_qoe) + 1
                r_qoe = get_qoe(improved)
        elif qoe == "MIGRATING":
            r_qoe = "BAD"

        last_r_qoe = r_qoe

        print("Distribution: " + str(current_distribution).rjust(2) +
              " | Stationary: " + str(stationary_iteration) +
              " | Time: " + str(time_instant).rjust(2) +
              " | Playing: " + str(playing)[:5].ljust(5) +
              " | User: " + user_node +
              " | Game: " + game_node +
              " | Latency: " + str(latency).rjust(4) + " ms" +
              " | Network QoE: " + str(qoe).ljust(9) +
              " | Perceived QoE: " + str(r_qoe).ljust(9) +
              " | Start migration: " + str(allow_migration).ljust(5) +
              " | Migrating: " + str(migrating) +
              " |")

        '''
    
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
        '''

        with open(stat_filename, "w") as f:
            f.write(json.dumps(stats, indent=4))

        '''
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
        '''

        if stationary_iteration < stationary_iterations[current_distribution]:
            stationary_iteration += 1
        else:
            stationary_iteration = 0
            current_distribution += 1

        # if current_distribution == 3 and stationary_iteration == 2:
        #     print("debug")

        if allow_migration:
            migrate(sdo_name, service_bundle)
        if migrating > 0:
            migrating -= 1

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
