import ast
from collections import OrderedDict

import itertools

filename = "sdos__FIXEDneighbor_prob__4nodes.txt"
c_filename = "sdos__FIXEDneighbor_prob__4nodes_CENTRALIZED.txt"
folder = "validation"
performance_output = folder + "/stat.dat"
cases_performance_output = folder + "/centralized_stat.dat"
messages_output = folder + "/messages.dat"

bundle_percentages = {}

last_updates = {}
demand_percentages = {}
assigned_percentages = {}
allocated_percentages = {}
sent_messages_l = {}
received_messages_l = {}
sum_private_utilities_l = {}

tot_avg_last_updates = {}
tot_avg_demand_percentages = {}
tot_avg_assigned_percentages = {}
tot_avg_allocated_percentages = {}
tot_avg_sent_messages = {}
tot_avg_received_messages = {}
tot_avg_sum_private_utilities = {}

avg_last_updates = {}
avg_demand_percentages = {}
avg_assigned_percentages = {}
avg_allocated_percentages = {}
avg_sent_messages = {}
avg_received_messages = {}
avg_sum_private_utilities = {}

time_rates_l = {}
messages_sample_l = {}

c_assigned_percentages = {}
c_allocated_percentages = {}
c_sum_private_utilities = {}

CENTRALIZED_CASES = ["SERVICE", "POWER-CONSUMPTION", "GREEDY", "LOAD-BALANCE", "NODE-LOADING", "BEST-FIT-POLICY"]

FIRST = 3
LAST = 21

CASES = 1
SAMPLES = 10

for i in range(FIRST, LAST):
    filename_i = folder + "/" + str(i) + filename
    c_filename_i = folder + "/" + str(i) + c_filename
    with open(filename_i, "r") as f:
        data = f.read()
    with open(c_filename_i, "r") as f:
        c_data = f.read()

    s = str(data)
    last_updates[i] = {}
    demand_percentages[i] = {}
    assigned_percentages[i] = {}
    allocated_percentages[i] = {}
    sent_messages_l[i] = {}
    received_messages_l[i] = {}
    sum_private_utilities_l[i] = {}

    avg_last_updates[i] = {}
    avg_demand_percentages[i] = {}
    avg_assigned_percentages[i] = {}
    avg_allocated_percentages[i] = {}
    avg_sent_messages[i] = {}
    avg_received_messages[i] = {}
    avg_sum_private_utilities[i] = {}

    c_assigned_percentages[i] = {}
    c_allocated_percentages[i] = {}
    c_sum_private_utilities[i] = {}

    incompleted = False
    for j in range(CASES):

        # read bundle percentage length
        p = s[s.find("BUNDLE_PERCENTAGE_LENGTH:") + len("BUNDLE_PERCENTAGE_LENGTH:"):s.find("BUNDLE_PERCENTAGE_LENGTH:") + len("BUNDLE_PERCENTAGE_LENGTH:") + s[s.find("BUNDLE_PERCENTAGE_LENGTH:") + len("BUNDLE_PERCENTAGE_LENGTH:"):].index("\n")]
        bundle_percentages[j] = int(p)

        demand_percentages[i][j] = 0
        assigned_percentages[i][j] = []
        allocated_percentages[i][j] = []
        sum_private_utilities_l[i][j] = []

        for k in range(SAMPLES):

            last_update = 0
            sent_messages = 0
            received_messages = 0
            for sdo in range(i):
                try:
                    # read last update time
                    t = s[s.find("last update on:") + len("last update on:"):s.find("last update on:") + len("last update on:") + 7]
                    s = s[s.find("last update on:") + len("last update on:") + 7:]
                    if float(t) > last_update:
                        last_update = float(t)

                    # read sent messages number
                    m = s[s.find("sent messages:") + len("sent messages:"):s.find("sent messages:") + len("sent messages:") + 9]
                    s = s[s.find("sent messages:") + len("sent messages:") + 9:]
                    sent_messages += int(m)

                    # read handled messages number
                    m = s[s.find("received messages:") + len("received messages:"):s.find("received messages:") + len("received messages:") + 9]
                    s = s[s.find("received messages:") + len("received messages:") + 9:]
                    received_messages += int(m)

                except ValueError:
                    incompleted = True
                    break

            # collect data for this sample
            # statistical total demand percentage
            p = s[s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:"):s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:") + s[s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:"):].index("\n")]
            demand_percentages[i][j] = round(float(p), 3)
            # sum of private utilities:
            p = s[s.find("Sum of private utilities:") + len("Sum of private utilities:"):s.find("Sum of private utilities:") + len("Sum of private utilities:") + s[s.find("Sum of private utilities:") + len("Sum of private utilities:"):].index("\n")]
            sum_private_utilities_l[i][j].append(int(p))
            # percentage of assigned resources
            p = s[s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:"):s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:") + s[s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:"):].index("\n")]
            assigned_percentages[i][j].append(round(float(p), 3))
            # percentage of successfully allocated bundles
            p = s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:") + s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):].index("\n")]
            allocated_percentages[i][j].append(round(float(p), 3))

        # calculate per case average data
        avg_demand_percentages[i][j] = demand_percentages[i][j]
        avg_assigned_percentages[i][j] = round(sum(assigned_percentages[i][j]) / SAMPLES, 3)
        avg_allocated_percentages[i][j] = round(sum(allocated_percentages[i][j]) / SAMPLES, 3)
        avg_sum_private_utilities[i][j] = round(sum(sum_private_utilities_l[i][j]) / SAMPLES, 1)

    # collect centralized performances
    for case in CENTRALIZED_CASES:
        s = str(c_data)
        m = s[s.find("sent messages:") + len("sent messages:"):s.find("sent messages:") + len("sent messages:") + 9]
        s = s[s.find("UTILITY: " + case) + len("UTILITY: " + case):]
        # sum of private utilities:
        p = s[s.find("Sum of service utilities:") + len("Sum of service utilities:"):s.find("Sum of service utilities:") + len("Sum of service utilities:") + s[s.find("Sum of service utilities:") + len("Sum of service utilities:"):].index("\n")]
        c_sum_private_utilities[i][case] = int(p)
        # percentage of assigned resources
        p = s[s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:"):s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:") + s[s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:"):].index("\n")]
        c_assigned_percentages[i][case] = round(float(p), 3)
        # percentage of successfully allocated bundles
        p = s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:") + s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):].index("\n")]
        c_allocated_percentages[i][case] = round(float(p), 3)

# save per case data on dat file
with open(cases_performance_output, "w") as f:
    for j in range(CASES):
        f.write("#" + " bundle percentage length: \t" + str(bundle_percentages[j]) + "\n")
        f.write("#" + "\t" + "\t" + "avg d%" + "\t" + "avg a%" + "\t" + "min a%" + "\t" + "max a%" + "\t" + "avg w%" + "\t" + "min w%" + "\t" + "max w%" + "\t" + "avg u" + "\t" + "min u" + "\t" + "max u" + "\n")
        for i in range(FIRST, LAST):
            # + "\t".join([str(x).ljust(5) for x in last_updates[i]])
            f.write(str(i) + "\t" + "\t" +
                    str(avg_demand_percentages[i][j]).ljust(5) + "\t" +
                    str(avg_assigned_percentages[i][j]).ljust(5) + "\t" +
                    str(min(assigned_percentages[i][j])).ljust(5) + "\t" +
                    str(max(assigned_percentages[i][j])).ljust(5) + "\t" +
                    str(avg_allocated_percentages[i][j]).ljust(5) + "\t" +
                    str(min(allocated_percentages[i][j])).ljust(5) + "\t" +
                    str(max(allocated_percentages[i][j])).ljust(5) + "\t" +
                    str(avg_sum_private_utilities[i][j]).ljust(5) + "\t" +
                    str(min(sum_private_utilities_l[i][j])).ljust(5) + "\t" +
                    str(max(sum_private_utilities_l[i][j])).ljust(5) + "\n")
        f.write("\n")
        f.write("\n")

    for case in CENTRALIZED_CASES:
        f.write("#" + " centralized case: \t" + case + "\n")
        f.write("#" + "\t" + "\t" + "avg d%" + "\t" + "avg a%" + "\t" + "min a%" + "\t" + "max a%" + "\t" + "avg w%" + "\t" + "min w%" + "\t" + "max w%" + "\t" + "avg u" + "\t" + "min u" + "\t" + "max u" + "\n")
        for i in range(FIRST, LAST):
            # + "\t".join([str(x).ljust(5) for x in last_updates[i]])
            f.write(str(i) + "\t" + "\t" +
                    str(avg_demand_percentages[i][0]).ljust(5) + "\t" +
                    str(c_assigned_percentages[i][case]).ljust(5) + "\t" +
                    str(c_assigned_percentages[i][case]).ljust(5) + "\t" +
                    str(c_assigned_percentages[i][case]).ljust(5) + "\t" +
                    str(c_allocated_percentages[i][case]).ljust(5) + "\t" +
                    str(c_allocated_percentages[i][case]).ljust(5) + "\t" +
                    str(c_allocated_percentages[i][case]).ljust(5) + "\t" +
                    str(c_sum_private_utilities[i][case]).ljust(5) + "\t" +
                    str(c_sum_private_utilities[i][case]).ljust(5) + "\t" +
                    str(c_sum_private_utilities[i][case]).ljust(5) + "\n")
        f.write("\n")
        f.write("\n")
