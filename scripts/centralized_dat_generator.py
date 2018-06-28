import ast
from collections import OrderedDict

import itertools

filename = "sdos__FIXEDneighbor_prob__4nodes.txt"
folder = "validation"
performance_output = folder + "/stat.dat"
cases_performance_output = folder + "/cases_stat.dat"
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

FIRST = 3
LAST = 21

CASES = 5
SAMPLES = 5

for i in range(FIRST, LAST):
    filename_i = folder + "/" + str(i) + filename
    with open(filename_i, "r") as f:
        data = f.read()

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

    incompleted = False
    for j in range(CASES):

        # read bundle percentage length
        p = s[s.find("BUNDLE_PERCENTAGE_LENGTH:") + len("BUNDLE_PERCENTAGE_LENGTH:"):s.find("BUNDLE_PERCENTAGE_LENGTH:") + len("BUNDLE_PERCENTAGE_LENGTH:") + s[s.find("BUNDLE_PERCENTAGE_LENGTH:") + len("BUNDLE_PERCENTAGE_LENGTH:"):].index("\n")]
        bundle_percentages[j] = int(p)

        last_updates[i][j] = []
        demand_percentages[i][j] = []
        assigned_percentages[i][j] = []
        allocated_percentages[i][j] = []
        sent_messages_l[i][j] = []
        received_messages_l[i][j] = []
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
            # last update time
            last_updates[i][j].append(last_update)
            # statistical total demand percentage
            p = s[s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:"):s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:") + s[s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:"):].index("\n")]
            demand_percentages[i][j].append(round(float(p), 3))
            # sum of private utilities:
            p = s[s.find("Sum of private utilities:") + len("Sum of private utilities:"):s.find("Sum of private utilities:") + len("Sum of private utilities:") + s[s.find("Sum of private utilities:") + len("Sum of private utilities:"):].index("\n")]
            sum_private_utilities_l[i][j].append(int(p))
            # percentage of assigned resources
            p = s[s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:"):s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:") + s[s.find("Percentage of assigned resources:") + len("Percentage of assigned resources:"):].index("\n")]
            assigned_percentages[i][j].append(round(float(p), 3))
            # percentage of successfully allocated bundles
            p = s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:") + s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):].index("\n")]
            allocated_percentages[i][j].append(round(float(p), 3))
            # messages number
            sent_messages_l[i][j].append(sent_messages)
            received_messages_l[i][j].append(received_messages)
            # message rates
            '''
            wanted_case = int(CASES/2)
            if i not in time_rates_l.keys() and j == wanted_case:
                d = s[s.find("OrderedDict") + len("OrderedDict"):s.find(")])") + len(")])")]
                time_rates = OrderedDict(ast.literal_eval(d))
                # skip if strange zero rate inside but keep if is last sample
                if 0.0 not in list(time_rates.values())[1:] or k == SAMPLES-1:
                    time_rates_l[i] = time_rates
                    messages_sample_l[i] = sent_messages
            '''

        # calculate per case average data
        avg_last_updates[i][j] = round(sum(last_updates[i][j]) / SAMPLES, 3)
        avg_demand_percentages[i][j] = round(sum(demand_percentages[i][j]) / SAMPLES, 3)
        avg_assigned_percentages[i][j] = round(sum(assigned_percentages[i][j]) / SAMPLES, 3)
        avg_allocated_percentages[i][j] = round(sum(allocated_percentages[i][j]) / SAMPLES, 3)
        avg_sent_messages[i][j] = round(sum(sent_messages_l[i][j]) / SAMPLES, 3)
        avg_received_messages[i][j] = round(sum(received_messages_l[i][j]) / SAMPLES, 3)
        avg_sum_private_utilities[i][j] = round(sum(sum_private_utilities_l[i][j]) / SAMPLES, 1)

    # calculate total average data
    tot_avg_last_updates[i] = round(sum(list(itertools.chain(*last_updates[i].values()))) / (CASES*SAMPLES), 3)
    tot_avg_demand_percentages[i] = round(sum(list(itertools.chain(*demand_percentages[i].values()))) / (CASES*SAMPLES), 3)
    tot_avg_assigned_percentages[i] = round(sum(list(itertools.chain(*assigned_percentages[i].values()))) / (CASES*SAMPLES), 3)
    tot_avg_allocated_percentages[i] = round(sum(list(itertools.chain(*allocated_percentages[i].values()))) / (CASES*SAMPLES), 3)
    tot_avg_sent_messages[i] = round(sum(list(itertools.chain(*sent_messages_l[i].values()))) / (CASES*SAMPLES), 3)
    tot_avg_received_messages[i] = round(sum(list(itertools.chain(*received_messages_l[i].values()))) / (CASES*SAMPLES), 3)
    tot_avg_sum_private_utilities[i] = round(sum(list(itertools.chain(*sum_private_utilities_l[i].values()))) / (CASES*SAMPLES), 1)

# save average data on dat file
with open(performance_output, "w") as f:
    # + "\t\t".join(["t n." + str(i+1) for i in range(SAMPLES)])
    f.write("#" + "\t" + "\t" + "t avg" + "\t" + "t min" + "\t" + "t max" + "\t" + "avg d%" + "\t" + "min d%" + "\t" + "max d%" + "\t" + "avg a%" + "\t" + "min a%" + "\t" + "max a%" + "\t" + "avg w%" + "\t" + "min w%" + "\t" + "max w%" + "\t" + "avg msg" + "\t" + "min msg" + "\t" + "max msg" + "\t" + "avg u" + "\t" + "min u" + "\t" + "max u" + "\n")
    for i in range(FIRST, LAST):
        # + "\t".join([str(x).ljust(5) for x in last_updates[i]])
        f.write(str(i) + "\t" + "\t" + str(tot_avg_last_updates[i]).ljust(5) + "\t" +
                str(min(list(itertools.chain(*last_updates[i].values())))).ljust(5) + "\t" +
                str(max(list(itertools.chain(*last_updates[i].values())))).ljust(5) + "\t" +
                str(tot_avg_demand_percentages[i]).ljust(5) + "\t" +
                str(min(list(itertools.chain(*demand_percentages[i].values())))).ljust(5) + "\t" +
                str(max(list(itertools.chain(*demand_percentages[i].values())))).ljust(5) + "\t" +
                str(tot_avg_assigned_percentages[i]).ljust(5) + "\t" +
                str(min(list(itertools.chain(*assigned_percentages[i].values())))).ljust(5) + "\t" +
                str(max(list(itertools.chain(*assigned_percentages[i].values())))).ljust(5) + "\t" +
                str(tot_avg_allocated_percentages[i]).ljust(5) + "\t" +
                str(min(list(itertools.chain(*allocated_percentages[i].values())))).ljust(5) + "\t" +
                str(max(list(itertools.chain(*allocated_percentages[i].values())))).ljust(5) + "\t" +
                str(tot_avg_sent_messages[i]).ljust(5) + "\t" +
                str(min(list(itertools.chain(*sent_messages_l[i].values())))).ljust(5) + "\t" +
                str(max(list(itertools.chain(*sent_messages_l[i].values())))).ljust(5) + "\t" +
                str(tot_avg_sum_private_utilities[i]).ljust(5) + "\t" +
                str(min(list(itertools.chain(*sum_private_utilities_l[i].values())))).ljust(5) + "\t" +
                str(max(list(itertools.chain(*sum_private_utilities_l[i].values())))).ljust(5) + "\n")

# save per case data on dat file
with open(cases_performance_output, "w") as f:
    for j in range(CASES):
        f.write("#" + " bundle percentage length: \t" + str(bundle_percentages[j]) + "\n")
        f.write("#" + "\t" + "\t" + "t avg" + "\t" + "t min" + "\t" + "t max" + "\t" + "avg d%" + "\t" + "min d%" + "\t" + "max d%" + "\t" + "avg a%" + "\t" + "min a%" + "\t" + "max a%" + "\t" + "avg w%" + "\t" + "min w%" + "\t" + "max w%" + "\t" + "avg msg" + "\t" + "min msg" + "\t" + "max msg" + "\t" + "avg u" + "\t" + "min u" + "\t" + "max u" + "\n")
        for i in range(FIRST, LAST):
            # + "\t".join([str(x).ljust(5) for x in last_updates[i]])
            f.write(str(i) + "\t" + "\t" +
                    str(avg_last_updates[i][j]).ljust(5) + "\t" +
                    str(min(last_updates[i][j])).ljust(5) + "\t" +
                    str(max(last_updates[i][j])).ljust(5) + "\t" +
                    str(avg_demand_percentages[i][j]).ljust(5) + "\t" +
                    str(min(demand_percentages[i][j])).ljust(5) + "\t" +
                    str(max(demand_percentages[i][j])).ljust(5) + "\t" +
                    str(avg_assigned_percentages[i][j]).ljust(5) + "\t" +
                    str(min(assigned_percentages[i][j])).ljust(5) + "\t" +
                    str(max(assigned_percentages[i][j])).ljust(5) + "\t" +
                    str(avg_allocated_percentages[i][j]).ljust(5) + "\t" +
                    str(min(allocated_percentages[i][j])).ljust(5) + "\t" +
                    str(max(allocated_percentages[i][j])).ljust(5) + "\t" +
                    str(avg_sent_messages[i][j]).ljust(5) + "\t" +
                    str(min(sent_messages_l[i][j])).ljust(5) + "\t" +
                    str(max(sent_messages_l[i][j])).ljust(5) + "\t" +
                    str(avg_sum_private_utilities[i][j]).ljust(5) + "\t" +
                    str(min(sum_private_utilities_l[i][j])).ljust(5) + "\t" +
                    str(max(sum_private_utilities_l[i][j])).ljust(5) + "\n")
        f.write("\n")
        f.write("\n")

'''
# save message rates on dat file
with open(messages_output, "w") as f:
    for i in range(FIRST, LAST):
        f.write(str(i) + "\n")
        f.write("\n")
        for sample in time_rates_l[i]:
            f.write(str(sample) + "\t\t" + str(time_rates_l[i][sample]) + "\n")
        f.write("\n")
        f.write(str(messages_sample_l[i]) + "\n")
        f.write("\n")
        f.write(" --------------- \n")
        f.write("\n")
'''
