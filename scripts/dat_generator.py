filename = "sdos__20neighbor_prob__4nodes.txt"
folder = "validation"
output = folder + "/stat.dat"

last_updates = {}
demand_percentage = {}
allocated_percentage = {}

for i in range(3, 21):
    filename_i = folder + "/" + str(i) + filename
    with open(filename_i, "r") as f:
        data = f.read()

    s = str(data)
    last_updates[i] = []
    demand_percentage_l = []
    allocated_percentage_l = []
    for j in range(5):

        last_update = 0
        for k in range(i):
            t = s[s.find("last update on:") + len("last update on:"):s.find("last update on:") + len("last update on:") + 7]
            s = s[s.find("last update on:") + len("last update on:") + 7:]
            if float(t) > last_update:
                last_update = float(t)
        last_updates[i].append(last_update)

        p = s[s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:"):s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:") + s[s.find("Statistical total demand percentage:") + len("Statistical total demand percentage:"):].index("\n")]
        demand_percentage_l.append(float(p))
        p = s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:") + s[s.find("Percentage of successfully allocated bundles:") + len("Percentage of successfully allocated bundles:"):].index("\n")]
        allocated_percentage_l.append(float(p))
    demand_percentage[i] = round(sum(demand_percentage_l)/5, 3)
    allocated_percentage[i] = round(sum(allocated_percentage_l)/5, 3)

with open(output, "w") as f:
    for i in range(3, 21):
        f.write(str(i) + "\t" + "\t".join([str(x).ljust(5) for x in last_updates[i]]) + "\t" + str(min(last_updates[i])).ljust(5) + "\t" + str(max(last_updates[i])).ljust(5) + "\t" + str(demand_percentage[i]).ljust(5) + "\t" + str(allocated_percentage[i]).ljust(5) + "\n")

