# import pika
import json
import time
import sys
import signal
import urllib.request

# from pyrabbit.api import Client
# import urllib.request
from urllib.error import HTTPError

from config.configuration import Configuration

'''
def get_message_number(queues):
    counter = 0

    for sdo in queues:
        q = channel.queue_declare(queue=sdo, passive=True, exclusive=False, auto_delete=False)
        counter += q.method.message_count

    return counter
'''
'''
def get_message_number_pyrabbit(queues):
    counter = 0

    for sdo in queues:
        queue_count = cl.get_messages('/', sdo)[0]['message_count']
        counter += queue_count

    return counter
'''


def get_message_number(queues):

    total = 0.0
    res = urllib.request.urlopen("http://localhost:15672/api/queues/%2f/").read()
    res_dict = json.loads(res)
    for queue in [e for e in res_dict if e['name'] in queues]:
        total += queue['message_stats']['deliver_get']
    return total


def get_message_rate(queues):

    total = 0.0
    res = urllib.request.urlopen("http://localhost:15672/api/queues/%2f/").read()
    res_dict = json.loads(res)
    for queue in [e for e in res_dict if e['name'] in queues]:
        total += queue['message_stats']['publish_details']['rate']
    return total/len(queues)


'''
def signal_handler(signal, frame):
    # connection.close()

    with open(file_name, "a+") as output_file:
        output_file.write("\n--------------------------\n")
        output_file.write("BUNDLE_PERCENTAGE = " + str(bundle_percentage) + "\n")
        output_file.write("AGREEMENT_TIMEOUT = " + str(agreement_timeout) + "\n")
        output_file.write("Messages: \n")
        for ts in sorted(counters):
            output_file.write(str(ts)[:5] + ": " + str(counters[ts]) + " \n")
        output_file.write("Total messages: " + str(sum(counters.values())) + "\n")
        output_file.write("\n--------------------------\n")

    sys.exit(0)
'''


if __name__ == "__main__":

    # signal.signal(signal.SIGINT, signal_handler)
    file_name = "validation/" + str(Configuration.SDO_NUMBER) + "sdos__" + str(Configuration.NEIGHBOR_PROBABILITY) + "neighbor_prob__" + str(Configuration.NODE_NUMBER) + "nodes__ts.txt"
    bundle_percentage = Configuration.BUNDLE_PERCENTAGE
    agreement_timeout = Configuration.AGREEMENT_TIMEOUT

    # create a password manager
    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()

    # Add the username and password.
    top_level_url = "http://localhost:15672/api/queues/%2f/"
    password_mgr.add_password(None, top_level_url, "guest", "guest")

    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)

    # create "opener" (OpenerDirector instance)
    opener = urllib.request.build_opener(handler)

    # Install the opener.
    # Now all calls to urllib.request.urlopen use our opener.
    urllib.request.install_opener(opener)

    # connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # channel = connection.channel()

    # cl = Client('localhost:55672', 'guest', 'guest')

    sdos = ["sdo" + str(n) for n in range(Configuration.SDO_NUMBER)]

    begin = False
    begining_time = time.time()
    counters = dict()
    counters[0.0] = 0
    last_count = 0
    static_counter = 0
    rate = 0

    while True:
        try:
            # count = get_message_number(sdos)
            rate = get_message_rate(sdos)
            timestamp = time.time()
            if begin is False and rate == 0:
                begining_time = timestamp
            else:
                begin = True
                # counters[timestamp - begining_time] = count - last_count
                counters[timestamp - begining_time] = rate
                # if count == last_count:
                if rate == 0:
                    static_counter += 1
                else:
                    static_counter = 0
                # last_count = count
            if static_counter > 5:
                break
            # time.sleep(0.01)
        except HTTPError:
            if begin:
                break

    with open(file_name, "a+") as output_file:
        output_file.write("\n--------------------------\n")
        output_file.write("BUNDLE_PERCENTAGE = " + str(bundle_percentage) + "\n")
        output_file.write("AGREEMENT_TIMEOUT = " + str(agreement_timeout) + "\n")
        output_file.write("Message rate: \n")
        for ts in sorted(counters):
            output_file.write(str(ts)[:5] + ": " + str(counters[ts]) + " \n")
        output_file.write("Total messages: " + str(sum(counters.values())) + "\n")
        output_file.write("\n--------------------------\n")
