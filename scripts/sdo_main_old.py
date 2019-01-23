import argparse
import logging
import pprint

from config.logging_configuration import LoggingConfiguration
from resource_assignment.resoruce_allocation_problem import ResourceAllocationProblem
from dragon_agent.agreement.sdo_agreement import SdoAgreement
from dragon_agent.orchestration.sdo_orchestrator import SdoOrchestrator
from dragon_agent.utils.bidding_message import BiddingMessage
from dragon_agent.utils.messaging import Messaging
from dragon_agent.utils.neighborhood import NeighborhoodDetector

AGREEMENT_TIMEOUT = 10


def parse_arguments():

    # need to modify global configuration
    global SDO_NAME
    global SERVICE_BUNDLE
    global LOG_LEVEL
    global LOG_FILE

    # define arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'sdo_name',
        metavar='sdo-name',
        type=str,
        help='Name of the sdo.',
    )
    parser.add_argument(
        'service',
        type=str,
        nargs='+',
        help='Name of the sdo.',
    )
    parser.add_argument(
        '-l',
        '--log-level',
        nargs='?',
        type=str,
        default='INFO',
        help='The logging level.'
    )
    parser.add_argument(
        '-o',
        '--log-on-file',
        # nargs='?',
        # dest='feature',
        action='store_true',
        help='The log file name. If not given, log will be redirected on stdout.'
    )
    parser.add_argument(
        '-f',
        '--log-file',
        nargs='?',
        default=None,
        help='The log file name. If not given, log will be redirected on stdout.'
    )
    parser.add_argument(
        '-d',
        '--conf_file',
        nargs='?',
        help='Configuration file [currently not supported].'
    )

    # parse arguments
    args = parser.parse_args()

    SDO_NAME = args.sdo_name
    SERVICE_BUNDLE = args.service
    LOG_LEVEL = args.log_level
    LOG_FILE = args.log_file
    if LOG_FILE is None and args.log_on_file:
        LOG_FILE = SDO_NAME + ".log"


def bid_message_handler(message):
    """

    :param message:
    :type message: BiddingMessage
    :return:
    """

    # delete timeout if any
    if sdo_agreement.agreement:
        messaging.del_stop_timeout()

    # run agreement process for this message
    logging.info("Handling message from '" + message.sender + "'")
    sdo_agreement.sdo_agreement(message.winners, message.bidding_data, message.sender)

    # rebroadcast
    if sdo_agreement.rebroadcast:
        broadcast()
    else:
        logging.info("No need to rebroadcast bidding information.")

    # agreement
    if sdo_agreement.agreement:
        logging.info("============================================================================================")
        logging.info("Sdo '" + sdo_agreement.sdo_name + "' has REACHED AGREEMENT SO FAR!!!")
        logging.info("============================================================================================")

        # set timeout to stop wait messages if nothing new arrives
        logging.info(" - Waiting " + str(AGREEMENT_TIMEOUT) + " seconds for new messages before stopping agreement ...")
        messaging.set_stop_timeout(AGREEMENT_TIMEOUT)


def broadcast():
    """

    :return:
    """
    logging.info("Broadcasting bidding information ...")

    # build the message to broadcast
    message_to_broadcast = BiddingMessage(sender=sdo_bidder.sdo_name,
                                          winners=sdo_bidder.per_node_winners,
                                          bidding_data=sdo_bidder.bidding_data)

    # get the neighbors list
    neighborhood = NeighborhoodDetector(sdo_bidder.rap.sdos, True).get_neighborhood(sdo_bidder.sdo_name)

    for neighbor in neighborhood:
        logging.info("Sending message to neighbor '" + neighbor + "' ...")
        send_bid_message(neighbor, message_to_broadcast)
        logging.info("Message has been sent.")

    logging.info("broadcast successfully completed.")


def send_bid_message(dst_sdo, message):
    """

    :param dst_sdo:
    :param message:
    :type dst_sdo: str
    :type message: BiddingMessage
    :return:
    """
    messaging.send_message(dst_sdo, message)


if __name__ == "__main__":

    parse_arguments()
    LoggingConfiguration(LOG_LEVEL, LOG_FILE).configure_log()

    # RAP instance
    sdos = ["sdo1", "sdo2", "sdo3"]
    services = ["s1", "s2", "s3", "s4", "s5", "s6"]
    functions = ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"]
    resources = ["cpu", "memory", "bandwidth"]
    consumption = dict()
    '''
    # TODO this shoul be the same for each sdo
    for function in functions:
        consumption[function] = dict()
        consumption[function]["cpu"] = random.randint(1, 4)
        consumption[function]["memory"] = int(random.triangular(1, 4096, 1024))
        consumption[function]["bandwidth"] = int(random.triangular(1, 1024, 256))
    # manual one, expensive function
    consumption["f1"]["cpu"] = 12
    consumption["f1"]["memory"] = 6*1024
    consumption["f1"]["bandwidth"] = 3*512
    '''
    consumption = {'f1': {'bandwidth': 393, 'cpu': 3, 'memory': 1737},
                   'f2': {'bandwidth': 970, 'cpu': 4, 'memory': 3299},
                   'f3': {'bandwidth': 970, 'cpu': 3, 'memory': 1093},
                   'f4': {'bandwidth': 422, 'cpu': 3, 'memory': 2014},
                   'f5': {'bandwidth': 182, 'cpu': 4, 'memory': 295},
                   'f6': {'bandwidth': 247, 'cpu': 1, 'memory': 3610},
                   'f7': {'bandwidth': 868, 'cpu': 4, 'memory': 3294},
                   'f8': {'bandwidth': 361, 'cpu': 4, 'memory': 3299},
                   'f9': {'bandwidth': 275, 'cpu': 1, 'memory': 1404}}
    available_resources = {"cpu": 18, "memory": 15*1024, "bandwidth": 3*1024}
    implementation = {
        "s1": ["f1", "f2", "f3"],
        "s2": ["f3", "f4"],
        "s3": ["f2", "f4", "f5"],
        "s4": ["f2", "f3"],
        "s5": ["f4", "f5", "f6"],
        "s6": ["f1", "f6"]
    }
    rap = ResourceAllocationProblem(sdos, services, functions, resources, consumption, available_resources,
                                    implementation)
    logging.info(rap)

    # SDO node
    sdo_bidder = SdoOrchestrator(SDO_NAME, rap, SERVICE_BUNDLE)
    sdo_agreement = SdoAgreement(SDO_NAME, rap, sdo_bidder)

    # first bidding
    sdo_bidder.sdo_orchestrate()
    logging.info(pprint.pformat(sdo_bidder.bidding_data))

    # init messaging
    messaging = Messaging("localhost")

    # broadcast first bidding data
    broadcast()

    # start to receive messages
    logging.info("Subscribing to handle messages with topic '" + sdo_bidder.sdo_name + "' ...")
    messaging.register_handler(sdo_bidder.sdo_name, bid_message_handler)
    logging.info("Listening for incoming messages ...")
    messaging.start_consuming()

    # agreement completed
    logging.log(25, "Agreement process reached convergence!")
    if sdo_agreement.sdo_name in sdo_agreement.sdo_bidder.winners:
        logging.info(" - Sdo '" + sdo_agreement.sdo_name + " got enough resources to implement bundle! :-)")
        logging.info(" - Assigned functions are: \n" + pprint.pformat(sdo_agreement.sdo_bidder.implementations))
    else:
        logging.info(" - Sdo '" + sdo_agreement.sdo_name + " didn't get enough resources to implement bundle :-(")
