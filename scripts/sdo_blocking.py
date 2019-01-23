import logging
import random
import pprint
import colorlog

from resource_assignment.resoruce_allocation_problem import ResourceAllocationProblem
from dragon_agent.agreement.sdo_agreement import SdoAgreement
from dragon_agent.orchestration.sdo_orchestrator import SdoOrchestrator
from dragon_agent.utils.bidding_message import BiddingMessage
from dragon_agent.utils.messaging import Messaging
from dragon_agent.utils.neighborhood import NeighborhoodDetector


SDO_NAME = "sdo2"
SERVICE_BUNDLE = ['s3', 's5', 's6']
LOG_LEVEL = "INFO"
LOG_FILE = None


def log_configuration():

    logging.addLevelName(15, "VERBOSE")
    log_colors = colorlog.default_log_colors
    log_colors["VERBOSE"] = "cyan"
    fmt = "%(log_color)s%(asctime)s.%(msecs)03d | %(levelname)s | [%(funcName)s] %(message)s - %(filename)s:%(lineno)s"
    formatter = colorlog.ColoredFormatter(
        fmt,
        datefmt="%d/%m/%Y %H:%M:%S",
        log_colors=log_colors,
        secondary_log_colors={},
        style='%'
    )
    if LOG_LEVEL == "DEBUG":
        log_level = logging.DEBUG
    elif LOG_LEVEL == "VERBOSE":
        log_level = "VERBOSE"
    elif LOG_LEVEL == "INFO":
        log_level = logging.INFO
    elif LOG_LEVEL == "WARNING":
        log_level = logging.WARNING
    else:
        log_level = logging.ERROR
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    if LOG_FILE is not None:
        # logging.basicConfig(filename=LOG_FILE, level=log_level, format=log_format, datefmt='%d/%m/%Y %H:%M:%S')
        file_handler = logging.FileHandler(LOG_FILE, mode='w')
        fmt = fmt.replace("%(log_color)", "")
        logging.basicConfig(level=log_level,
                            format=fmt,
                            datefmt='%d/%m/%Y %H:%M:%S',
                            handlers=[file_handler])
    else:
        logging.basicConfig(level=log_level,
                            datefmt='%d/%m/%Y %H:%M:%S',
                            handlers=[stream_handler])
    logging.getLogger()
    logging.info("Logging just started!")


def bid_message_handler(message):
    """

    :param message:
    :type message: BiddingMessage
    :return:
    """
    need_broadcast = False
    logging.info("Handling message from '" + message.sender + "'")
    sdo_agreement.sdo_agreement(message.winners, message.bidding_data, message.sender)

    # rebroadcast
    if sdo_agreement.rebroadcast:
        # broadcast(sdo_bidder)
        need_broadcast = True
    else:
        logging.info("No need to rebroadcast bidding information.")

    # agreement
    if sdo_agreement.agreement:
        logging.info("============================================================================================")
        logging.info("Sdo '" + sdo_agreement.sdo_name + "' has REACHED AGREEMENT SO FAR!!!")
        logging.info("============================================================================================")

        if sdo_agreement.sdo_name in sdo_agreement.sdo_bidder.winners:
            logging.info(" - Sdo '" + sdo_agreement.sdo_name + " got enough resources to implement bundle! :-)")
            logging.info(" - Assigned functions are: \n" + pprint.pformat(sdo_agreement.sdo_bidder.implementations))
        else:
            logging.info(" - Sdo '" + sdo_agreement.sdo_name + " didn't get enough resources to implement bundle :-(")
    return need_broadcast


def broadcast(sdo_bidder):
    """

    :param SdoOrchestrator sdo_bidder:
    :return:
    """
    logging.info("Broadcasting bidding information ...")

    # build the message to broadcast
    message_to_broadcast = BiddingMessage(sender=sdo_bidder.sdo_name,
                                          winners=sdo_bidder.per_node_winners,
                                          bidding_data=sdo_bidder.bidding_data)

    # get the neighbors list
    neighborhood = NeighborhoodDetector(sdo_bidder.rap.sdos).get_neighborhood(sdo_bidder.sdo_name)

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

    log_configuration()

    # RAP instance
    sdos = ["sdo1", "sdo2", "sdo3"]
    services = ["s1", "s2", "s3", "s4", "s5", "s6"]
    functions = ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"]
    resources = ["cpu", "memory", "bandwidth"]
    consumption = dict()
    for function in functions:
        consumption[function] = dict()
        consumption[function]["cpu"] = random.randint(1, 4)
        consumption[function]["memory"] = int(random.triangular(1, 4096, 1024))
        consumption[function]["bandwidth"] = int(random.triangular(1, 1024, 256))
    # manual one, expensive function
    consumption["f1"]["cpu"] = 12
    consumption["f1"]["memory"] = 6*1024
    consumption["f1"]["bandwidth"] = 3*512
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
    broadcast(sdo_bidder)

    # consume
    while not sdo_agreement.agreement:
        print("consuming...")
        message = messaging.consume(sdo_bidder.sdo_name)
        need_broadcast = bid_message_handler(message)
        if need_broadcast:
            # input("Press ENTER to broadcast...")
            broadcast(sdo_bidder)

    logging.info("Agreement")




