import logging
import random
import pprint
import colorlog

from resource_assignment.resoruce_allocation_problem import ResourceAllocationProblem
from dragon_agent.agreement.sdo_agreement import SdoAgreement
from dragon_agent.orchestration.sdo_orchestrator import SdoOrchestrator

LOG_ON_FILE = False
LOG_FILE = "sdo1_log.log"
LOG_LEVEL = "VERBOSE"


def log_configuration():

    logging.addLevelName(5, "VERBOSE")
    log_colors = colorlog.default_log_colors
    log_colors["VERBOSE"] = "cyan"
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s.%(msecs)03d | %(levelname)s | [%(funcName)s] %(message)s - %(filename)s:%(lineno)s",
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

    if LOG_ON_FILE:
        # logging.basicConfig(filename=LOG_FILE, level=log_level, format=log_format, datefmt='%d/%m/%Y %H:%M:%S')
        file_handler = logging.FileHandler(LOG_FILE)
        logging.basicConfig(level=log_level, datefmt='%d/%m/%Y %H:%M:%S', handlers=[stream_handler, file_handler])
    else:
        logging.basicConfig(level=log_level, datefmt='%d/%m/%Y %H:%M:%S', handlers=[stream_handler])
    logging.getLogger()
    logging.info("Logging just started!")


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
    sdo1_service_bundle = ["s1", "s2", "s3"]
    sdo1_bidder = SdoOrchestrator("sdo1", rap, sdo1_service_bundle)
    sdo1_agreement = SdoAgreement("sdo1", rap, sdo1_bidder)
    # mu_1 = node_bidder._marginal_utility({}, "s2", "f4")
    # node_bidder.bidding_data['sdo2'] = [('f2', 50), ('f3', 49), ('f5', 48)]
    #sdo1_bidder.bidding_data['sdo2'] = {'bid': 230,
    #                                   'consumption': {'bandwidth': 700, 'cpu': 6, 'memory': 2500},
    #                                   'timestamp': 1517014425.854801}
    sdo1_bidder.sdo_orchestrate()
    logging.info(pprint.pformat(sdo1_bidder.bidding_data))

    # time.sleep(2)

    sdo2_service_bundle = ["s5", "s4", "s4"]
    sdo2_bidder = SdoOrchestrator("sdo2", rap, sdo2_service_bundle)
    sdo2_agreement = SdoAgreement("sdo2", rap, sdo2_bidder)
    sdo2_bidder.sdo_orchestrate()
    logging.info(pprint.pformat(sdo2_bidder.bidding_data))

    # time.sleep(2)
    sdo1_bidding_data_0 = dict(sdo1_bidder.bidding_data)
    sdo1_winners_0 = list(sdo1_bidder.per_node_winners)

    # sdo1 receive info from sdo2
    sdo1_agreement.sdo_agreement(sdo2_bidder.per_node_winners, sdo2_bidder.bidding_data, sdo2_bidder.sdo_name)
    # sdo2 receive old info from sdo1
    sdo2_agreement.sdo_agreement(sdo1_winners_0, sdo1_bidding_data_0, sdo1_bidder.sdo_name)

    print("sdo1_data: \n" + pprint.pformat(sdo1_bidder.bidding_data))
    print("sdo2_data: \n" + pprint.pformat(sdo2_bidder.bidding_data))

    # time.sleep(2)
    sdo1_bidding_data_1 = dict(sdo1_bidder.bidding_data)
    sdo1_winners_1 = list(sdo1_bidder.per_node_winners)

    # sdo1 receive info from sdo2
    sdo1_agreement.sdo_agreement(sdo2_bidder.per_node_winners, sdo2_bidder.bidding_data, sdo2_bidder.sdo_name)
    # sdo2 receive info from sdo1
    sdo2_agreement.sdo_agreement(sdo1_winners_1, sdo1_bidding_data_1, sdo1_bidder.sdo_name)

    print("sdo1_data: \n" + pprint.pformat(sdo1_bidder.bidding_data))
    print("sdo2_data: \n" + pprint.pformat(sdo2_bidder.bidding_data))
