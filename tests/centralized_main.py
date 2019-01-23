import argparse
import json
import logging
import os

from config.config import Configuration
from config.logging_configuration import LoggingConfiguration
from resource_assignment.resource_assignment_problem import ResourceAllocationProblem
from dragon_agent.centralized_node import CentralizedNode


def parse_arguments():

    # need to modify global configuration
    global SDO_NAMES
    global SERVICE_BUNDLES
    global CONF_FILE
    global LOG_LEVEL
    global LOG_FILE

    # define arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'bundle',
        type=str,
        nargs='+',
        help='Name of the sdo + services.',
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
        action='store_true',
        help='If not given, log will be redirected on stdout.'
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
        default='config/default-config.ini',
        help='Configuration file [currently not supported].'
    )
    parser.add_argument(
        '-c',
        '--centralized',
        action='store_true',
        help='The log file name. If not given, log will be redirected on stdout.'
    )

    # parse arguments
    args = parser.parse_args()

    SDO_NAMES = list()
    SERVICE_BUNDLES = list()
    CONF_FILE = args.conf_file

    last_sdo = None
    for s in args.bundle:
        if s == ',':
            last_sdo = None
        elif last_sdo is None:
            last_sdo = s
            SDO_NAMES.append(s)
            SERVICE_BUNDLES.append(list())
        else:
            SERVICE_BUNDLES[-1].append(s)

    LOG_LEVEL = args.log_level
    LOG_FILE = args.log_file
    if LOG_FILE is None and args.log_on_file:
        LOG_FILE = "centralized.log"


if __name__ == "__main__":

    parse_arguments()
    configuration = Configuration(CONF_FILE)
    LoggingConfiguration(LOG_LEVEL, LOG_FILE).configure_log()

    rap = ResourceAllocationProblem()
    with open(configuration.RAP_INSTANCE) as rap_file:
        rap.parse_dict(json.loads(rap_file.read()))
    logging.info(rap)

    print(len(SDO_NAMES))

    '''
    for i, sdo in enumerate(SDO_NAMES):
        logging.info(sdo + " [" + ", ".join(SERVICE_BUNDLES[i]) + "]")
        print(sdo + " [" + ", ".join(SERVICE_BUNDLES[i]) + "]")
    '''

    # SDO node
    sdo_node = CentralizedNode(SDO_NAMES, rap, SERVICE_BUNDLES)

    # Start scheduling
    strong, placements, utilities = sdo_node.start_centralized_scheduling()

    for sdo in SDO_NAMES:
        placement_filename = configuration.RESULTS_FOLDER + "/placement_" + sdo + ".json"
        os.makedirs(os.path.dirname(placement_filename), exist_ok=True)
        with open(placement_filename, "w") as f:
            f.write(json.dumps(placements[sdo], indent=4))

        utility_filename = configuration.RESULTS_FOLDER + "/utility_" + sdo + ".json"
        with open(utility_filename, "w") as f:
            f.write(str(utilities[sdo]))

    exit(sum(utilities.values()))
