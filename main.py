import argparse
import json
import logging
import os

from config.configuration import Configuration
from config.logging_configuration import LoggingConfiguration
from resource_allocation.resoruce_allocation_problem import ResourceAllocationProblem
from sdo_node.sdo_node import SDONode


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


if __name__ == "__main__":

    parse_arguments()
    LoggingConfiguration(LOG_LEVEL, LOG_FILE).configure_log()

    rap = ResourceAllocationProblem()
    with open(Configuration.RAP_INSTANCE) as rap_file:
        rap.parse_dict(json.loads(rap_file.read()))
    logging.info(rap)

    # SDO node
    sdo_node = SDONode(SDO_NAME, rap, SERVICE_BUNDLE)

    # Start scheduling
    strong, placement, rates = sdo_node.start_distributed_scheduling()

    placement_filename = Configuration.RESULTS_FOLDER + "/placement_" + SDO_NAME + ".json"
    os.makedirs(os.path.dirname(placement_filename), exist_ok=True)
    with open(placement_filename, "w") as f:
        f.write(json.dumps(placement, indent=4))

    rates_filename = Configuration.RESULTS_FOLDER + "/rates_" + SDO_NAME + ".json"
    with open(rates_filename, "w") as f:
        f.write(json.dumps(list(rates.items()), indent=4))
