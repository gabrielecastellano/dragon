import logging
import pprint
import time

from config.logging_configuration import LoggingConfiguration
from sdo_node.orchestration.sdo_orchestrator import SdoOrchestrator


class CentralizedNode:

    def __init__(self, sdo_names, rap, service_bundles):

        # SDO node
        self.sdo_names = sdo_names
        self.rap = rap
        self.sdo_bidders = {}
        for i, sdo_name in enumerate(self.sdo_names):
            self.sdo_bidders[sdo_name] = SdoOrchestrator(sdo_name, self.rap, service_bundles[i])
        self.service_bundles = service_bundles

        self.implementations = {}

        # self.sdo_agreement = SdoAgreement(sdo_name, rap, self.sdo_bidder)

        self.begin_time = 0
        self.end_time = 0

    def start_centralized_scheduling(self):

        self.begin_time = time.time()

        global_bidding_data = None
        losers = set()

        for i, sdo_name in enumerate(self.sdo_names):
            if global_bidding_data is not None:
                self.sdo_bidders[sdo_name].bidding_data = global_bidding_data
            self.sdo_bidders[sdo_name].sdo_orchestrate()
            for sdo in self.sdo_names:
                if sdo not in self.sdo_bidders[sdo_name].get_winners():
                    # release biddings
                    for node in self.rap.nodes:
                        self.sdo_bidders[sdo_name].bidding_data[node][sdo] = self.sdo_bidders[sdo_name].init_bid(time.time())
            global_bidding_data = self.sdo_bidders[sdo_name].bidding_data

        convergence = False
        while not convergence:
            convergence = True
            for i, sdo_name in enumerate(self.sdo_names):
                if sdo_name not in losers:
                    if len([node for node in global_bidding_data
                            if global_bidding_data[node][sdo_name]['bid'] != 0]) == 0:
                        # overbid
                        convergence = False
                        self.sdo_bidders[sdo_name].bidding_data = global_bidding_data
                        self.sdo_bidders[sdo_name].sdo_orchestrate()
                        for sdo in self.sdo_names:
                            if sdo not in self.sdo_bidders[sdo_name].get_winners():
                                # release biddings
                                for node in self.rap.nodes:
                                    self.sdo_bidders[sdo_name].bidding_data[node][sdo] = self.sdo_bidders[
                                        sdo_name].init_bid(time.time())
                        global_bidding_data = self.sdo_bidders[sdo_name].bidding_data
                        if len(self.sdo_bidders[sdo_name].implementations) == 0:
                            losers.add(sdo_name)

        self.end_time = time.time()

        logging.log(LoggingConfiguration.IMPORTANT, "COMPLETED")
        for i, sdo_name in enumerate(self.sdo_names):
            if sdo_name not in losers:
                logging.info(" - Sdo '" + sdo_name + " got enough resources to implement bundle! :-)")
                logging.info(" - Assigned functions are: \n" + pprint.pformat(self.sdo_bidders[sdo_name].implementations))
            else:
                logging.info(" - Sdo '" + sdo_name + " didn't get enough resources to implement bundle :-(")
            print(sdo_name.ljust(5) +
                  " | u: " + str(self.sdo_bidders[sdo_name].private_utility).rjust(3) +
                  " | total time: " + str(self.end_time - self.begin_time)[:5])

        return {sdo: True for sdo in self.sdo_names}, \
               {sdo: self.sdo_bidders[sdo].implementations for sdo in self.sdo_names}, \
               {sdo: self.sdo_bidders[sdo].private_utility for sdo in self.sdo_names}
