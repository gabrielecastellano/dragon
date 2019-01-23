import hashlib
import json
import logging
import pprint
import math
import time
import itertools
import sys

from functools import reduce

from config.config import Configuration
from config.logging_configuration import LoggingConfiguration
from resource_assignment.resoruce_allocation_problem import ResourceAllocationProblem
from dragon_agent.orchestration.exceptions import NoFunctionsLeft, SchedulingTimeout


configuration = None


class SdoOrchestrator:
    """
    This class builds the assignment vector for the given node, through a greedy approach,
    bidding for each function/resource taken.
    bid_bundle: for each service gives function, node and utility - dict[str, union[str, int]]
    implementation_bundle: list of tuple (service, function)
    assignment: the final data that is stored, i.e. dict with bid, consumption, timestamp.
    """

    def __init__(self, sdo_name, resource_allocation_problem, service_bundle):
        """

        :param sdo_name: the name of this node
        :param resource_allocation_problem: the instance of the problem
        :param service_bundle: list of services that this sdo have to deploy
        :type sdo_name: str
        :type resource_allocation_problem: ResourceAllocationProblem
        :type service_bundle: list
        """
        super().__init__()

        global configuration
        configuration = Configuration()

        self.rap = resource_allocation_problem
        self.service_bundle = [str(i)+"_"+s for i, s in enumerate(service_bundle)]
        self.sdo_name = sdo_name

        self.bidding_data = {node: {sdo: self.init_bid() for sdo in self.rap.sdos} for node in self.rap.nodes}
        """ For each node, current resources assigned to sdos with bid values """

        self.per_node_winners = {node: set() for node in self.rap.nodes}
        """ Winners sdos computed at the last iteration for each node """

        self.per_node_max_bid_ratio = {node: sys.maxsize for node in self.rap.nodes}
        """ Last bids/demand placed for each nodes. Cannot be exceeded during each rebidding """

        self.implementations = list()
        """ If node is a winner, contains all the won implementation for each service of its bundle """

        self.private_utility = 0
        """ Stores the value of the private utility function for the current assignment """

        self.detailed_implementations = list()
        """ If node is a winner, contains all the won implementation for each service of its bundle with utilities """

        self._DEBUG_first = True

    def serialize(self):
        """
        Serialize the class instance state into a dict
        :return:
        """
        sdo_dict = dict()
        sdo_dict['bidding-data'] = self.bidding_data
        sdo_dict['per-node-winners'] = self.per_node_winners
        sdo_dict['per-node-max-bid-ratio'] = self.per_node_max_bid_ratio
        sdo_dict['implementations'] = self.implementations
        sdo_dict['private-utility'] = self.private_utility
        sdo_dict['detailed-implementations'] = self.detailed_implementations
        sdo_dict['DEBUG-FIRST'] = self._DEBUG_first

        return sdo_dict

    def parse(self, sdo_dict):
        """
        Parse the class instance state from a dict
        :param sdo_dict:
        :return:
        """
        self.bidding_data = sdo_dict['bidding-data']
        self.per_node_winners = sdo_dict['per-node-winners']
        self.per_node_max_bid_ratio = sdo_dict['per-node-max-bid-ratio']
        self.implementations = sdo_dict['implementations']
        self.private_utility = sdo_dict['private-utility']
        self.detailed_implementations = sdo_dict['detailed-implementations']
        self._DEBUG_first = False

    def multi_node_election(self, blacklisted_sdos=set()):
        """

        :param set of str blacklisted_sdos:
        :return: winner_list, assignment_dict, lost_nodes
        """

        logging.info("****** Start Election ******")
        logging.log(LoggingConfiguration.VERBOSE, ": blacklisted sdos: " + str(blacklisted_sdos))
        winners = {node: set() for node in self.rap.nodes}
        lost_nodes = {sdo: set() for sdo in self.rap.sdos}
        bidded_nodes = {sdo: set() for sdo in self.rap.sdos}

        # compute election for all nodes
        assignment_dict = dict()
        for node in self.rap.nodes:
            node_winner_list, node_assignment_dict = self.election(node, blacklisted_sdos)
            logging.debug(": node_winner_list: " + str(node_winner_list))
            winners[node] = node_winner_list
            assignment_dict[node] = node_assignment_dict

        # stores, for each sdo, lost nodes and bidded nodes
        for sdo in self.rap.sdos:
            bidded_nodes[sdo] = self.get_sdo_bid_nodes(sdo)
            lost_nodes[sdo] = {n for n in bidded_nodes[sdo] if sdo not in winners[n]}

        # check if, in some nodes, there are winner that lost for sure at least an other node
        # in that case, remove them and repeate again the election
        false_winners = self._compute_false_winners(winners, bidded_nodes, lost_nodes)
        logging.debug("fake winners: " + str(false_winners))
        if len(false_winners) > 0:
            # recursion
            new_winners, assignment_dict, residual_lost_nodes = self.multi_node_election(set.union(blacklisted_sdos,
                                                                                                   false_winners))
            for sdo in self.rap.sdos:
                if sdo not in blacklisted_sdos:
                    if sdo not in false_winners:
                        lost_nodes[sdo] = residual_lost_nodes[sdo]
            return new_winners, assignment_dict, lost_nodes

        # Election completed
        logging.info(" WINNERS DICT: '" + pprint.pformat(winners))
        logging.info(" LOST NODES DICT: '" + pprint.pformat(lost_nodes))
        logging.info("******* End Election *******")
        return winners, assignment_dict, lost_nodes

    def _compute_false_winners(self, winners, bidded_nodes, lost_nodes):
        """
        Fake winner definition: an sdo that won some nodes, but lost at least an other node against somebody that is
        not, in turn, an other fake winner.
        Ambiguous situation are solved given precedence to the higher bidder.
        Example:
        (let's suppose that just the first sdo of the list is the winner for that node)
        n0: [sdo1, sdo0]
        n1: [sdo0, sdo1]
        n2: [sdo2, sdo1]
        sdo0 needs n0 and n1, but lost n0 against sdo1. However, sdo1 is a "false winner". In fact, he needs n2, but he
        lost it against sdo2, that is not a "false winner" for sure, since he won all needed nodes.
        In absence of sdo2, both sdo0 and sdo1 may be fake winners. The precedence is given to the one with the higher
        bid value between each nodes. So if sdo1 max bid is higher that the sdo0 one, in absence of sdo2, sdo0 will be
        the fake winner.

        :param winners:
        :param bidded_nodes:
        :param lost_nodes:
        :return:
        """

        known_fakes = set()
        max_bids = {sdo: max([self.bidding_data[node][sdo]['bid'] for node in self.rap.nodes]) for sdo in self.rap.sdos}

        for sdo in sorted(set(itertools.chain(*winners.values())), key=lambda x: max_bids[x], reverse=True):
            logging.debug("checking sdo:" + sdo)
            if sdo in known_fakes:
                continue
            # check if the sdo won in all nodes
            if len(bidded_nodes[sdo]) > 0 and len(lost_nodes[sdo]) > 0:
                # sdo lost some nodes
                logging.debug("sdo lost some nodes")
                collected_fakes = set()
                for node in lost_nodes[sdo]:
                    # check if the node is really lost
                    logging.debug("check if node " + node + " is really lost")
                    # takes the possible fake winners starting from this sdo
                    false_winner, found_falses = self._find_false_winner(sdo, node, winners, max_bids, bidded_nodes,
                                                                         lost_nodes, known_fakes)
                    collected_fakes.update(found_falses)
                    if false_winner is not None:
                        # found a possible fake winner
                        logging.debug("possible fake winner: " + false_winner)
                        collected_fakes.add(false_winner)
                        continue
                    else:
                        logging.debug("node is lost, checked sdo is a fake winner!")
                        known_fakes.add(sdo)
                        break
                # a collected fake is considered fake only if he lost with at least someone that is not a fake in turn
                for fake in sorted(set(collected_fakes)):
                    real_losts = [node for node in lost_nodes[fake] if len(winners[node].difference(known_fakes)) > 0]
                    if len(real_losts) > 0:
                        known_fakes.add(fake)
        return set(known_fakes)

    @staticmethod
    def _find_false_winner(sdo, node, winners, max_bids, bidded_nodes, lost_nodes, known_falses, ignore=list()):
        """
        Search and return for an sdo against who the given sdo lost the given node, but that,
        recursively, lost for sure an other node against someone else.
        :param sdo:
        :param node:
        :param winners:
        :param max_bids:
        :param bidded_nodes:
        :param lost_nodes:
        :param known_falses:
        :param ignore: the recursion chain of sdos to ignore (avoid recursion loops)
        :return:
        """
        found_falses = set()
        for w in sorted(winners[node], key=lambda x: max_bids[x]):
            # check if w is a fake winner
            if w in known_falses:
                # w is already known to be fake!
                return w, found_falses
            if w not in ignore and len(bidded_nodes[w]) > 0:
                for lost_node in lost_nodes[w]:
                    # w lost this node, check if, for this node there is a fake winner
                    other_false, other_falses = SdoOrchestrator._find_false_winner(w, lost_node, winners, max_bids,
                                                                                   bidded_nodes, lost_nodes,
                                                                                   known_falses.union(found_falses),
                                                                                   ignore + [sdo])
                    if other_false is None:
                        # no fakes winners found to save w, he lost that node for sure! so w is a fake winner
                        return w, found_falses
                    else:
                        # w is not for sure a fake winner, because we found that he lost, in turn, against a fake one
                        found_falses.add(other_false)
                        found_falses.update(other_falses)
        return None, found_falses

    def election(self, node, blacklisted_sdos=set()):
        """
        Greedy approach to solve the knapsack problem:
        select winner sdo maximizing total vote and fitting node resources
        :param str node:
        :param set of str blacklisted_sdos:
        :return: list of winners, node assignment_dict
        """

        logging.info("****** Election on node '" + node + "' ******")
        node_winners = set()
        node_residual_resources = dict(self.rap.available_resources[node])
        node_assignment_dict = {sdo: dict() for sdo in self.rap.sdos}
        logging.info("Voting data: " + pprint.pformat(self.bidding_data[node], compact=True))
        while True:
            logging.debug(" - Search for best voter to add ...")
            best_bid_demand_ratio = 0
            best_bidder = None

            # look for the highest one
            for sdo in sorted(self.bidding_data[node], key=lambda x: x):
                # skip if blacklisted
                if sdo in blacklisted_sdos:
                    continue
                # skip if is already a winner
                if sdo in node_winners or 'bid' not in self.bidding_data[node][sdo]:
                    continue
                # skip if bid is 0
                if self.bidding_data[node][sdo]['bid'] == 0:
                    continue
                # get total bid for this sdo
                sdo_bid = self.bidding_data[node][sdo]['bid']
                sdo_demand = self.bidding_data[node][sdo]['consumption']
                logging.debug(" --- candidate: '" + sdo + "' | bid: '" + str(sdo_bid) + "'")
                # check if is the higher so far
                sdo_ratio = sdo_bid/self.rap.norm(node, sdo_demand)
                if sdo_ratio > best_bid_demand_ratio:
                    logging.debug(" ----- is the best so far ...")
                    # check if solution would be infrastructure-bounded
                    if self.rap.check_custom_node_bound({sdo: self.bidding_data[node][sdo]}, node_residual_resources):
                        logging.debug(" ----- is feasible: update best sdo.")
                        best_bidder = sdo
                        best_bid_demand_ratio = sdo_ratio

            # check if we found one
            if best_bidder is not None:
                # add the bidder to the winner list
                logging.debug(" - WINNER: '" + best_bidder + "' | BID_RATIO: '" + str(best_bid_demand_ratio) + "'")
                node_assignment_dict[best_bidder] = self.bidding_data[node][best_bidder]
                node_winners.add(best_bidder)
                allocated_resources = node_assignment_dict[best_bidder]["consumption"]
                node_residual_resources = self.rap.sub_resources(node_residual_resources, allocated_resources)
            else:
                # greedy process has finished
                logging.debug(" - No winner found, election terminated.'")
                break

        logging.info(" NODE " + node + " | WINNER LIST: " + pprint.pformat(node_winners))
        logging.info("******* End Election *******")
        return node_winners, node_assignment_dict

    def sdo_orchestrate(self):
        """
        Builds, if possible, a winning assignment for this sdo, and add it to the global bidding data.
        Assignment can be the one optimizing the utility or, if it would not win, the one fitting left space, if any.
        :return:
        """

        logging.info("------------ Starting orchestration process -------------")
        # 1. Build, greedy, the best function vector (max total BID), that also is infrastructure-bounded
        winners = {node: set() for node in self.rap.nodes}
        assignment_dict = None
        resource_bound = dict(self.rap.available_resources)
        blacklisted_nodes = set()
        desired_implementation = list()
        self.implementations = list()
        self.detailed_implementations = list()
        self.private_utility = 0
        # try to get the best greedy bundle stopping if sdo lost election in all the nodes
        desired_bid_bundle = None
        impl = list()
        winners_set = set()
        for node in self.rap.nodes:
            self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())
        while self.sdo_name not in winners_set and len(blacklisted_nodes) < len(self.rap.nodes):
            logging.info("Search for desired bundle ...")
            logging.info("Blacklisting nodes " + str(blacklisted_nodes))
            try:
                desired_bid_bundle, impl = self._greedy_embedding(resource_bound, blacklisted_nodes)
            except SchedulingTimeout as ste:
                logging.info("Scheduling Timeout: " + ste.message)
                desired_bid_bundle = None
                impl = list()
            logging.info("Desired bundle: " + pprint.pformat(desired_bid_bundle))
            if desired_bid_bundle is None:
                # release biddings
                for node in self.rap.nodes:
                    self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())
                # compute election to discover residual resources
                winners, assignment_dict, lost_nodes = self.multi_node_election()
                self.per_node_winners = winners
                winners_set = set(itertools.chain(*self.per_node_winners.values()))
                break

            assignment = self._build_assignment_from_bid_bundle(desired_bid_bundle)
            desired_implementation = self._build_implementation_bundle_from_bid_bundle(desired_bid_bundle)

            # 2. Check if the bundle would win
            logging.info(" - checking if desired bundle would win ...")
            for node in assignment:
                self.bidding_data[node][self.sdo_name] = assignment[node][self.sdo_name]
            # compute election
            winners, assignment_dict, lost_nodes = self.multi_node_election()
            # set new bid ratio bound
            self._update_bid_ratio_bound(winners, lost_nodes)
            self.per_node_winners = winners
            blacklisted_nodes.update(lost_nodes[self.sdo_name])
            # release the bidding on lost nodes
            for node in lost_nodes[self.sdo_name]:
                self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())
                # bound next assignment on lost node to residual resources (rather than blacklist it)
                resource_bound[node] = dict(self.rap.available_resources[node])
                for w in self.per_node_winners[node]:
                    resource_bound[node] = self.rap.sub_resources(resource_bound[node],
                                                                  self.bidding_data[node][w]['consumption'])
            winners_set = self.get_winners()

        logging.info(" --- Winners dict: " + pprint.pformat(winners, compact=True))
        logging.info(" --- Assignment dict: " + pprint.pformat(assignment_dict))
        if self.sdo_name in winners_set:
            # we found the new bids for this sdo
            logging.info(" --- Sdo is a strong winner!!!")
            self.implementations = desired_implementation
            self.detailed_implementations = impl
            self.private_utility = self._private_utility_from_bid_bundle(desired_bid_bundle)
        else:
            # 3. If not, repeat bid but just into the residual capacity
            logging.info(" --- Sdo lost election, checking for a less expensive solution ...")

            for node in self.rap.nodes:
                if self.sdo_name in self.per_node_winners[node]:
                    # remove from winners
                    self.per_node_winners[node].discard(self.sdo_name)
                #else:
                #    # set a limit lower than the current lowest one to avoid over-rebid
                #    self.per_node_last_bids[node] = min([self.bidding_data[node][s]['bid']
                #                                         for s in self.rap.sdos
                #                                         if self.bidding_data[node][s]['bid'] > 0] +
                #                                        [self.per_node_last_bids[node]])

                # reset bid
                self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())

            self.implementations = list()
            self.detailed_implementations = list()
            residual_resources = self.rap.get_residual_resources(assignment_dict)
            logging.info(" ----- Residual resources: " + pprint.pformat(residual_resources))
            logging.info("Search for lighter bundle ...")
            try:
                lighter_bid_bundle, impl = self._patience_embedding(residual_resources)
            except SchedulingTimeout as ste:
                logging.info("Scheduling Timeout: " + ste.message)
                lighter_bid_bundle = None
                impl = list()
            logging.info("Lighter bundle: " + pprint.pformat(lighter_bid_bundle))
            if lighter_bid_bundle is None:
                logging.info(" ----- There are no solutions fitting the remaining space.")
            else:
                logging.info(" --- Sdo is a weak winner!!!")
                assignment = self._build_assignment_from_bid_bundle(lighter_bid_bundle)
                lighter_implementation = self._build_implementation_bundle_from_bid_bundle(lighter_bid_bundle)
                for node in assignment:
                    self.bidding_data[node][self.sdo_name] = assignment[node][self.sdo_name]
                    self.per_node_winners[node].add(self.sdo_name)
                self.implementations = lighter_implementation
                self.detailed_implementations = impl
                self.private_utility = self._private_utility_from_bid_bundle(lighter_bid_bundle)
        # set the limits for future rebidding
        if self.sdo_name in self.get_winners():
            for node in self.bidding_data:
                # TODO il prossimo if va fatto con i nodi con bid non zero invece di così? (cambia?)
                if self.sdo_name in self.per_node_winners[node]:
                    self.per_node_max_bid_ratio[node] = min(self.per_node_max_bid_ratio[node], self.bidding_data[node][self.sdo_name]['bid'] / self.rap.norm(
                                                            node, self.bidding_data[node][self.sdo_name]['consumption']))

        logging.info("Sdo final voting: " + pprint.pformat({node: self.bidding_data[node][self.sdo_name]
                                                            for node in self.rap.nodes}))
        logging.info("------------ End of orchestration process -------------")

    def _update_bid_ratio_bound(self, winners, lost_nodes):
        """
        bound future bids on each node
        :param winners:
        :param lost_nodes:
        :return:
        """
        for node in self.rap.nodes:
            if len(winners[node]) > 0 and (self.sdo_name in winners[node] or node in lost_nodes[self.sdo_name]
                                          or self.per_node_max_bid_ratio[node] != sys.maxsize):
                min_bid_ratio = min([self.bidding_data[node][w]['bid']/self.rap.norm(
                                                                        node, self.bidding_data[node][w]['consumption'])
                                     for w in winners[node]])
                if self.sdo_name not in winners[node] or \
                        min_bid_ratio < self.bidding_data[node][self.sdo_name]['bid']/self.rap.norm(
                                                        node, self.bidding_data[node][self.sdo_name]['consumption']):
                    min_bid_ratio -= sys.float_info.epsilon
                self.per_node_max_bid_ratio[node] = min(self.per_node_max_bid_ratio[node], min_bid_ratio)

    def _greedy_embedding(self, resource_bound, blacklisted_nodes=set()):
        """
        Find the greedy-best solution fitting the given resources
        :param dict[str, dict[str, int]] resource_bound: for each node, resources that the solution must fit
        :param set of str blacklisted_nodes: those nodes will not be taken in account
        :return dict[str, dict[str, union[str, int]]]: the best optimization bid_bundle found
        """
        begin_ts = time.time()
        current_bid_bundle = dict()
        """ { service_name: { function: function_name, node: node_name, utility: utility_value } } """
        current_utility = 0
        skip_vector = [0]*len(self.service_bundle)
        added_services = list()

        while len(current_bid_bundle) < len(self.service_bundle):
            logging.debug(" - Current bundle: " + pprint.pformat(current_bid_bundle, compact=True))
            logging.debug(" - Skip vector: " + pprint.pformat(skip_vector, compact=True))
            logging.debug(" - Searching for service to add at index: " + str(len(current_bid_bundle)))
            try:
                # exclude nodes where bid is completed
                completed_bid_nodes = set()
                # completed_bid_nodes = self._get_completed_bid_nodes(current_bid_bundle)
                # get the best greedy
                s, f, n, mu = self._get_next_best_service(current_bid_bundle,
                                                          skip_vector[len(current_bid_bundle)],
                                                          set.union(blacklisted_nodes, completed_bid_nodes))
                logging.debug(" --- Found the next " + str(skip_vector[len(current_bid_bundle)]+1) +
                              "-best service: '" + s +
                              "' with function '" + f +
                              "' on node '" + n +
                              "' giving marginal utility " + str(mu))
                # add to current bundle
                current_bid_bundle[s] = {"function": f, "node": n, "utility": mu, "added_at": time.time()}
                current_implementations = [(serv,
                                            current_bid_bundle[serv]["function"],
                                            current_bid_bundle[serv]["node"],
                                            current_bid_bundle[serv]["utility"])
                                           for serv in sorted(current_bid_bundle,
                                                              key=lambda x: current_bid_bundle[x]["added_at"])]
                logging.debug(" --- Current bundle = " + str(current_implementations))
                # check if the total is bounded
                assignments = self._build_assignment_from_bid_bundle(current_bid_bundle)
                if not self.rap.check_custom_bound(assignments, resource_bound):
                    # if not, remove the new function and repeat iteration looking for a worse one
                    logging.debug(" ----- Exceeded capacity, looking for an other one ...")
                    del current_bid_bundle[s]
                    skip_vector[len(current_bid_bundle)] += 1
                    # check timeout
                    #if time.time() > begin_ts + configuration.SCHEDULING_TIME_LIMIT:
                    #    print("TIMEOUT")
                    #    raise SchedulingTimeout("Scheduling took to long, aborted")
                else:
                    # update utility and go next iteration
                    logging.debug(" ----- Bounded, added to bundle.")
                    added_services.append(s)
                    current_utility += mu
            except NoFunctionsLeft:
                # remove most recently added function to go back to previous iteration
                logging.debug(" --- No function fits the remaining capacity, "
                              "changing the one picked at the previous step ...")
                skip_vector[len(current_bid_bundle)] = 0
                if len(added_services) == 0:
                    # there are no feasible solution
                    return None, None
                del current_bid_bundle[added_services[-1]]
                added_services = added_services[:-1]
                skip_vector[len(current_bid_bundle)] += 1

        current_implementations = [(serv,
                                    current_bid_bundle[serv]["function"],
                                    current_bid_bundle[serv]["node"],
                                    current_bid_bundle[serv]["utility"])
                                   for serv in sorted(current_bid_bundle,
                                                      key=lambda x: current_bid_bundle[x]["added_at"])]

        # round utilities
        current_bid_bundle = {k: {'function': v['function'],
                                  'node': v['node'],
                                  'utility': int(round(v['utility'])),
                                  'added_at': v['added_at']}
                              for k, v in current_bid_bundle.items()}
        return current_bid_bundle, current_implementations

    def _patience_embedding(self, resource_bound, blacklisted_nodes=set()):
        """
        Find the patience-best solution fitting the given resources.
        Patience algorithm starts from a lower bound solution and try to substitute function one-by-one
        in order to increase the utility without exceeding the resources.
        :param dict[str, dict[str, int]] resource_bound: for each node, resources that the solution must fit
        :param set of str blacklisted_nodes: those nodes will not be taken in account
        :return dict[str, dict[str, union[str, int]]]: the best optimization bid_bundle found
        """
        begin_ts = time.time()
        current_bid_bundle = dict()
        """ { service_name: { function: function_name, node: node_name, utility: utility_value } } """
        current_utility = 0
        skip_vector = [0]*len(self.service_bundle)
        added_services = list()
        consumption_iterator = {s: 0 for s in self.service_bundle}
        while len(current_bid_bundle) < len(self.service_bundle):
            logging.debug(" - Current bundle: " + pprint.pformat(current_bid_bundle, compact=True))
            logging.debug(" - Skip vector: " + pprint.pformat(skip_vector, compact=True))
            logging.debug(" - Searching for service to add at index: " + str(len(current_bid_bundle)))
            # exclude nodes where bid is completed
            completed_bid_nodes = self._get_completed_bid_nodes(current_bid_bundle)
            # get the best greedy
            s, f, n, mu = self._get_next_lighter_service(current_bid_bundle,
                                                         consumption_iterator,
                                                         {s for s in current_bid_bundle},
                                                         set.union(blacklisted_nodes, completed_bid_nodes),
                                                         resource_bound=resource_bound)
            if s is None:
                # building of bid_bundle is not possible
                return None, None
            logging.debug(" --- Found the next " + str(skip_vector[len(current_bid_bundle)]+1) +
                          "-lighter service: '" + s +
                          "' with function '" + f +
                          "' on node '" + n +
                          "' giving marginal utility " + str(mu))
            # add to current bundle
            current_bid_bundle[s] = {"function": f, "node": n, "utility": mu, "added_at": time.time()}
            current_implementations = [(serv,
                                        current_bid_bundle[serv]["function"],
                                        current_bid_bundle[serv]["node"],
                                        current_bid_bundle[serv]["utility"])
                                       for serv in sorted(current_bid_bundle,
                                                          key=lambda x: current_bid_bundle[x]["added_at"])]

            logging.debug(" --- Current bundle = " + str(current_implementations))
            # check if the total is bounded
            assignments = self._build_assignment_from_bid_bundle(current_bid_bundle)
            if not self.rap.check_custom_bound(assignments, resource_bound):
                # if not, there are no feasible solution
                logging.debug(" ----- Exceeded capacity, no feasible assignment found ...")
                return None, None
            else:
                # update utility and go next iteration
                logging.debug(" ----- Bounded, added to bundle.")
                added_services.append(s)
                current_utility += mu

        logging.debug(" - lightest bundle found, trying to improve it.")
        not_improvable_services = set()
        consumption_iterator = {s: self._get_function_average_consumption(current_bid_bundle[s]["function"],
                                                                          node=current_bid_bundle[s]["node"],
                                                                          resources=resource_bound)
                                for s in current_bid_bundle}
        while len(not_improvable_services) < len(current_bid_bundle):
            # exclude nodes where bid is completed
            # improvable_services = {s for s in current_bid_bundle if s not in not_improvable_services}
            completed_bid_nodes = self._get_completed_bid_nodes(current_bid_bundle,
                                                                to_consider_services=not_improvable_services)
            s, f, n, mu = self._get_next_lighter_service(current_bid_bundle,
                                                         consumption_iterator,
                                                         not_improvable_services,
                                                         set.union(blacklisted_nodes, completed_bid_nodes),
                                                         resource_bound=resource_bound)
            if s is None:
                # nothing better found
                break
            consumption_iterator[s] = self._get_function_average_consumption(f, node=n, resources=resource_bound)
            if mu > current_bid_bundle[s]["utility"]:
                old_impl = dict(current_bid_bundle[s])
                current_bid_bundle[s] = {"function": f, "node": n, "utility": mu, "added_at": time.time()}

                # check if the total is bounded
                assignments = self._build_assignment_from_bid_bundle(current_bid_bundle)
                if not self.rap.check_custom_bound(assignments, resource_bound):
                    # if not, go back to last bundle
                    logging.debug(" ----- Exceeded capacity, this service cannot be improved ...")
                    current_bid_bundle[s] = old_impl
                    not_improvable_services.add(s)
                else:
                    # update utility and go next iteration
                    logging.debug(" ----- Bounded, added to bundle.")
                    current_implementations = [(serv,
                                                current_bid_bundle[serv]["function"],
                                                current_bid_bundle[serv]["node"],
                                                current_bid_bundle[serv]["utility"])
                                               for serv in sorted(current_bid_bundle,
                                                                  key=lambda x: current_bid_bundle[x]["added_at"])]
                    logging.debug(" --- Current bundle = " + str(current_implementations))
                    added_services.append(s)
                    current_utility += mu
                    if time.time() > begin_ts + configuration.SCHEDULING_TIME_LIMIT:
                        break

        current_implementations = [(serv,
                                    current_bid_bundle[serv]["function"],
                                    current_bid_bundle[serv]["node"],
                                    current_bid_bundle[serv]["utility"])
                                   for serv in sorted(current_bid_bundle,
                                                      key=lambda x: current_bid_bundle[x]["added_at"])]

        # round utilities
        current_bid_bundle = {k: {'function': v['function'],
                                  'node': v['node'],
                                  'utility': int(round(v['utility'])),
                                  'added_at': v['added_at']}
                              for k, v in current_bid_bundle.items()}

        return current_bid_bundle, current_implementations

    def _get_next_best_service(self, bid_bundle, skip_first=0, blacklisted_nodes=set()):
        """

        :param bid_bundle:
        :param int skip_first: skip specified number of best services
        :param set of str blacklisted_nodes: those nodes will not be taken in account
        :raises NoFunctionsLeft: when is requested to skip mor services/functions than the available
        :return (str, str, str, float): service, function, node, marginal utility
        """
        utility_list = list()

        for service in self.service_bundle:
            if service not in bid_bundle:
                ranked_functions = self._rank_function_for_service(bid_bundle, service, blacklisted_nodes)
                for function, node in ranked_functions:
                    utility_gain = ranked_functions[(function, node)]
                    utility_list.append((utility_gain, service, function, node))

        if skip_first >= len(utility_list):
            raise NoFunctionsLeft("No function left for this bundle")
        marginal_utility, best_service, best_function, best_node = sorted(utility_list, key=lambda x: x[0],
                                                                          reverse=True)[skip_first]
        return best_service, best_function, best_node, marginal_utility

    def _get_next_lighter_service(self, bid_bundle, consumptions_iterator, skip_services=set(),
                                  blacklisted_nodes=set(), resource_bound=None):
        """

        :param bid_bundle:
        :param skip_services:
        :param blacklisted_nodes:
        :param resource_bound:
        :return:
        """

        lighter_function = (None, None, None, None)
        lighter_consumption = sys.maxsize

        for service in self.service_bundle:
            if service not in skip_services:
                free_bid_bundle = {s: bid_bundle[s] for s in bid_bundle if s != service}
                for function in self.rap.get_implementations_for_service(service.split('_', 1)[-1]):
                    for node in [node for node in self.rap.nodes if node not in blacklisted_nodes]:
                        function_average_consumption = self._get_function_average_consumption(function,
                                                                                              node=node,
                                                                                              resources=resource_bound)
                        if lighter_consumption > function_average_consumption > consumptions_iterator[service]:
                            lighter_consumption = function_average_consumption
                            marginal_utility = self._marginal_utility(free_bid_bundle, service, function, node)
                            lighter_function = (service, function, node, marginal_utility)
        return lighter_function

    # Not used
    def _get_best_function_for_service(self, bid_bundle, service):
        """

        :param bid_bundle:
        :param service:
        :return: function, node, marginal utility
        """
        # search for nodes that have already been used excluding the last one
        # in order to skip if node has been used before the last one (to ensure sub-modularity of node bidding)
        used_nodes = self._get_completed_bid_nodes(bid_bundle)
        # search best function
        best_function = None
        best_node = None
        best_marginal_utility = 0
        for function in self.rap.get_implementations_for_service(service.split('_', 1)[-1]):
            for node in [node for node in self.rap.nodes if node not in used_nodes]:
                marginal_utility = self._marginal_utility(bid_bundle, service, function, node)
                if marginal_utility > best_marginal_utility:
                    best_function = function
                    best_node = node
                    best_marginal_utility = marginal_utility
        return best_function, best_node, best_marginal_utility

    def _rank_function_for_service(self, bid_bundle, service, blacklisted_nodes=set()):
        """
        Returns a set of possible function:node implementing the given service together with the marginal gain
        :param bid_bundle:
        :param service:
        :param blacklisted_nodes: those nodes will not be taken in account
        :type bid_bundle: dict
        :type service: str
        :type blacklisted_nodes: set of str
        :return dict[(str, str), float]: dict of (function, node), marginal utility
        """
        # search for functions
        ranked_functions = dict()
        for function in self.rap.get_implementations_for_service(service.split('_', 1)[-1]):
            for node in [node for node in self.rap.nodes if node not in blacklisted_nodes]:
                marginal_utility = self._marginal_utility(bid_bundle, service, function, node)
                ranked_functions[(function, node)] = marginal_utility
        return ranked_functions

    def _marginal_utility(self, bid_bundle, service, function, node, service_specific=False):
        """
        Compute the marginal utility that sdo gains by adding given service:function:node to the bundle.
        This function may depend by the particular SDO.
        :param bid_bundle: initial bundle
        :param str service: service to add to the bundle
        :param str function: function implementing the service to add to the bundle
        :param str node: node where the function will be placed
        :return: the marginal utility
        """
        spu = configuration.SUBMODULAR_P_UTILITY
        if not self.rap.check_function_implements_service(service.split('_', 1)[-1], function):
            return 0
        if configuration.PRIVATE_UTILITY == "SERVICE" or service_specific:
            # currently a pseudo-randomized value is returned
            if self.sdo_name == 'sdo0':
                if self._DEBUG_first is True:
                    return self._mobile_game_marginal_utility(bid_bundle, service, function, node, submodular=spu)
                else:
                    return self._mobile_game_marginal_utility(bid_bundle, service, function, node, submodular=spu)
            return self._pseudo_marginal_utility(bid_bundle, service, function, node, submodular=spu)
        elif configuration.PRIVATE_UTILITY == "POWER-CONSUMPTION":
            return self._power_consumption_marginal_utility(bid_bundle, service, function, node, submodular=spu)
        elif configuration.PRIVATE_UTILITY == "GREEDY":
            return self._greed_marginal_utility(bid_bundle, service, function, node, submodular=spu)
        elif configuration.PRIVATE_UTILITY == "LOAD-BALANCE":
            return self._load_balancer_marginal_utility(bid_bundle, service, function, node, submodular=spu)
        elif configuration.PRIVATE_UTILITY == "NODE-LOADING":
            return self._node_loading_marginal_utility(bid_bundle, service, function, node, submodular=spu)
        elif configuration.PRIVATE_UTILITY == "BEST-FIT-POLICY":
            # if self.sdo_name == 'sdo1' or self.sdo_name == 'sdo4' or self.sdo_name == 'sdo7' or self.sdo_name == 'sdo10' or self.sdo_name == 'sdo13' or self.sdo_name == 'sdo19':
            #     return self._load_balancer_marginal_utility(bid_bundle, service, function, node, submodular=spu)
            # elif self.sdo_name == 'sdo8' or self.sdo_name == 'sdo11' or self.sdo_name == 'sdo12' or self.sdo_name == 'sdo15' or self.sdo_name == 'sdo16' or self.sdo_name == 'sdo18':
            #     return self._node_loading_marginal_utility(bid_bundle, service, function, node, submodular=spu)
            if self.sdo_name == 'sdo1' or self.sdo_name == 'sdo2' or self.sdo_name == 'sdo4' or self.sdo_name == 'sdo5' or self.sdo_name == 'sdo10' or self.sdo_name == 'sdo11' or self.sdo_name == 'sdo13' or self.sdo_name == 'sdo14' or self.sdo_name == 'sdo15':
                return self._load_balancer_marginal_utility(bid_bundle, service, function, node, submodular=spu)
            elif self.sdo_name == 'sdo12' or self.sdo_name == 'sdo13' or self.sdo_name == 'sdo17':
                return self._node_loading_marginal_utility(bid_bundle, service, function, node, submodular=spu)
            else:
                return self._greed_marginal_utility(bid_bundle, service, function, node, submodular=spu)
        else:
            return 0

    def _balanced_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """

        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """

        if function == "f11":
            return 0

        '''
        if len(bid_bundle) > 0:
            last_node = bid_bundle[sorted(bid_bundle, key=lambda x: bid_bundle[x]['added_at'])[-1]]['node']
        else:
            last_node = ''
        bonus = 1.001 if last_node == node else 1
        '''

        function_consumption = self.rap.norm(node, self.rap.consumption[function])
        already_scheduled = len([s for s in bid_bundle if bid_bundle[s]['node'] == node])
        already_scheduled_percentage = already_scheduled / len(self.service_bundle)
        residual = 1 - already_scheduled_percentage
        return function_consumption*residual

    def _traffic_based_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """

        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """

        if function == "f11":
            return 1

        stat_filename = "config/use_case_stat/cache_statistics.json"
        with open(stat_filename, "r") as stat_file:
            stats = json.loads(stat_file.read())
        traffic_percentage = stats["traffic"][node]/(100*300)
        # print(traffic_percentage)
        already_scheduled = len([s for s in bid_bundle if bid_bundle[s]['node'] == node])
        already_scheduled_percentage = already_scheduled/len(self.service_bundle)
        function_consumption = self.rap.norm(node, self.rap.consumption[function])
        # function_consumption = self._get_function_average_consumption(function)

        if already_scheduled_percentage < traffic_percentage:
            return function_consumption/4
        else:
            return 0

    def _mobile_game_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """

        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :param submodular:
        :return:
        """
        # take latency statistics from file
        stat_filename = "config/use_case_stat/game_statistics.json"
        with open(stat_filename, "r") as stat_file:
            stats = json.loads(stat_file.read())

        user_node = [node for node in stats['users'] if stats['users'][node] == 1][0]
        game_node = [node for node in stats['current-copies'] if stats['current-copies'][node] == 1][0]

        max_latency = stats['max-latency']
        latency = self._get_latency(stats['topology'], user_node, node)

        same_node_factor = int(node == game_node)
        latency_factor = (max_latency - latency) / max_latency * 100

        function_factor = 1
        if function == 'f9':
            function_factor = 3
        if function == 'f10':
            function_factor = 10

        return (latency_factor/1.5 + same_node_factor)/function_factor

    @staticmethod
    def _get_latency(topology, node_a, node_b):

        shortest_path = SdoOrchestrator._get_path(topology, node_a, node_b)

        latency = 0
        src = node_a
        for dst in shortest_path[1:]:
            latency += topology[src + ':' + dst]
            src = dst
        return latency

    @staticmethod
    def _get_path(topology, node_a, node_b):

        if node_a == node_b:
            return []

        a_tree = dict()
        a_tree[node_a] = SdoOrchestrator._get_tree_to_domain(topology, node_a, node_b, Configuration().NODE_NUMBER - 2)
        a_paths = SdoOrchestrator._get_path_list(a_tree, [])
        a_paths = [path for path in a_paths if path[-1] == node_b]

        if len(a_paths) == 0:
            return None
        shortest_path = a_paths[0]
        for path in a_paths:
            if len(path) < len(shortest_path):
                shortest_path = path
        return shortest_path

    @staticmethod
    def _get_path_list(tree, prefix):
        paths = []
        for domain in tree:
            p = []
            p.extend(prefix)
            p.append(domain)
            if len(tree[domain]) > 0:
                paths.extend(SdoOrchestrator._get_path_list(tree[domain], p))
            else:
                prefix.append(domain)
                paths.append(prefix)
        return paths

    @staticmethod
    def _get_tree_to_domain(topology, root_node, leaf_node, deep):
        tree = {}
        for link in [l for l in topology if l.split(':')[0] == root_node]:
            if link.split(':')[1] != leaf_node and deep > 0:
                tree[link.split(':')[1]] = SdoOrchestrator._get_tree_to_domain(topology, link.split(':')[1], leaf_node, deep - 1)
            else:
                tree[link.split(':')[1]] = {}
        return tree

    def _pseudo_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """
        Function returning a meaningless, submodular, utility for the given function:node.
        The utility is:
         1. higher for function with an high resource usage
         2. different between different orchestrators
         3. between 0 and 100
        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """
        logging.debug(" - Getting utility for function '" + function + "' on service '" + service + "'")

        # put a placeholder element just to avoid zip() complain
        bid_bundle['.'] = {'function': '.', 'added_at': 0}
        # create two lists of services and functions that are in the bundle, temporally ordered (decreasing utility)
        taken_services, taken_functions = zip(*sorted([(k, v['function']) for k, v in bid_bundle.items()],
                                                      key=lambda x: bid_bundle[x[0]]['added_at']))

        # remove the placeholder
        del bid_bundle['.']
        taken_services = list(taken_services)[1:]
        taken_functions = list(taken_functions)[1:]

        logging.debug("Services in bundle: " + pprint.pformat(taken_services))
        logging.debug("Functions in bundle: " + pprint.pformat(taken_functions))

        # Average consumption of first function of the bundle bounds all the utilities
        first_function_consumption = self._get_function_average_consumption((taken_functions + [function])[0])
        # first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 40, 1, 54.598)
        first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 30, 1, 20.0855)
        logging.debug("First function scalar: " + str(first_function_spreaded_consumption))

        if submodular:
            # bounds
            bounds = [((len(self.service_bundle)-x)/len(self.service_bundle)) for x in range(len(taken_services)+2)]
            logging.debug("Bounds: " + pprint.pformat(bounds))

            # apply a transformation to the bounds (transformation remains the same for previous bound)
            taken_services.append(service)
            taken_functions.append(function)
            transformed_bounds = list()
            for index, bound in enumerate(bounds):
                transformation, params = self._get_transformation(taken_services[:index], taken_functions[:index])
                logging.debug("Transformation: " + str(transformation) + ", " + str(bound) + ", " + str(params))
                transformed_bound = transformation(bound, *params)
                if index > 0:
                    transformed_bound = transformed_bound*transformed_bounds[index-1]
                transformed_bounds.append(transformed_bound)
            logging.debug("Transformed bounds: " + pprint.pformat(transformed_bounds))
            transformed_bounds = [int(x*100) for x in transformed_bounds]
            logging.debug("Final bounds: " + pprint.pformat(transformed_bounds))

            # range
            inf = transformed_bounds[-1]
            sup = transformed_bounds[-2]
            if inf == 0:
                inf = 1
        else:
            inf = 1
            sup = 100
        logging.debug("inf: " + str(inf) + " | sup: " + str(sup))

        # calculate a pseudo-random normalized utility on 1. resource usage 2. bundle+node_name+services+functions
        function_consumption = self._get_function_average_consumption(function)
        # spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 40, 1, 54.598)
        spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 30, 1, 20.0855)  # [0.00, 0.20] +-0.05
        digest = int(hashlib.sha256((reduce(lambda x, y: x + y, sorted([""]+taken_services)) +
                                     reduce(lambda x, y: x + y, sorted([""]+taken_functions)) +
                                     self.sdo_name).encode('utf-8')).hexdigest(), 16)
        decimal_digest = digest/2**256
        # perturbation_factor = (0.3-(-0.3))*decimal_digest + (-0.3)
        perturbation_factor = 0
        logging.debug("av_decimal_consumption: " + str(function_consumption) + " | decimal_digest: " + str(decimal_digest))
        logging.debug("spreaded_consumption: " + str(spreaded_consumption))
        logging.debug("perturbation_factor: " + str(perturbation_factor))
        # transform the decimal consumption so that it is better spread on [0, 1]
        # normalized_value = (spread_consumption+decimal_digest)/2
        perturbated_value = spreaded_consumption+perturbation_factor
        logging.debug("perturbated_value: " + str(perturbated_value))
        # normalized_value = (perturbated_value+0.3)/1.6
        normalized_value = perturbated_value

        logging.debug("normalized_value: " + str(normalized_value))

        # scale utility according to first function
        # utility = normalized_value*first_function_spreaded_consumption
        # logging.debug("utility: " + str(utility))

        if self.sdo_name == 'sdo1' or self.sdo_name == 'sdo2' or self.sdo_name == 'sdo4' or self.sdo_name == 'sdo5' or self.sdo_name == 'sdo7' or self.sdo_name == 'sdo8' or self.sdo_name == 'sdo11' or self.sdo_name == 'sdo12' or self.sdo_name == 'sdo15' or self.sdo_name == 'sdo19':
                # or self.sdo_name == 'sdo4' or self.sdo_name == 'sdo8' or self.sdo_name == 'sdo13' or self.sdo_name == 'sdo17':
            normalized_value = 0.5

        if self.sdo_name == 'sdo15':
            normalized_value = 0.2

        utility = normalized_value

        # apply node-based scaling
        scaling_factor = int(hashlib.sha256((self.sdo_name + node + service).encode()).hexdigest(), 16) / 2 ** 256
        if self.sdo_name == 'sdo1' or self.sdo_name == 'sdo4' or self.sdo_name == 'sdo7' or self.sdo_name == 'sdo15' or self.sdo_name == 'sdo16':
            # put a low node scaling for already used node (between 0.0 and 3)
            if len(taken_services) > 1 and node in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
                scaling_factor = (0.1 - 0) * scaling_factor
        #if True:
            # put an high node scaling for the last used node (between 0.7 and 1)
            #if len(taken_services) > 1 and bid_bundle[taken_services[-2]]['node'] == node:
            #    scaling_factor = (1 - 0.7) * scaling_factor + 0.7
        #elif False:
            # put the maximum for the already used ones
            #if len(taken_services) > 1 and node in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
            #   scaling_factor = 1
        else: # self.sdo_name == 'sdo2' or self.sdo_name == 'sdo6' or self.sdo_name == 'sdo10' or self.sdo_name == 'sdo14' or self.sdo_name == 'sdo18':
            # put a low node scaling for not used nodes (between 0.00 and 0.5)
            if len(taken_services) > 1 and node not in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
                # return 0
                scaling_factor = (0.1 - 0) * scaling_factor
        #elif False:
            # put a low node scaling for already used node (between 0.0 and 3)
            # if len(taken_services) > 1 and node in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
                # return 0
            #    scaling_factor = (0.3 - 0) * scaling_factor
        #elif False:
            # do not reuse nodes
            #if len(taken_services) > 1 and node in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
            #
        utility = utility*scaling_factor
        logging.debug("node-based scaled utility: " + str(utility))

        # apply a scaling (given for orchestrator)
        scaling_factor = int(hashlib.sha256(self.sdo_name.encode('utf-8')).hexdigest(), 16)/2**256
        utility = utility*scaling_factor
        logging.debug("sdo-based scaled utility: " + str(utility))

        utility = 1.043935 + (0.0002072756 - 1.043935)/(1 + (utility/0.1348168)**1.411127)

        # put the utility value between inf and sup
        utility = (sup - inf) * utility + inf
        logging.debug("bounded utility: " + str(utility))

        logging.debug("marginal_utility for function '" + function + "' on service '" + service + " ... \n" +
                      " ... taken services " + str(taken_services) + " ... \n" +
                      " ... and functions " + str(taken_functions) + " ... \n" +
                      " ... is: " + str(utility))

        if utility < 5:
            utility = 5

        return utility

    def _power_consumption_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """
        Function returning a sub-modular for the given function:node.
        The utility is:
         1. higher for function with an small resource usage
         2. higher for already used nodes
         3. between 0 and 100
        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """
        logging.debug(" - Getting utility for function '" + function + "' on service '" + service + "'")

        # put a placeholder element just to avoid zip() complain
        bid_bundle['.'] = {'function': '.', 'added_at': 0}
        # create two lists of services and functions that are in the bundle, temporally ordered (decreasing utility)
        taken_services, taken_functions = zip(*sorted([(k, v['function']) for k, v in bid_bundle.items()],
                                                      key=lambda x: bid_bundle[x[0]]['added_at']))
        # remove the placeholder
        del bid_bundle['.']
        taken_services = list(taken_services)[1:]
        taken_functions = list(taken_functions)[1:]

        logging.debug("Services in bundle: " + pprint.pformat(taken_services))
        logging.debug("Functions in bundle: " + pprint.pformat(taken_functions))

        # Average consumption of first function of the bundle bounds all the utilities
        first_function_consumption = self._get_function_average_consumption((taken_functions + [function])[0])
        # first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 40, 1, 54.598)
        first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 30, 1, 20.0855)
        logging.debug("First function scalar: " + str(first_function_spreaded_consumption))

        if submodular:
            # bounds
            bounds = [((len(self.service_bundle)-x)/len(self.service_bundle)) for x in range(len(taken_services)+2)]
            logging.debug("Bounds: " + pprint.pformat(bounds))

            # apply a transformation to the bounds (transformation remains the same for previous bound)
            taken_services.append(service)
            taken_functions.append(function)
            transformed_bounds = list()
            for index, bound in enumerate(bounds):
                transformation, params = self._get_transformation(taken_services[:index], taken_functions[:index])
                logging.debug("Transformation: " + str(transformation) + ", " + str(bound) + ", " + str(params))
                transformed_bound = transformation(bound, *params)
                if index > 0:
                    transformed_bound = transformed_bound*transformed_bounds[index-1]
                transformed_bounds.append(transformed_bound)
            logging.debug("Transformed bounds: " + pprint.pformat(transformed_bounds))
            transformed_bounds = [int(x*100) for x in transformed_bounds]
            logging.debug("Final bounds: " + pprint.pformat(transformed_bounds))

            # range
            inf = transformed_bounds[-1]
            sup = transformed_bounds[-2]
            if inf == 0:
                inf = 1
        else:
            inf = 1
            sup = 100
        logging.debug("inf: " + str(inf) + " | sup: " + str(sup))

        # calculate a pseudo-random normalized utility on 1. resource usage !(2. bundle+node_name+services+functions)
        function_consumption = self._get_function_average_consumption(function)
        # spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 40, 1, 54.598)
        spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 30, 1, 20.0855)  # [0.00, 0.20] +-0.05

        logging.debug("av_decimal_consumption: " + str(function_consumption))
        logging.debug("spreaded_consumption: " + str(spreaded_consumption))

        utility = 1 - spreaded_consumption

        # apply node-based scaling
        # scaling_factor = 0.5
        # scaling_factor = int(hashlib.sha256((self.sdo_name + service + node).encode()).hexdigest(), 16) / 2 ** 256
        #if len(taken_services) > 1 and node not in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
        #    scaling_factor = (0.1 - 0) * scaling_factor
        # utility = utility*scaling_factor
        logging.debug("node-based scaled utility: " + str(utility))

        # apply a scaling (given for orchestrator)
        # scaling_factor = 0.5
        # scaling_factor = int(hashlib.sha256(("-" + self.sdo_name).encode('utf-8')).hexdigest(), 16)/2**256
        # utility = utility*scaling_factor
        logging.debug("sdo-based scaled utility: " + str(utility))

        # put the utility value between inf and sup
        utility = (sup - inf) * utility + inf
        logging.debug("bounded utility: " + str(utility))

        logging.debug("marginal_utility for function '" + function + "' on service '" + service + " ... \n" +
                      " ... taken services " + str(taken_services) + " ... \n" +
                      " ... and functions " + str(taken_functions) + " ... \n" +
                      " ... is: " + str(utility))
        if utility < 5:
            utility = 5
        return utility

    def _greed_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """
        Function returning a sub-modular for the given function:node.
        The utility is:
         1. higher for function with an high resource usage
         2. higher for already used nodes
         3. between 0 and 100
        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """
        logging.debug(" - Getting utility for function '" + function + "' on service '" + service + "'")

        # put a placeholder element just to avoid zip() complain
        bid_bundle['.'] = {'function': '.', 'added_at': 0}
        # create two lists of services and functions that are in the bundle, temporally ordered (decreasing utility)
        taken_services, taken_functions = zip(*sorted([(k, v['function']) for k, v in bid_bundle.items()],
                                                      key=lambda x: bid_bundle[x[0]]['added_at']))
        # remove the placeholder
        del bid_bundle['.']
        taken_services = list(taken_services)[1:]
        taken_functions = list(taken_functions)[1:]

        logging.debug("Services in bundle: " + pprint.pformat(taken_services))
        logging.debug("Functions in bundle: " + pprint.pformat(taken_functions))

        # Average consumption of first function of the bundle bounds all the utilities
        first_function_consumption = self._get_function_average_consumption((taken_functions + [function])[0])
        # first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 40, 1, 54.598)
        first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 30, 1, 20.0855)
        logging.debug("First function scalar: " + str(first_function_spreaded_consumption))

        if submodular:
            # bounds
            bounds = [((len(self.service_bundle)-x)/len(self.service_bundle)) for x in range(len(taken_services)+2)]
            logging.debug("Bounds: " + pprint.pformat(bounds))

            # apply a transformation to the bounds (transformation remains the same for previous bound)
            taken_services.append(service)
            taken_functions.append(function)
            transformed_bounds = list()
            for index, bound in enumerate(bounds):
                transformation, params = self._get_transformation(taken_services[:index], taken_functions[:index])
                logging.debug("Transformation: " + str(transformation) + ", " + str(bound) + ", " + str(params))
                transformed_bound = transformation(bound, *params)
                if index > 0:
                    transformed_bound = transformed_bound*transformed_bounds[index-1]
                transformed_bounds.append(transformed_bound)
            logging.debug("Transformed bounds: " + pprint.pformat(transformed_bounds))
            transformed_bounds = [int(x*100) for x in transformed_bounds]
            logging.debug("Final bounds: " + pprint.pformat(transformed_bounds))

            # range
            inf = transformed_bounds[-1]
            sup = transformed_bounds[-2]
            if inf == 0:
                inf = 1
        else:
            inf = 1
            sup = 100
        logging.debug("inf: " + str(inf) + " | sup: " + str(sup))

        # calculate a pseudo-random normalized utility on 1. resource usage !(2. bundle+node_name+services+functions)
        function_consumption = self._get_function_average_consumption(function)
        # spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 40, 1, 54.598)
        spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 30, 1, 20.0855)  # [0.00, 0.20] +-0.05

        logging.debug("av_decimal_consumption: " + str(function_consumption))
        logging.debug("spreaded_consumption: " + str(spreaded_consumption))

        utility = spreaded_consumption

        # apply node-based scaling
        # scaling_factor = 0.5
        scaling_factor = int(hashlib.sha256((self.sdo_name + node + service).encode()).hexdigest(), 16) / 2 ** 256
        # if len(taken_services) > 1 and node not in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
        #    scaling_factor = (0.1 - 0) * scaling_factor
        utility = utility*scaling_factor
        logging.debug("node-based scaled utility: " + str(utility))

        # apply a scaling (given for orchestrator)
        # scaling_factor = 0.5
        # scaling_factor = int(hashlib.sha256(("--" + self.sdo_name).encode('utf-8')).hexdigest(), 16)/2**256
        # utility = utility*scaling_factor
        logging.debug("sdo-based scaled utility: " + str(utility))

        # put the utility value between inf and sup
        utility = (sup - inf) * utility + inf
        logging.debug("bounded utility: " + str(utility))

        logging.debug("marginal_utility for function '" + function + "' on service '" + service + " ... \n" +
                      " ... taken services " + str(taken_services) + " ... \n" +
                      " ... and functions " + str(taken_functions) + " ... \n" +
                      " ... is: " + str(utility))
        if utility < 5:
            utility = 5
        return utility

    def _load_balancer_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """
        Function returning a sub-modular for the given function:node.
        The utility is:
         1. higher for not used nodes
         2. between 0 and 100
        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """
        logging.debug(" - Getting utility for function '" + function + "' on service '" + service + "'")

        # put a placeholder element just to avoid zip() complain
        bid_bundle['.'] = {'function': '.', 'added_at': 0}
        # create two lists of services and functions that are in the bundle, temporally ordered (decreasing utility)
        taken_services, taken_functions = zip(*sorted([(k, v['function']) for k, v in bid_bundle.items()],
                                                      key=lambda x: bid_bundle[x[0]]['added_at']))
        # remove the placeholder
        del bid_bundle['.']
        taken_services = list(taken_services)[1:]
        taken_functions = list(taken_functions)[1:]

        logging.debug("Services in bundle: " + pprint.pformat(taken_services))
        logging.debug("Functions in bundle: " + pprint.pformat(taken_functions))

        # Average consumption of first function of the bundle bounds all the utilities
        first_function_consumption = self._get_function_average_consumption((taken_functions + [function])[0])
        # first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 40, 1, 54.598)
        first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 30, 1, 20.0855)
        logging.debug("First function scalar: " + str(first_function_spreaded_consumption))

        if submodular:
            # bounds
            bounds = [((len(self.service_bundle)-x)/len(self.service_bundle)) for x in range(len(taken_services)+2)]
            logging.debug("Bounds: " + pprint.pformat(bounds))

            # apply a transformation to the bounds (transformation remains the same for previous bound)
            taken_services.append(service)
            taken_functions.append(function)
            transformed_bounds = list()
            for index, bound in enumerate(bounds):
                transformation, params = self._get_transformation(taken_services[:index], taken_functions[:index])
                logging.debug("Transformation: " + str(transformation) + ", " + str(bound) + ", " + str(params))
                transformed_bound = transformation(bound, *params)
                if index > 0:
                    transformed_bound = transformed_bound*transformed_bounds[index-1]
                transformed_bounds.append(transformed_bound)
            logging.debug("Transformed bounds: " + pprint.pformat(transformed_bounds))
            transformed_bounds = [int(x*100) for x in transformed_bounds]
            logging.debug("Final bounds: " + pprint.pformat(transformed_bounds))

            # range
            inf = transformed_bounds[-1]
            sup = transformed_bounds[-2]
            if inf == 0:
                inf = 1
        else:
            inf = 1
            sup = 100
        logging.debug("inf: " + str(inf) + " | sup: " + str(sup))

        # calculate a pseudo-random normalized utility on 1. resource usage !(2. bundle+node_name+services+functions)
        function_consumption = self._get_function_average_consumption(function)
        # spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 40, 1, 54.598)
        spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 30, 1, 20.0855)  # [0.00, 0.20] +-0.05

        logging.debug("av_decimal_consumption: " + str(function_consumption))
        logging.debug("spreaded_consumption: " + str(spreaded_consumption))

        utility = 0.5

        # apply node-based scaling
        # scaling_factor = 0.5
        scaling_factor = int(hashlib.sha256((self.sdo_name + service + node).encode()).hexdigest(), 16) / 2 ** 256
        if len(taken_services) > 1 and node in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
            scaling_factor = (0.1 - 0) * scaling_factor
        utility = utility*scaling_factor
        logging.debug("node-based scaled utility: " + str(utility))

        # apply a scaling (given for orchestrator)
        # scaling_factor = 0.5
        # scaling_factor = int(hashlib.sha256(("--" + self.sdo_name).encode('utf-8')).hexdigest(), 16)/2**256
        # utility = utility*scaling_factor
        logging.debug("sdo-based scaled utility: " + str(utility))

        # put the utility value between inf and sup
        utility = (sup - inf) * utility + inf
        logging.debug("bounded utility: " + str(utility))

        logging.debug("marginal_utility for function '" + function + "' on service '" + service + " ... \n" +
                      " ... taken services " + str(taken_services) + " ... \n" +
                      " ... and functions " + str(taken_functions) + " ... \n" +
                      " ... is: " + str(utility))
        if utility < 5:
            utility = 5
        return utility

    def _node_loading_marginal_utility(self, bid_bundle, service, function, node, submodular=True):
        """
        Function returning a sub-modular for the given function:node.
        The utility is:
         1. higher for used nodes
         2. between 0 and 100
        :param bid_bundle:
        :param service:
        :param function:
        :param node:
        :return:
        """
        logging.debug(" - Getting utility for function '" + function + "' on service '" + service + "'")

        # put a placeholder element just to avoid zip() complain
        bid_bundle['.'] = {'function': '.', 'added_at': 0}
        # create two lists of services and functions that are in the bundle, temporally ordered (decreasing utility)
        taken_services, taken_functions = zip(*sorted([(k, v['function']) for k, v in bid_bundle.items()],
                                                      key=lambda x: bid_bundle[x[0]]['added_at']))
        # remove the placeholder
        del bid_bundle['.']
        taken_services = list(taken_services)[1:]
        taken_functions = list(taken_functions)[1:]

        logging.debug("Services in bundle: " + pprint.pformat(taken_services))
        logging.debug("Functions in bundle: " + pprint.pformat(taken_functions))

        # Average consumption of first function of the bundle bounds all the utilities
        first_function_consumption = self._get_function_average_consumption((taken_functions + [function])[0])
        # first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 40, 1, 54.598)
        first_function_spreaded_consumption = self._gen_log_func(first_function_consumption, 0, 1, 30, 1, 20.0855)
        logging.debug("First function scalar: " + str(first_function_spreaded_consumption))

        if submodular:
            # bounds
            bounds = [((len(self.service_bundle)-x)/len(self.service_bundle)) for x in range(len(taken_services)+2)]
            logging.debug("Bounds: " + pprint.pformat(bounds))

            # apply a transformation to the bounds (transformation remains the same for previous bound)
            taken_services.append(service)
            taken_functions.append(function)
            transformed_bounds = list()
            for index, bound in enumerate(bounds):
                transformation, params = self._get_transformation(taken_services[:index], taken_functions[:index])
                logging.debug("Transformation: " + str(transformation) + ", " + str(bound) + ", " + str(params))
                transformed_bound = transformation(bound, *params)
                if index > 0:
                    transformed_bound = transformed_bound*transformed_bounds[index-1]
                transformed_bounds.append(transformed_bound)
            logging.debug("Transformed bounds: " + pprint.pformat(transformed_bounds))
            transformed_bounds = [int(x*100) for x in transformed_bounds]
            logging.debug("Final bounds: " + pprint.pformat(transformed_bounds))

            # range
            inf = transformed_bounds[-1]
            sup = transformed_bounds[-2]
            if inf == 0:
                inf = 1
        else:
            inf = 1
            sup = 100
        logging.debug("inf: " + str(inf) + " | sup: " + str(sup))

        # calculate a pseudo-random normalized utility on 1. resource usage !(2. bundle+node_name+services+functions)
        function_consumption = self._get_function_average_consumption(function)
        # spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 40, 1, 54.598)
        spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 30, 1, 20.0855)  # [0.00, 0.20] +-0.05

        logging.debug("av_decimal_consumption: " + str(function_consumption))
        logging.debug("spreaded_consumption: " + str(spreaded_consumption))

        utility = 0.5

        # apply node-based scaling
        # scaling_factor = 0.5
        scaling_factor = int(hashlib.sha256((self.sdo_name + service + node).encode()).hexdigest(), 16) / 2 ** 256
        if len(taken_services) > 1 and node not in [bid_bundle[s]['node'] for s in taken_services[:-1]]:
            scaling_factor = (0.1 - 0) * scaling_factor
        utility = utility*scaling_factor
        logging.debug("node-based scaled utility: " + str(utility))

        # apply a scaling (given for orchestrator)
        # scaling_factor = 0.5
        # scaling_factor = int(hashlib.sha256(("--" + self.sdo_name).encode('utf-8')).hexdigest(), 16)/2**256
        # utility = utility*scaling_factor
        logging.debug("sdo-based scaled utility: " + str(utility))

        # put the utility value between inf and sup
        utility = (sup - inf) * utility + inf
        logging.debug("bounded utility: " + str(utility))

        logging.debug("marginal_utility for function '" + function + "' on service '" + service + " ... \n" +
                      " ... taken services " + str(taken_services) + " ... \n" +
                      " ... and functions " + str(taken_functions) + " ... \n" +
                      " ... is: " + str(utility))
        if utility < 5:
            utility = 5
        return utility

    def _get_transformation(self, services, functions):
        """
        Returns, pseudo-random, a transformation basing on inputs
        :param services:
        :param functions:
        :return:
        """
        services = [""] + services
        functions = [""] + functions
        pr_1 = int(hashlib.sha256(reduce(lambda x, y: x + y, services+functions).encode('utf-8')).hexdigest(), 16)
        pr_2 = int(hashlib.sha256(reduce(lambda x, y: x + y, functions+services).encode('utf-8')).hexdigest(), 16)
        normalized_pr1 = pr_1/2**256
        normalized_pr2 = pr_2/2**256
        # bits_pr = bin(pr_2)[2:]

        # choose transformation
        if normalized_pr1 > 0.5:
            # polinomial transformation
            power = (5 - 1) * normalized_pr2 + 1
            root = int(normalized_pr1*100) % 2 == 0
            return self._polynomial_transformation, [power, root]
        else:
            # sin transformation
            sin_sign = int(normalized_pr1*100) % 2 == 0
            b = int((5 - 2) * normalized_pr2 + 2)
            a = int((5 - b) * normalized_pr1 + b)
            if not sin_sign:
                a = -a
            return self._x_sin_transformation, [a, b]

    @staticmethod
    def _polynomial_transformation(x, a, root=False):
        """
        Scales input value according to a pow transformation
        :param x: bounded in [0, 1]
        :param a: the pow exponent
        :param root: if true, transform according to a root curve instead of a polynomial one
        :return: the transformed value
        """
        if root:
            a = 1/a
        return x**a

    @staticmethod
    def _x_sin_transformation(x, a, b):
        """
        Scales input value according to a x*sin transformation
        :param x: bounded in [0, 1]
        :param a: modulates sin amplitude (higher is smaller)
        :param a: modulates sin frequency (higher is faster)
        :return: the transformed value
        """
        return x + 1/(a*math.pi)*math.sin(b*math.pi*x)

    @staticmethod
    def _gen_log_func(x, a, k, b, v, q, c=1):
        """

        :param x: the function parameter
        :param a: the lower asymptote
        :param k: the upper asymptote.
        :param b: the growth rate
        :param v: affects near which asymptote maximum growth occurs.
        :param q: is related to the value Y(0)
        :param c:
        :return:
        """
        return float('%.5f' % (a + ((k - a) / (c + q * math.exp(1) ** (-b * x)) ** (1 / v))))

    def _get_function_average_consumption(self, function, node=None, resources=None):
        """

        :param function:
        :return: decimal average consumption
        """
        consumption_percentages = list()

        if node is None and resources is None:
            total_resources_amount = self.rap.get_total_resources_amount()
            nodes_number = len(self.rap.nodes)
        elif node is not None and resources is None:
            total_resources_amount = self.rap.available_resources[node]
            nodes_number = 1
        elif node is None and resources is not None:
            total_resources_amount = {r: sum([resources[n][r] for n in self.rap.nodes]) for r in self.rap.resources}
            nodes_number = len(self.rap.nodes)
        else:
            total_resources_amount = resources[node]
            nodes_number = 1

        average_node_resources = {}
        for resource in self.rap.resources:
            average_node_resources[resource] = total_resources_amount[resource]/nodes_number
        for resource in self.rap.resources:
            consumption = self.rap.get_function_resource_consumption(function)[resource]
            total = average_node_resources[resource]
            if total == 0:
                return sys.maxsize
            consumption_percentages.append(consumption / total)

        av_decimal_consumption = sum(consumption_percentages) / float(len(consumption_percentages))
        return av_decimal_consumption

    def _build_assignment_from_bid_bundle(self, bid_bundle):
        """
        Builds, votes and returns the assignment of this sdo for each node.
        :param dict[str, dict[str, union[str, int]]] bid_bundle:
        :return dict[str, [dict[str, union[str, dict, float]]:
        """
        assignments = dict()
        ts = time.time()

        for n in set([bid_bundle[s]['node'] for s in bid_bundle]):
            private_node_utility = self._private_node_utility_from_bid_bundle(bid_bundle, n)
            overall_node_consumption = self.rap.get_bundle_resource_consumption([bid_bundle[s]['function']
                                                                                 for s in bid_bundle
                                                                                 if bid_bundle[s]['node'] == n])
            # [ VOTING ]
            '''
            # node_bid = private_node_utility  # global policy is to maximize private utilities
            node_cons = {node: 0 for node in self.rap.nodes}
            node_cons[n] = sum([0] + [self.rap.norm(n, overall_node_consumption)
                                      for s in bid_bundle
                                      if bid_bundle[s]['node'] == n])
            node_bid = sum([(sum([self.rap.norm(node, self.bidding_data[node][s]['consumption'])
                                  for s in self.rap.sdos if s != self.sdo_name]) + node_cons[node])**2
                            for node in self.rap.nodes])/100
            '''
            node_bid = private_node_utility  # global policy is to maximize private utilities
            # node_bid = 1/len(set([bid_bundle[s]['node'] for s in bid_bundle]))*100
            # node_bid = (1/(self.rap.norm(n, overall_node_consumption))**2)*10000000
            # [scoring function] ensures convergence guarantees
            demand_norm = self.rap.norm(n, overall_node_consumption)
            if node_bid/demand_norm > self.per_node_max_bid_ratio[n]:
                node_bid = int(demand_norm*self.per_node_max_bid_ratio[n])
            node_assignment = {'bid': node_bid,
                               'consumption': overall_node_consumption,
                               'timestamp': ts}
            assignments[n] = {self.sdo_name: node_assignment}
        return assignments

    @staticmethod
    def _build_implementation_bundle_from_bid_bundle(bid_bundle):
        """

        :param dict[str, union[str, int]]] bid_bundle:
        :return list of (str, str, str): list of implementations (service, function, node), ordered by service name
        """
        return sorted([(s, bid_bundle[s]['function'], bid_bundle[s]['node'])
                       for s in bid_bundle], key=lambda x: x[0])

    @staticmethod
    def _private_node_utility_from_bid_bundle(bid_bundle, node):
        """

        :param dict[str, union[str, int]]] bid_bundle:
        :return:
        """
        if node in set([bid_bundle[s]['node'] for s in bid_bundle]):
            services_in_node = [s for s in bid_bundle if bid_bundle[s]['node'] == node]
            # average
            # return round(sum([bid_bundle[s]['utility'] for s in services_in_node])/len(services_in_node))
            # max
            # return max([bid_bundle[s]['utility'] for s in services_in_node])
            # sum
            return sum([bid_bundle[s]['utility'] for s in services_in_node])
        else:
            return 0

    @staticmethod
    def _private_utility_from_bid_bundle(bid_bundle):
        """

        :param dict[str, union[str, int]]] bid_bundle:
        :return:
        """
        return sum([SdoOrchestrator._private_node_utility_from_bid_bundle(bid_bundle, node)
                    for node in set([bid_bundle[s]['node'] for s in bid_bundle])])

    def init_bid(self, timestamp=0.0):
        """
        Return a 0-bid
        :param timestamp:
        :return dict[str, union[int, dict, float]]:
        """
        return {'bid': 0, 'consumption': {r: 0 for r in self.rap.resources}, 'timestamp': timestamp}

    def get_sdo_bid_nodes(self, sdo):
        """
        Get nodes where sdo has a not-null bid
        :param sdo:
        :return list of str:
        """
        return [node for node in self.rap.nodes if self.bidding_data[node][sdo]['bid'] != 0]

    @staticmethod
    def _get_completed_bid_nodes(bid_bundle, to_consider_services=None):
        """
        Searches and returns nodes already used, except the last one. The last one is the only used node where the sdo
        can still add some function to modify the bid value.
        :param bid_bundle:
        :param to_consider_services: consider just nodes with those services
        :return set of str: the set of node where sdo cannot modify its bid
        """
        if to_consider_services is None:
            to_consider_services = {s for s in bid_bundle}
        used_nodes = [bid_bundle[s]['node']
                      for s in sorted(bid_bundle, key=lambda s: bid_bundle[s]['utility'], reverse=True)
                      if s in to_consider_services]
        if len(used_nodes) > 0:
            last_used_node = used_nodes[-1]
            used_nodes = set(used_nodes)
            used_nodes.remove(last_used_node)
        return set(used_nodes)

    def get_winners(self, winners_dict=None):
        """
        Returns the winners set merging winners for each node
        :param dict[str, set of str] winners_dict: if given, use it instead of the current winners
        :return set of str:
        """
        if winners_dict is not None:
            return set(itertools.chain(*winners_dict.values()))
        else:
            return set(itertools.chain(*self.per_node_winners.values()))

    def sum_bids(self):
        """

        :return:
        """
        return sum([self.bidding_data[n][sdo]['bid'] for sdo in self.rap.sdos for n in self.rap.nodes])

    def get_service_utility(self):
        """
        Computes and returns the real service utility from the implementation bundle
        :return:
        """
        bid_bundle = {}
        difference = 0
        for i, (s, f, n, mu) in enumerate(self.detailed_implementations):
            s_mu = self._marginal_utility(bid_bundle, s, f, n, service_specific=True)
            s_mu = s_mu/100
            # s_mu = 1.043935 + (0.0002072756 - 1.043935)/(1 + (s_mu/0.1348168)**1.411127)
            s_mu = s_mu*100
            bid_bundle[s] = {"function": f, "node": n, "utility": s_mu, "added_at": i+1}
            difference += (s_mu - mu)
        # round utilities
        bid_bundle = {k: {'function': v['function'], 'node': v['node'], 'utility': int(round(v['utility']))}
                      for k, v in bid_bundle.items()}
        return self._private_utility_from_bid_bundle(bid_bundle), difference

    def reset_bids(self, bidding_data):
        """
        Release all previous assigned resources and reset implementations and utility
        :return:
        """
        self.implementations = list()
        self.detailed_implementations = list()
        self.private_utility = 0
        # release the bidding on each bidded nodes
        for node in bidding_data:
            if bidding_data[node][self.sdo_name] != 0:
                self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())
