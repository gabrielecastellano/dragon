import hashlib
import logging
import pprint
import math
from functools import reduce

import time

import itertools

import sys

from resource_allocation.resoruce_allocation_problem import ResourceAllocationProblem
from sdo_node.bidding.exceptions import NoFunctionsLeft


class SdoBidder:
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

        self.rap = resource_allocation_problem
        self.service_bundle = [str(i)+"_"+s for i, s in enumerate(service_bundle)]
        self.sdo_name = sdo_name

        self.bidding_data = {node: {sdo: self.init_bid() for sdo in self.rap.sdos} for node in self.rap.nodes}
        """ For each node, current resources assigned to sdos with bid values """

        self.per_node_winners = {node: set() for node in self.rap.nodes}
        """ Winners sdos computed at the last iteration for each node """

        self.per_node_last_bids = {node: sys.maxsize for node in self.rap.nodes}
        """ Last bids placed for each nodes. Cannot be exceeded during each rebidding """

        self.implementations = list()
        """ If node is a winner, contains all the won implementation for each service of its bundle """

    def multi_node_auction(self, blacklisted_sdos=set()):
        """

        :param set of str blacklisted_sdos:
        :return: winner_list, assignment_dict, lost_nodes
        """

        logging.info("****** Start Auction ******")
        logging.debug(": blacklisted sdos: " + str(blacklisted_sdos))
        winners = {node: set() for node in self.rap.nodes}
        lost_nodes = {sdo: set() for sdo in self.rap.sdos}
        bidded_nodes = {sdo: set() for sdo in self.rap.sdos}

        # compute auction for all nodes
        assignment_dict = dict()
        for node in self.rap.nodes:
            node_winner_list, node_assignment_dict = self.auction(node, blacklisted_sdos)
            logging.debug(": node_winner_list: " + str(node_winner_list))
            winners[node] = node_winner_list
            assignment_dict[node] = node_assignment_dict

        # stores, for each sdo, lost nodes and bidded nodes
        for sdo in self.rap.sdos:
            bidded_nodes[sdo] = self.get_sdo_bid_nodes(sdo)
            lost_nodes[sdo] = {n for n in bidded_nodes[sdo] if sdo not in winners[n]}

        # check if, in some nodes, there are winner that lost for sure at least an other node
        # in that case, remove them and repeate again the auction
        fake_winners = self._compute_fake_winners(winners, bidded_nodes, lost_nodes)
        logging.debug("fake winners: " + str(fake_winners))
        if len(fake_winners) > 0:
            # recursion
            new_winners, assignment_dict, residual_lost_nodes = self.multi_node_auction(set.union(blacklisted_sdos,
                                                                                                  fake_winners))
            for sdo in self.rap.sdos:
                if sdo not in blacklisted_sdos:
                    if sdo not in fake_winners:
                        lost_nodes[sdo] = residual_lost_nodes[sdo]
            return new_winners, assignment_dict, lost_nodes

        # Auction completed
        logging.info(" WINNERS DICT: '" + pprint.pformat(winners))
        logging.info(" LOST NODES DICT: '" + pprint.pformat(lost_nodes))
        logging.info("******* End Auction *******")
        return winners, assignment_dict, lost_nodes

    def _compute_fake_winners(self, winners, bidded_nodes, lost_nodes):
        """
        Fake winner definition: an sdo that won some nodes, but lost at least an other node against somebody that is
        not, in turn, an other fake winner.
        Ambiguous situation are solved given precedence to the higher bidder.
        Example:
        (let's suppose that just the first sdo of the list is the winner for that node)
        n0: [sdo1, sdo0]
        n1: [sdo0, sdo1]
        n2: [sdo2, sdo1]
        sdo0 needs n0 and n1, but lost n0 against sdo1. However, sdo1 is a "fake winner". In fact, he needs n2, but he
        lost it against sdo2, that is not a "fake winner" for sure, since he won all needed nodes.
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
                    fake_winner, found_fakes = self._find_fake_winner(sdo, node, winners, max_bids, bidded_nodes,
                                                                      lost_nodes, known_fakes)
                    collected_fakes.update(found_fakes)
                    if fake_winner is not None:
                        # found a possible fake winner
                        logging.debug("possible fake winner: " + fake_winner)
                        collected_fakes.add(fake_winner)
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
    def _find_fake_winner(sdo, node, winners, max_bids, bidded_nodes, lost_nodes, known_fakes, ignore=list()):
        """
        Search and return for an sdo against who the given sdo lost the given node, but that,
        recursively, lost for sure an other node against someone else.
        :param sdo:
        :param node:
        :param winners:
        :param max_bids:
        :param bidded_nodes:
        :param lost_nodes:
        :param known_fakes:
        :param ignore: the recursion chain of sdos to ignore (avoid recursion loops)
        :return:
        """
        found_fakes = set()
        for w in sorted(winners[node], key=lambda x: max_bids[x]):
            # check if w is a fake winner
            if w in known_fakes:
                # w is already known to be fake!
                return w, found_fakes
            if w not in ignore and len(bidded_nodes[w]) > 0:
                for lost_node in lost_nodes[w]:
                    # w lost this node, check if, for this node there is a fake winner
                    other_fake, other_fakes = SdoBidder._find_fake_winner(w, lost_node, winners, max_bids, bidded_nodes,
                                                                          lost_nodes, known_fakes.union(found_fakes),
                                                                          ignore + [sdo])
                    if other_fake is None:
                        # no fakes winners found to save w, he lost that node for sure! so w is a fake winner
                        return w, found_fakes
                    else:
                        # w is not for sure a fake winner, because we found that he lost, in turn, against a fake one
                        found_fakes.add(other_fake)
                        found_fakes.update(other_fakes)
        return None, found_fakes

    def auction(self, node, blacklisted_sdos=set()):
        """
        Greedy approach to solve the knapsack problem: select winner sdo maximizing total bid and fitting node resources
        :param str node:
        :param set of str blacklisted_sdos:
        :return: list of winners, node assignment_dict
        """

        logging.info("****** Auction on node '" + node + "' ******")
        node_winners = set()
        node_residual_resources = dict(self.rap.available_resources[node])
        node_assignment_dict = {sdo: dict() for sdo in self.rap.sdos}
        logging.info("Bidding data: " + pprint.pformat(self.bidding_data[node], compact=True))
        while True:
            logging.debug(" - Search for best bidder to add ...")
            higher_bid = 0
            higher_bidder = None

            # look for the highest one
            for sdo in sorted(self.bidding_data[node], key=lambda x: x):
                # skip if blacklisted
                if sdo in blacklisted_sdos:
                    continue
                # skip if is already a winner
                if sdo in node_winners or 'bid' not in self.bidding_data[node][sdo]:
                    continue
                # get total bid for this sdo
                sdo_bid = self.bidding_data[node][sdo]['bid']
                logging.debug(" --- candidate: '" + sdo + "' | bid: '" + str(sdo_bid) + "'")
                # check if is the higher so far
                if sdo_bid > higher_bid:
                    logging.debug(" ----- is the best so far ...")
                    # check if solution would be infrastructure-bounded
                    if self.rap.check_custom_node_bound({sdo: self.bidding_data[node][sdo]}, node_residual_resources):
                        logging.debug(" ----- is feasible: update best sdo.")
                        higher_bid = sdo_bid
                        higher_bidder = sdo

            # check if we found one
            if higher_bid != 0:
                # add the bidder to the winner list
                logging.debug(" - WINNER: '" + higher_bidder + "' | BID: '" + str(higher_bid) + "'")
                node_assignment_dict[higher_bidder] = self.bidding_data[node][higher_bidder]
                node_winners.add(higher_bidder)
                allocated_resources = node_assignment_dict[higher_bidder]["consumption"]
                node_residual_resources = self.rap.sub_resources(node_residual_resources, allocated_resources)
            else:
                # greedy process has finished
                logging.debug(" - No winner found, auction terminated.'")
                break

        logging.info(" NODE " + node + " | WINNER LIST: " + pprint.pformat(node_winners))
        logging.info("******* End Auction *******")
        return node_winners, node_assignment_dict

    def sdo_bidding(self):
        """
        Builds, if possible, a winning assignment for this sdo, and add it to the global bidding data.
        Assignment can be the one optimizing the utility or, if it would not win, the one fitting left space, if any.
        :return:
        """

        logging.info("------------ Starting bid process -------------")
        # 1. Build, greedy, the best function vector (max total BID), that also is infrastructure-bounded
        winners = {node: set() for node in self.rap.nodes}
        assignment_dict = None
        blacklisted_nodes = set()
        desired_implementation = list()
        self.implementations = list()
        # try to get the best greedy bundle stopping if sdo lost auction in all the nodes
        winners_set = self.get_winners(winners)
        while self.sdo_name not in winners_set and len(blacklisted_nodes) < len(self.rap.nodes):
            logging.info("Search for desired bundle ...")
            logging.info("Blacklisting nodes " + str(blacklisted_nodes))
            desired_bid_bundle = self._greedy_bid(self.rap.available_resources, blacklisted_nodes)
            logging.info("Desired bundle: " + pprint.pformat(desired_bid_bundle))
            if desired_bid_bundle is None:
                # release biddings
                for node in self.rap.nodes:
                    self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())
                # compute auction to discover residual resources
                winners, assignment_dict, lost_nodes = self.multi_node_auction()
                self.per_node_winners = winners
                winners_set = set(itertools.chain(*self.per_node_winners.values()))
                break

            assignment = self._build_assignment_from_bid_bundle(desired_bid_bundle)
            desired_implementation = self._build_implementation_bundle_from_bid_bundle(desired_bid_bundle)

            # 2. Check if the bundle would win
            logging.info(" - checking if desired bundle would win ...")
            for node in assignment:
                self.bidding_data[node][self.sdo_name] = assignment[node][self.sdo_name]
            winners, assignment_dict, lost_nodes = self.multi_node_auction()
            self.per_node_winners = winners
            blacklisted_nodes.update(lost_nodes[self.sdo_name])
            # release the bidding on lost nodes
            for node in blacklisted_nodes:
                self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())
            winners_set = self.get_winners()

        logging.info(" --- Winners dict: " + pprint.pformat(winners, compact=True))
        logging.info(" --- Assignment dict: " + pprint.pformat(assignment_dict))
        if self.sdo_name in winners_set:
            # we found the new bids for this sdo
            logging.info(" --- Sdo is a strong winner!!!")
            self.implementations = desired_implementation
        else:
            # 3. If not, repeat bid but just into the residual capacity
            logging.info(" --- Sdo lost auction, checking for a less expensive solution ...")

            for node in self.rap.nodes:
                if self.sdo_name in self.per_node_winners[node]:
                    # set the limits for future rebidding
                    self.per_node_last_bids[node] = self.bidding_data[node][self.sdo_name]['bid']
                    # remove from winners
                    self.per_node_winners[node].discard(self.sdo_name)
                # reset bid
                self.bidding_data[node][self.sdo_name] = self.init_bid(time.time())

            self.implementations = list()
            residual_resources = self.rap.get_residual_resources(assignment_dict)
            logging.info(" ----- Residual resources: " + pprint.pformat(residual_resources))
            logging.info("Search for lighter bundle ...")
            lighter_bid_bundle = self._greedy_bid(residual_resources)
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
        # set the limits for future rebidding
        if self.sdo_name in self.get_winners():
            for node in self.bidding_data:
                # TODO il prossimo if va fatto con i nodi con bid non zero invece di così? (cambia?)
                if self.sdo_name in self.per_node_winners[node]:
                    self.per_node_last_bids[node] = self.bidding_data[node][self.sdo_name]['bid']

        logging.info("Sdo final bidding: " + pprint.pformat({node: self.bidding_data[node][self.sdo_name]
                                                             for node in self.rap.nodes}))
        logging.info("------------ End of bid process -------------")

    def _greedy_bid(self, resource_bound, blacklisted_nodes=set()):
        """
        Find the greedy-best solution fitting the given resources
        :param dict[str, dict[str, int]] resource_bound: for each node, resources that the solution must fit
        :param set of str blacklisted_nodes: those nodes will not be taken in account
        :return dict[str, dict[str, union[str, int]]]: the best optimization bid_bundle found
        """
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
                completed_bid_nodes = self._get_completed_bid_nodes(current_bid_bundle)
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
                current_bid_bundle[s] = {"function": f, "node": n, "utility": mu}
                current_implementations = [(serv,
                                            current_bid_bundle[serv]["function"],
                                            current_bid_bundle[serv]["node"],
                                            current_bid_bundle[serv]["utility"])
                                           for serv in sorted(current_bid_bundle,
                                                              key=lambda x: current_bid_bundle[x]["utility"],
                                                              reverse=True)]
                logging.debug(" --- Current bundle = " + str(current_implementations))
                # check if the total is bounded
                assignments = self._build_assignment_from_bid_bundle(current_bid_bundle)
                if not self.rap.check_custom_bound(assignments, resource_bound):
                    # if not, remove the new function and repeat iteration looking for a worse one
                    logging.debug(" ----- Exceeded capacity, looking for an other one ...")
                    del current_bid_bundle[s]
                    skip_vector[len(current_bid_bundle)] += 1
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
                    return None
                del current_bid_bundle[added_services[-1]]
                added_services = added_services[:-1]
                skip_vector[len(current_bid_bundle)] += 1
        # round utilities
        current_bid_bundle = {k: {'function': v['function'], 'node': v['node'], 'utility': int(round(v['utility']))}
                              for k, v in current_bid_bundle.items()}
        return current_bid_bundle

    def _greedy_bid_bottom(self, resource_bound):
        """
        Find the greedy-best solution fitting the given resources.
        This algorithm starts from a lower bound solution and try to substitute function one-by-one
        in order to increase the utility without exceeding the resources.
        :param resource_bound: resources that the solution must fit
        :return: the best optimization bundle found
        """
        pass

    def _get_next_best_service(self, bid_bundle, skip_first=0, blacklisted_nodes=set()):
        """

        :param bid_bundle:
        :param int skip_first: skip specified number of best services
        :param set of str blacklisted_nodes: those nodes will not be taken in account
        :raises NoFunctionsLeft: when is requested to skip mor services/functions than the available
        :return (str, str, str, float): service, function, node, marginal utility
        """
        utility_dict = dict()

        for service in self.service_bundle:
            if service not in bid_bundle:
                ranked_functions = self._rank_function_for_service(bid_bundle, service, blacklisted_nodes)
                for function, node in ranked_functions:
                    utility_gain = ranked_functions[(function, node)]
                    utility_dict[utility_gain] = service, function, node

        if skip_first >= len(utility_dict):
            raise NoFunctionsLeft("No function left for this bundle")
        marginal_utility, (best_service, best_function, best_node) = sorted(utility_dict.items(),
                                                                            reverse=True)[skip_first]
        return best_service, best_function, best_node, marginal_utility

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
                if int(round(marginal_utility)) <= self.per_node_last_bids[node]:
                    ranked_functions[(function, node)] = marginal_utility
        return ranked_functions

    def _marginal_utility(self, bid_bundle, service, function, node):
        """
        Compute the marginal utility that sdo gains by adding given service:function:node to the bundle.
        This function may depend by the particular SDO.
        :param bid_bundle: initial bundle
        :param str service: service to add to the bundle
        :param str function: function implementing the service to add to the bundle
        :param str node: node where the function will be placed
        :return: the marginal utility
        """
        if not self.rap.check_function_implements_service(service.split('_', 1)[-1], function):
            return 0
        # currently a pseudo-randomized value is returned
        return self._pseudo_marginal_utility(bid_bundle, service, function, node)

    def _pseudo_marginal_utility(self, bid_bundle, service, function, node):
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
        bid_bundle['.'] = {'function': '.', 'utility': 101}
        # create two lists of services and functions that are in the bundle, temporally ordered (decreasing utility)
        taken_services, taken_functions = zip(*sorted([(k, v['function']) for k, v in bid_bundle.items()],
                                                      key=lambda x: bid_bundle[x[0]]['utility'],
                                                      reverse=True))
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
        logging.debug("inf: " + str(inf) + " | sup: " + str(sup))

        # calculate a pseudo-random normalized utility on 1. resource usage 2. bundle+node_name+services+functions
        function_consumption = self._get_function_average_consumption(function)
        # spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 40, 1, 54.598)
        spreaded_consumption = self._gen_log_func(function_consumption, 0, 1, 30, 1, 20.0855)  # [0.00, 0.20] +-0.05
        digest = int(hashlib.sha256((reduce(lambda x, y: x + y, sorted([""]+taken_services)) +
                                     reduce(lambda x, y: x + y, sorted([""]+taken_functions)) +
                                     self.sdo_name).encode('utf-8')).hexdigest(), 16)
        decimal_digest = digest/2**256
        perturbation_factor = (0.3-(-0.3))*decimal_digest + (-0.3)
        logging.debug("av_decimal_consumption: " + str(function_consumption) + " | decimal_digest: " + str(decimal_digest))
        logging.debug("spreaded_consumption: " + str(spreaded_consumption))
        logging.debug("perturbation_factor: " + str(perturbation_factor))
        # transform the decimal consumption so that it is better spread on [0, 1]
        # normalized_value = (spread_consumption+decimal_digest)/2
        perturbated_value = spreaded_consumption+perturbation_factor
        logging.debug("perturbated_value: " + str(perturbated_value))
        if perturbated_value < 0:
            normalized_value = 0
        elif perturbated_value > 1:
            normalized_value = 1
        else:
            normalized_value = perturbated_value

        logging.debug("normalized_value: " + str(normalized_value))

        # scale utility according to first function
        # utility = normalized_value*first_function_spreaded_consumption
        # logging.debug("utility: " + str(utility))

        # apply node-based scaling
        scaling_factor = int(hashlib.sha256((self.sdo_name + node + service).encode()).hexdigest(), 16) / 2 ** 256
        if len(taken_services) > 1 and bid_bundle[taken_services[-2]]['node'] == node:
            # put an high node scaling for the last used node (between 0.70 and 1)
            scaling_factor = (1 - 0.7) * scaling_factor + 0.7

        utility = normalized_value*scaling_factor
        logging.debug("node-based scaled utility: " + str(utility))

        # apply a scaling (given for orchestrator)
        scaling_factor = int(hashlib.sha256(self.sdo_name.encode('utf-8')).hexdigest(), 16)/2**256
        utility = utility*scaling_factor
        logging.debug("sdo-based scaled utility: " + str(utility))

        # put the utility value between inf and sup
        utility = (sup - inf) * utility + inf
        logging.debug("bounded utility: " + str(utility))

        logging.debug("marginal_utility for function '" + function + "' on service '" + service + " ... \n" +
                      " ... taken services " + str(taken_services) + " ... \n" +
                      " ... and functions " + str(taken_functions) + " ... \n" +
                      " ... is: " + str(utility))
        if self.sdo_name == 'sdo1' and utility == 0:
            print(node)
        # exceptional case! (python3 would round at 0)
        if utility < 0.5:
            utility = 0.51
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

    def _get_function_average_consumption(self, function):
        """

        :param function:
        :return: decimal average consumption
        """
        consumption_percentages = list()
        total_resources_amount = self.rap.get_total_resources_amount()
        average_node_resources = {}
        for resource in self.rap.resources:
            average_node_resources[resource] = total_resources_amount[resource]/len(self.rap.nodes)
        for resource in self.rap.resources:
            consumption = self.rap.get_function_resource_consumption(function)[resource]
            total = average_node_resources[resource]
            consumption_percentages.append(consumption / total)

        av_decimal_consumption = sum(consumption_percentages) / float(len(consumption_percentages))
        return av_decimal_consumption

    def _build_assignment_from_bid_bundle(self, bid_bundle):
        """
        Builds and returns the assignment of this sdo for each node
        :param dict[str, dict[str, union[str, int]]] bid_bundle:
        :return dict[str, [dict[str, union[str, dict, float]]:
        """
        assignments = dict()
        ts = time.time()
        for node in set([bid_bundle[s]['node'] for s in bid_bundle]):
            # average
            # overall_node_utility = int(sum([bid_bundle[s]['utility']
            #                           for s in bid_bundle if bid_bundle[s]['node'] == node])/len(bid_bundle))
            # max
            overall_node_utility = max([bid_bundle[s]['utility'] for s in bid_bundle if bid_bundle[s]['node'] == node])
            overall_node_consumption = self.rap.get_bundle_resource_consumption([bid_bundle[s]['function']
                                                                                 for s in bid_bundle
                                                                                 if bid_bundle[s]['node'] == node])
            node_assignment = {'bid': overall_node_utility, 'consumption': overall_node_consumption, 'timestamp': ts}
            assignments[node] = {self.sdo_name: node_assignment}
        return assignments

    @staticmethod
    def _build_implementation_bundle_from_bid_bundle(bid_bundle):
        """

        :param dict[str, union[str, int]]] bid_bundle:
        :return list of (str, str, str): list of implementations (service, function, node), ordered by service name
        """
        return sorted([(s, bid_bundle[s]['function'], bid_bundle[s]['node'])
                       for s in bid_bundle], key=lambda x: x[0])

    def init_bid(self, timestamp=0):
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
    def _get_completed_bid_nodes(bid_bundle):
        """
        Searches and returns nodes already used, except the last one. The last one is the only used node where the sdo
        can still add some function to modify the bid value.
        :param bid_bundle:
        :return set of str: the set of node where sdo cannot modify its bid
        """

        used_nodes = [bid_bundle[s]['node']
                      for s in sorted(bid_bundle, key=lambda s: bid_bundle[s]['utility'], reverse=True)]
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
