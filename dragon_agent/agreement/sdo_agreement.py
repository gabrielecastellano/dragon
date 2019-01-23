import hashlib
import logging
import pprint

import time

from config.logging_configuration import LoggingConfiguration
from resource_assignment.resource_assignment_problem import ResourceAllocationProblem
from dragon_agent.orchestration.sdo_orchestrator import SdoOrchestrator


class SdoAgreement:
    """
    This class manage the consensus phase, where each node solves conflict
    based on bidding data received from neighbors.
    """

    def __init__(self, sdo_name, resource_allocation_problem, sdo_bidder):
        """

        :param sdo_name:
        :param resource_allocation_problem:
        :param sdo_bidder:
        :type sdo_name: str
        :type resource_allocation_problem: ResourceAllocationProblem
        :type sdo_bidder: SdoOrchestrator
        :return:
        """
        self.sdo_name = sdo_name
        self.rap = resource_allocation_problem
        self.sdo_bidder = sdo_bidder
        self.rebroadcast = False
        self.agreement = False
        self.updated = False
        self.per_sdo_agreement = set()
        self._pending_rebid = False

    def sdo_multi_agreement(self, received_data, rebid_enabled=True):
        """

        :param received_data:
        :param rebid_enabled:
        :type received_data: dict[str, dict[str, union[ dict[str, set of str], dict[str, dict[str, dict[str, union[int, str]]]] ]]]
        :return:
        """

        logging.info("--------------- START AGREEMENT ---------------")

        current_bidding_data = dict(self.sdo_bidder.bidding_data)
        current_winners = dict(self.sdo_bidder.per_node_winners)

        overbid = False
        self.agreement = True
        self.updated = False
        self.rebroadcast = False
        self.per_sdo_agreement = set()

        for node in self.rap.nodes:

            logging.log(LoggingConfiguration.IMPORTANT, "Conflict resolution for node '" + node + "'")

            # merge all information keeping the most updated one
            merged_data = dict()

            for sdo in self.rap.sdos:
                if sdo in received_data.keys():
                    # use the data sdo sent about itself
                    merged_data[sdo] = received_data[sdo]['bidding-data'][node][sdo]
                elif sdo == self.sdo_name:
                    # leave local data about myself
                    merged_data[sdo] = current_bidding_data[node][sdo]
                else:
                    # use the most recent data sent
                    bidding_data_list = [received_data[s]['bidding-data'][node][sdo] for s in received_data.keys()]
                    max_ts = max([bidding_data['timestamp'] for bidding_data in bidding_data_list])
                    merged_data[sdo] = [bidding_data for bidding_data in bidding_data_list
                                        if bidding_data['timestamp'] == max_ts][0]

            # compute new winners for this node
            self.sdo_bidder.bidding_data[node] = merged_data

        logging.info("Computing election on new data")
        winners, assignment_dict, lost_nodes = self.sdo_bidder.multi_node_election()
        logging.info("Election completed on new data")

        self.sdo_bidder.per_node_winners = winners
        # blacklisted_nodes = lost_nodes[self.sdo_name]
        if len(lost_nodes[self.sdo_name]) > 0:
            # node has been overbidded
            logging.log(LoggingConfiguration.IMPORTANT, "Node has been overbidded!!")
            # empty implementations
            self.sdo_bidder.reset_bids(current_bidding_data)
            # try to repeat bidding on residual resources
            # self.sdo_bidder.sdo_bidding()
            # update & rebroadcast
            overbid = True

        # if overbid:
        if rebid_enabled and (overbid or self._pending_rebid):
            # try to repeat bidding on residual resources
            self.sdo_bidder.sdo_orchestrate()
            self._pending_rebid = False
        elif overbid:
            # postpone rebid
            self._pending_rebid = True

        # check sdos agreement
        if overbid:
            # update & rebroadcast
            logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
            self.rebroadcast = True
            self.agreement = False
            self.updated = True
            logging.info("---------------- END AGREEMENT ----------------")
            return  # if sdo rebidded, we already completed the agreement phase for all nodes

        for sender in received_data.keys():

            logging.info("Checking agreement with sdo '" + sender + "'")
            self.per_sdo_agreement.add(sender)

            # check if the received message changed the situation
            for node in self.rap.nodes:

                logging.info("Loc winners: " + str(sorted(current_winners[node])))
                logging.info("Rec winners: " + str(sorted(received_data[sender]['winners'][node])))
                logging.info("New winners: " + str(sorted(self.sdo_bidder.per_node_winners[node])))

                current_winners_digest = hashlib.sha256(str(sorted(current_winners[node])).encode()).hexdigest()
                rcvd_winners_digest = hashlib.sha256(
                    str(sorted(received_data[sender]['winners'][node])).encode()).hexdigest()
                new_winners_digest = hashlib.sha256(
                    str(sorted(self.sdo_bidder.per_node_winners[node])).encode()).hexdigest()

                current_node_consumption = {r: sum([current_bidding_data[node][s]["consumption"][r]
                                                    for s in self.rap.sdos])
                                            for r in self.rap.resources}
                rcvd_node_consumption = {r: sum([received_data[sender]['bidding-data'][node][s]["consumption"][r]
                                                 for s in self.rap.sdos])
                                         for r in self.rap.resources}
                new_node_consumption = {r: sum([self.sdo_bidder.bidding_data[node][s]["consumption"][r]
                                                for s in self.rap.sdos])
                                        for r in self.rap.resources}

                # NOTE: in our decision table "UPDATE" means "keep the merge result"

                '''
                elif self._old_bids_win(self.sdo_bidder.bidding_data, self.sdo_bidder.winners):  # some winner has older ts
                    if self.sdo_name in self.sdo_bidder.winners:
                        # update-time & rebroadcast
                        logging.info("UPDATE-TIME & REBROADCAST")
                        self._update_time()
                        self.rebroadcast = True
                    else:
                        # update & rebroadcast
                        logging.debug"UPDATE & REBROADCAST")
                        self.rebroadcast = True
                '''
                agreement_on_node = False

                if sender in current_winners and sender not in received_data[sender]['winners'] \
                        and self.sdo_name in received_data[sender]['winners'] and self.sdo_name not in current_winners:
                    # i is winner for k and k is winner for i
                    # reset & rebroadcast (*?)
                    logging.log(LoggingConfiguration.IMPORTANT, "RESET & REBROADCAST")
                    self._reset(node)
                    self.rebroadcast = True
                    self.updated = True
                elif current_winners_digest == rcvd_winners_digest == new_winners_digest:
                    logging.info("Current winners are equals to received!")
                    if self._compare_bid_times(received_data[sender]['bidding-data'][node], current_bidding_data[node]) > 0:
                        # received at least a new bid time
                        if self.rap.check_equals(current_node_consumption, rcvd_node_consumption)\
                                and self.rap.check_equals(current_node_consumption, new_node_consumption):
                            # some new timestamp but no changes in resource assignment
                            agreement_on_node = True
                            self.updated = True
                            logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & NO-REBROADCAST")
                        else:
                            # winners remain the same but there is some change on resource assignment
                            # update & rebroadcast
                            logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                            self.rebroadcast = True
                            self.updated = True
                    elif self._compare_bid_times(received_data[sender]['bidding-data'][node], current_bidding_data[node]) == 0:
                        # leave & no-rebroadcast
                        agreement_on_node = True
                        logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & NO-REBROADCAST")
                    else:
                        # leave & no-rebroadcast
                        agreement_on_node = True
                        logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & NO-REBROADCAST")
                        # if self.sdo_name not in self.sdo_bidder.winners[node]:
                        #     # leave & no-rebroadcast
                        #     logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & NO-REBROADCAST")
                        # else:
                        #     # leave & rebroadcast
                        #     logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & REBROADCAST")
                        #     self.rebroadcast = True
                elif rcvd_winners_digest == new_winners_digest:  # winners are same of received
                    # update & rebroadcast
                    logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                    if self.rap.check_equals(rcvd_node_consumption, current_node_consumption):
                        agreement_on_node = True
                    self.rebroadcast = True
                    self.updated = True
                elif current_winners_digest == new_winners_digest:  # winners remains the same
                    logging.info("New winners are same of current but not received")
                    if self.sdo_name in self.sdo_bidder.per_node_winners[node]:
                        if self.sdo_name not in received_data[sender]['winners'][node]:
                            # update-time & rebroadcast
                            logging.log(LoggingConfiguration.IMPORTANT, "UPDATE-TIME & REBROADCAST")
                            # self._update_time(node)
                            self.rebroadcast = True
                        else:
                            # update & rebroadcast
                            logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & REBROADCAST")
                            self.rebroadcast = True
                    elif self._compare_bid_times(self.sdo_bidder.bidding_data[node], current_bidding_data[node]) > 0:
                        # received new ts
                        # update & no-rebroadcast
                        logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                        self.rebroadcast = True
                    else:
                        # leave & rebroadcast
                        logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & REBROADCAST")
                        self.rebroadcast = True
                else:  # new data different
                    # update & rebroadcast
                    logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                    self.rebroadcast = True
                    self.updated = True

                if not agreement_on_node:
                    self.agreement = False
                    self.per_sdo_agreement.discard(sender)
                logging.info("Agreement for node: " + str(agreement_on_node))
            # repeat for each node. --)
            logging.info("Agreement with sdo '" + sender + "':" + str(sender in self.per_sdo_agreement))
        logging.info("---------------- END AGREEMENT ----------------")

    def sdo_agreement(self, received_winners, received_bidding_data, sender, rebid_enabled=True):
        """

        :param received_winners: list of the winners for sender (for each node)
        :param received_bidding_data: contains, for each node, all the last bids known by the sender for each sdo
        :param sender:
        :param rebid_enabled:
        :type received_winners: dict[str, set of str]
        :type received_bidding_data: dict[str, dict[str, dict[str, union[int, str]]]]
        :type sender: str
        :return:
        """

        logging.info("--------------- START AGREEMENT ---------------")

        current_bidding_data = dict(self.sdo_bidder.bidding_data)
        current_winners = dict(self.sdo_bidder.per_node_winners)

        logging.info("Received data from '" + sender + "'")
        logging.log(LoggingConfiguration.VERBOSE, "Local data: " + pprint.pformat(current_bidding_data))
        logging.log(LoggingConfiguration.VERBOSE, "Received data: " + pprint.pformat(received_bidding_data))

        overbid = False
        self.agreement = True
        self.updated = False
        self.rebroadcast = False

        '''
        Bid for a single node (current implementation is for single node)
        {'sdo1': {},
         'sdo2': {'bid': 230,
                  'consumption': {'bandwidth': 700, 'cpu': 6, 'memory': 2500},
                  'timestamp': 1517014425.854801},
         'sdo3': {}}
        '''
        # ( -- for each node
        for node in self.rap.nodes:

            logging.log(LoggingConfiguration.IMPORTANT, "Conflict resolution for node '" + node + "'")

            # conflict resolution rules (?) //è diverso dal task allocation, la tabella non va bene//
            # merge all information keeping the most updated one
            merged_data = dict()
            for sdo in self.rap.sdos:
                if current_bidding_data[node][sdo]['timestamp'] >= received_bidding_data[node][sdo]['timestamp']:
                    # own data is the most updated
                    if sdo != sender:
                        # leave
                        merged_data[sdo] = current_bidding_data[node][sdo]
                    else:
                        # update
                        merged_data[sdo] = received_bidding_data[node][sdo]
                else:
                    # received data is newer than own one
                    if sdo != self.sdo_name:
                        # update
                        merged_data[sdo] = received_bidding_data[node][sdo]
                    else:
                        # leave
                        merged_data[sdo] = current_bidding_data[node][sdo]

            # compute new winners for this node
            self.sdo_bidder.bidding_data[node] = merged_data
        '''
            node_winner_list, node_assignment_dict = self.sdo_bidder.election(node)
        '''
        logging.info("Computing election on new data")
        winners, assignment_dict, lost_nodes = self.sdo_bidder.multi_node_election()
        logging.info("Election completed on new data")

        self.sdo_bidder.per_node_winners = winners
        # blacklisted_nodes = lost_nodes[self.sdo_name]
        if len(lost_nodes[self.sdo_name]) > 0:
            # node has been overbidded
            logging.log(LoggingConfiguration.IMPORTANT, "Node has been overbidded!!")
            # empty implementations
            self.sdo_bidder.reset_bids(current_bidding_data)
            # try to repeat bidding on residual resources
            # self.sdo_bidder.sdo_bidding()
            # update & rebroadcast
            overbid = True

        # if overbid:
        if rebid_enabled and (overbid or self._pending_rebid):
            # try to repeat bidding on residual resources
            self.sdo_bidder.sdo_orchestrate()
            self._pending_rebid = False
        elif overbid:
            # postpone rebid
            self._pending_rebid = True

        '''

            if local:
                continue

            # if node was a winner, check if it has been overbidded and lost the resources
            if self.sdo_name in current_winners[node] and self.sdo_name not in node_winner_list \
                    and node in self.sdo_bidder.get_sdo_bid_nodes(self.sdo_name):
                # node has been overbidded
                logging.log(LoggingConfiguration.IMPORTANT, "Node has been overbidded!!")
                # reset his bid for each node
                for n in self.rap.nodes:
                    self.sdo_bidder.bidding_data[n][self.sdo_name] = self.sdo_bidder.init_bid(time.time())
                    self.sdo_bidder.per_node_winners[n].discard(self.sdo_name)
                # empty implementations
                self.sdo_bidder.implementations = list()
                # repeat agreement from beginning
                self.sdo_agreement(received_winners, received_bidding_data, sender, local=True)
                # try to repeat bidding on residual resources
                self.sdo_bidder.sdo_bidding()
                # update & rebroadcast
                rebid = True

        '''

        # check if the received message changed the situation
        for node in self.rap.nodes:

            logging.info("Loc winners: " + str(sorted(current_winners[node])))
            logging.info("Rec winners: " + str(sorted(received_winners[node])))
            logging.info("New winners: " + str(sorted(self.sdo_bidder.per_node_winners[node])))

            current_winners_digest = hashlib.sha256(str(sorted(current_winners[node])).encode()).hexdigest()
            rcvd_winners_digest = hashlib.sha256(str(sorted(received_winners[node])).encode()).hexdigest()
            new_winners_digest = hashlib.sha256(
                str(sorted(self.sdo_bidder.per_node_winners[node])).encode()).hexdigest()

            current_node_consumption = {r: sum([current_bidding_data[node][s]["consumption"][r]
                                                for s in self.rap.sdos])
                                        for r in self.rap.resources}
            rcvd_node_consumption = {r: sum([received_bidding_data[node][s]["consumption"][r]
                                             for s in self.rap.sdos])
                                     for r in self.rap.resources}
            new_node_consumption = {r: sum([self.sdo_bidder.bidding_data[node][s]["consumption"][r]
                                            for s in self.rap.sdos])
                                    for r in self.rap.resources}

            # NOTE: in our decision table "UPDATE" means "keep the merge result"

            '''
            elif self._old_bids_win(self.sdo_bidder.bidding_data, self.sdo_bidder.winners):  # some winner has older ts
                if self.sdo_name in self.sdo_bidder.winners:
                    # update-time & rebroadcast
                    logging.info("UPDATE-TIME & REBROADCAST")
                    self._update_time()
                    self.rebroadcast = True
                else:
                    # update & rebroadcast
                    logging.debug"UPDATE & REBROADCAST")
                    self.rebroadcast = True
            '''
            agreement_on_node = False

            if overbid:
                # update & rebroadcast
                logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                self.rebroadcast = True
                self.agreement = False
                self.updated = True
                logging.info("---------------- END AGREEMENT ----------------")
                return  # if sdo rebidded, we already completed the agreement phase for all nodes
            elif sender in current_winners and sender not in received_winners \
                    and self.sdo_name in received_winners and self.sdo_name not in current_winners:
                # i is winner for k and k is winner for i
                # reset & rebroadcast (*?)
                logging.log(LoggingConfiguration.IMPORTANT, "RESET & REBROADCAST")
                self._reset(node)
                self.rebroadcast = True
                self.updated = True
            elif current_winners_digest == rcvd_winners_digest == new_winners_digest:
                logging.info("Current winners are equals to received!")
                if self._compare_bid_times(received_bidding_data[node], current_bidding_data[node]) > 0:
                    # received at least a new bid time
                    if self.rap.check_equals(current_node_consumption, rcvd_node_consumption)\
                            and self.rap.check_equals(current_node_consumption, new_node_consumption):
                        # some new timestamp but no changes in resource assignment
                        agreement_on_node = True
                        self.updated = True
                        logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & NO-REBROADCAST")
                    else:
                        # winners remain the same but there is some change on resource assignment
                        # update & rebroadcast
                        logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                        self.rebroadcast = True
                        self.updated = True
                elif self._compare_bid_times(received_bidding_data[node], current_bidding_data[node]) == 0:
                    # leave & no-rebroadcast
                    agreement_on_node = True
                    logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & NO-REBROADCAST")
                else:
                    # leave & no-rebroadcast
                    agreement_on_node = True
                    logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & NO-REBROADCAST")
                    # if self.sdo_name not in self.sdo_bidder.winners[node]:
                    #     # leave & no-rebroadcast
                    #     logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & NO-REBROADCAST")
                    # else:
                    #     # leave & rebroadcast
                    #     logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & REBROADCAST")
                    #     self.rebroadcast = True
            elif rcvd_winners_digest == new_winners_digest:  # winners are same of received
                # update & rebroadcast
                logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                if self.rap.check_equals(rcvd_node_consumption, current_node_consumption):
                    agreement_on_node = True
                self.rebroadcast = True
                self.updated = True
            elif current_winners_digest == new_winners_digest:  # winners remains the same
                logging.info("New winners are same of current but not received")
                if self.sdo_name in self.sdo_bidder.per_node_winners[node]:
                    if self.sdo_name not in received_winners[node]:
                        # update-time & rebroadcast
                        logging.log(LoggingConfiguration.IMPORTANT, "UPDATE-TIME & REBROADCAST")
                        # self._update_time(node)
                        self.rebroadcast = True
                    else:
                        # update & rebroadcast
                        logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & REBROADCAST")
                        self.rebroadcast = True
                elif self._compare_bid_times(self.sdo_bidder.bidding_data[node], current_bidding_data[node]) > 0:
                    # received new ts
                    # update & no-rebroadcast
                    logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                    self.rebroadcast = True
                else:
                    # leave & rebroadcast
                    logging.log(LoggingConfiguration.IMPORTANT, "LEAVE & REBROADCAST")
                    self.rebroadcast = True
            else:  # new data different
                # update & rebroadcast
                logging.log(LoggingConfiguration.IMPORTANT, "UPDATE & REBROADCAST")
                self.rebroadcast = True
                self.updated = True

            if not agreement_on_node:
                self.agreement = False
            logging.info("Agreement for node: " + str(agreement_on_node))
        # repeat for each node. --)
        logging.info("---------------- END AGREEMENT ----------------")

    @staticmethod
    def _old_bids_win(node_bidding_data, winners):
        """

        :param node_bidding_data: dict[str, dict[str, int]]
        :param winners: list of str
        :return: True if old bids continue to win
        """
        losers_ts = [node_bidding_data[sdo]['timestamp'] for sdo in node_bidding_data
                     if sdo not in winners and node_bidding_data[sdo]['bid'] != 0]
        winners_ts = [node_bidding_data[sdo]['timestamp'] for sdo in node_bidding_data if sdo in winners]
        return max(losers_ts) > min(winners_ts)

    @staticmethod
    def _compare_bid_times(node_bidding_data_1, node_bidding_data_2, sdo=None):
        """

        :param node_bidding_data_1:
        :param node_bidding_data_2:
        :param sdo: if given, compare times just for it
        :type node_bidding_data_1: dict[str, dict[str, int]]
        :type node_bidding_data_2: dict[str, dict[str, int]]
        :return: 1 if first has at least a newer time, 0 if all are equal, -1 otherwise
        """
        equal_flag = True
        for sdo_i in node_bidding_data_1:
            if sdo is not None and sdo_i != sdo:
                continue
            if node_bidding_data_1[sdo_i]['timestamp'] > node_bidding_data_2[sdo_i]['timestamp']:
                return 1
            elif node_bidding_data_1[sdo_i]['timestamp'] < node_bidding_data_2[sdo_i]['timestamp']:
                equal_flag = False
        if equal_flag:
            return 0
        else:
            return -1

    def _update_time(self, node):
        """

        :param node: the node where the bid time should be updated
        """
        self.sdo_bidder.bidding_data[node][self.sdo_name]['timestamp'] = time.time()

    def _reset(self, node):
        """

        :param node: the node where the bid time should be reset
        """
        for sdo in self.sdo_bidder.bidding_data:
            self.sdo_bidder.bidding_data[node][sdo] = self.sdo_bidder.init_bid(time.time())
