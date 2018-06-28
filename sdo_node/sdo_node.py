import logging
import pprint
import time
from collections import OrderedDict

from datetime import datetime
from threading import Lock, Thread, Condition

from config.configuration import Configuration
from config.logging_configuration import LoggingConfiguration
from sdo_node.agreement.sdo_agreement import SdoAgreement
from sdo_node.orchestration.sdo_orchestrator import SdoOrchestrator
from sdo_node.utils.bidding_message import BiddingMessage
from sdo_node.utils.messaging import Messaging
from sdo_node.utils.neighborhood import NeighborhoodDetector


class SDONode:

    def __init__(self, sdo_name, rap, service_bundle):

        # SDO node
        self.sdo_name = sdo_name
        self.rap = rap
        self.sdo_bidder = SdoOrchestrator(sdo_name, rap, service_bundle)
        self.sdo_agreement = SdoAgreement(sdo_name, rap, self.sdo_bidder)
        self.agree_neighbors = set()

        self.neighborhood_detector = NeighborhoodDetector(sdos=self.rap.sdos,
                                                          base_sdo=self.sdo_name,
                                                          load_neighborhood=Configuration.LOAD_TOPOLOGY,
                                                          neighbor_probability=Configuration.NEIGHBOR_PROBABILITY,
                                                          topology_file=Configuration.TOPOLOGY_FILE,
                                                          stable_connections=Configuration.STABLE_CONNECTIONS)
        self.neighborhood = self.neighborhood_detector.get_neighborhood()

        # init messaging
        self._messaging = Messaging("localhost")

        # message counters
        self.message_counter = 0
        self.received_messages = 0

        # times
        self.begin_time = 0
        self.last_update_time = 0
        self.agreement_time = 0
        self.last_message_time = 0
        self.end_time = 0
        self.last_seen = {sdo: 0 for sdo in self.neighborhood}

        # messages queues
        self.message_queues = {sdo: list() for sdo in self.neighborhood}
        self.queue_locks = {sdo: Lock() for sdo in self.neighborhood}
        self.cv = Condition()
        self.pending_rebroadcast = False

        # validation
        self.sent_count = 0
        self.message_rates = OrderedDict()

    def start_distributed_scheduling(self):

        self.begin_time = time.time()

        # connect
        self._messaging.connect()
        self._messaging.set_stop_timeout(Configuration.WEAK_AGREEMENT_TIMEOUT, permanent=True)

        # first bidding
        self.sdo_bidder.sdo_orchestrate()
        logging.info(pprint.pformat(self.sdo_bidder.bidding_data))

        # broadcast first bidding data
        # self.broadcast()

        # start to receive messages
        logging.info("Subscribing to handle messages with topic '" + self.sdo_bidder.sdo_name + "' ...")
        # self._messaging.register_handler(self.sdo_bidder.sdo_name, self.bid_message_handler)
        self._messaging.register_handler(self.sdo_bidder.sdo_name, self.bid_message_enqueue)
        # start to handle messages
        thread = Thread(target=self.consumer)
        thread.start()
        logging.info("Listening for incoming messages ...")
        self._messaging.start_consuming()

        # agreement completed
        logging.log(LoggingConfiguration.IMPORTANT, "waiting for handler thread ...")
        self.cv.acquire()
        self.end_time = time.time()
        self.cv.notify()
        self.cv.release()
        thread.join()
        strong_agreement = len(self.agree_neighbors) == len(self.neighborhood)
        logging.log(LoggingConfiguration.IMPORTANT, "Agreement process reached convergence! " +
                                                    "(strong=" + str(strong_agreement) + ")")
        if self.sdo_name in self.sdo_bidder.get_winners():
            logging.info(" - Sdo '" + self.sdo_name + " got enough resources to implement bundle! :-)")
            logging.info(" - Assigned functions are: \n" + pprint.pformat(self.sdo_bidder.implementations))
        else:
            logging.info(" - Sdo '" + self.sdo_name + " didn't get enough resources to implement bundle :-(")
        self.sdo_bidder.private_utility = self.sdo_bidder.get_service_utility()[0]
        print(self.sdo_name.ljust(5) +
              " | strong: " + str(strong_agreement).ljust(5) +
              " | winners: " + str(sorted(self.sdo_bidder.get_winners())) +
              " | B: " + str(int(self.sdo_bidder.sum_bids())) +
              " | u: " + str(self.sdo_bidder.private_utility).rjust(3) +
              " | last update on: " + str(self.last_update_time - self.begin_time)[:5] +
              " | agreement on: " + str(self.agreement_time - self.begin_time)[:5] +
              " | last message on: " + str(self.last_message_time - self.begin_time)[:5] +
              " | total time: " + str(self.end_time - self.begin_time)[:5] +
              " | sent messages: " + str(self.message_counter).rjust(7) +
              " | received messages: " + str(self.received_messages).rjust(7))

        # disconnect
        self._messaging.disconnect()
        return strong_agreement, self.sdo_bidder.implementations, self.message_rates

    def consumer(self):
        self._messaging.connect_write()
        self.broadcast()
        while self.end_time == 0:
            messages = self.dequeue_next_messages()
            if len(messages) == 0:
                continue
            self.bid_messages_handler(messages)
            # for i, message in enumerate(messages):
            #     is_last = i == len(messages) - 1
            #     self.bid_message_handler(message, last=is_last)
        self._messaging.disconnect_write()

    def bid_message_enqueue(self, message):
        """

        :param message:
        :return:
        """
        self.last_message_time = time.time()
        self.cv.acquire()
        # self.queue_locks[message.sender].acquire()
        self.message_queues[message.sender].append(message)
        # self.queue_locks[message.sender].release()
        self.cv.notify_all()
        self.cv.release()

    def dequeue_next_messages(self):
        """

        :return:
        """
        self.cv.acquire()

        '''
        while len(list(itertools.chain(*self.message_queues.values()))) == 0 and self.end_time == 0:
            self.cv.wait()
        '''

        # '''
        timeout = float(Configuration.ASYNC_TIMEOUT)
        while timeout > 0 \
                and len([q for q in self.message_queues if q not in self.agree_neighbors and len(self.message_queues[q]) == 0]) > 0 \
                and self.end_time == 0:
            start_t = time.time()
            self.cv.wait(timeout)
            end_t = time.time()
            timeout -= end_t-start_t
            # if self.sdo_name == 'sdo0':
            #    print("timeout: " + str(timeout))
        # '''

        if self.end_time != 0:
            self.cv.release()
            return list()

        messages = list()
        for sdo in self.neighborhood:
            # self.queue_locks[sdo].acquire()
            if len(self.message_queues[sdo]) > 0:
                # message = self.message_queues[sdo][0]
                # self.message_queues[sdo] = self.message_queues[sdo][1:]
                # consider just the last message for each neighbor
                messages.append(self.message_queues[sdo][-1])
                self.message_queues[sdo] = list()
                #
                # break
            # self.queue_locks[message.sender].release()
        self.cv.release()
        return messages

    def bid_messages_handler(self, messages):
        """
        This one calls an agreement function that merge data at once
        :param messages:
        :type messages: list of BiddingMessage
        :return:
        """

        self.received_messages += len(messages)
        for message in messages:
            self.last_seen[message.sender] = message.timestamp

        # [ agreement process for these messages ]
        logging.log(LoggingConfiguration.IMPORTANT, "Handling messages from '" + ",".join([m.sender for m in messages]) + "'")
        data = {m.sender: {'bidding-data': m.bidding_data, 'winners': m.winners} for m in messages}
        self.sdo_agreement.sdo_multi_agreement(data)

        # [ rebroadcast ]
        if self.sdo_agreement.rebroadcast:
            self.broadcast()
        else:
            logging.info("No need to rebroadcast bidding information.")

        # [ agreement check ]
        if self.sdo_agreement.updated:
            self.last_update_time = time.time()
            if len(self.agree_neighbors) == len(self.neighborhood):
                # old agreement has been broken
                logging.log(LoggingConfiguration.IMPORTANT, "Previous agreement has been broken.")
                self.agree_neighbors = set()
                self.agreement_time = 0
                # delete timeout if any
                self._messaging.del_stop_timeout()
        for message in messages:
            if message.sender in self.sdo_agreement.per_sdo_agreement:
                # NEIGHBOR AGREEMENT - data that neighbor sent are consistent with local
                logging.log(LoggingConfiguration.IMPORTANT, "Agreement reached with neighbor '" + message.sender + "'")
                prev_len = len(self.agree_neighbors)
                self.agree_neighbors.add(message.sender)
                if len(self.agree_neighbors) == len(self.neighborhood):
                    # NEIGHBORHOOD AGREEMENT - data that all neighbor sent are consistent with local
                    if prev_len < len(self.neighborhood):
                        # this agreement is new
                        logging.log(LoggingConfiguration.IMPORTANT, "=================================================")
                        logging.log(LoggingConfiguration.IMPORTANT, "AGREEMENT REACHED WITH '" + self.sdo_name + "'!!!")
                        logging.log(LoggingConfiguration.IMPORTANT, "=================================================")
                        self.agreement_time = time.time()
                        # set timeout to stop wait messages if nothing new arrives
                        logging.log(LoggingConfiguration.IMPORTANT, " - Waiting " +
                                    str(Configuration.AGREEMENT_TIMEOUT) +
                                    " seconds for new messages before stop agreement ...")
                        self._messaging.set_stop_timeout(Configuration.AGREEMENT_TIMEOUT)
                    else:
                        logging.info("Confirmed last agreement")

    def bid_message_handler(self, message, last=True):
        """
        This one calls an agreement function that merge data received from a single sender
        :param message:
        :param last: if True, manages rebid and rebroadcast
        :type message: BiddingMessage
        :type last: bool
        :return:
        """

        logging.log(LoggingConfiguration.IMPORTANT, "last: " + str(last))

        self.received_messages += 1
        self.last_seen[message.sender] = message.timestamp

        # [ agreement process for this message ]
        logging.log(LoggingConfiguration.IMPORTANT, "Handling message from '" + message.sender + "'" +
                    " - ts: " + datetime.fromtimestamp(message.timestamp).strftime('%d/%m/%Y %H:%M:%S.%f')[:-3])
        self.sdo_agreement.sdo_agreement(message.winners, message.bidding_data, message.sender, rebid_enabled=last)

        # [ rebroadcast ]
        if self.sdo_agreement.rebroadcast or self.pending_rebroadcast:
            if not last:
                self.pending_rebroadcast = True
            else:
                self.broadcast()
                self.pending_rebroadcast = False
        else:
            logging.info("No need to rebroadcast bidding information.")

        # [ agreement check ]
        if self.sdo_agreement.updated:
            self.last_update_time = time.time()
            if len(self.agree_neighbors) == len(self.neighborhood):
                # old agreement has been broken
                logging.log(LoggingConfiguration.IMPORTANT, "Previous agreement has been broken.")
                self.agree_neighbors = set()
                self.agreement_time = 0
                # delete timeout if any
                self._messaging.del_stop_timeout()
        if self.sdo_agreement.agreement:
            # NEIGHBOR AGREEMENT - data that neighbor sent are consistent with local
            logging.log(LoggingConfiguration.IMPORTANT, "Agreement reached with neighbor '" + message.sender + "'")
            prev_len = len(self.agree_neighbors)
            self.agree_neighbors.add(message.sender)
            if len(self.agree_neighbors) == len(self.neighborhood):
                # NEIGHBORHOOD AGREEMENT - data that all neighbor sent are consistent with local
                if prev_len < len(self.neighborhood):
                    # this agreement is new
                    logging.log(LoggingConfiguration.IMPORTANT, "====================================================")
                    logging.log(LoggingConfiguration.IMPORTANT, "Sdo '" + self.sdo_name + "' HAS REACHED AGREEMENT!!!")
                    logging.log(LoggingConfiguration.IMPORTANT, "====================================================")
                    self.agreement_time = time.time()
                    # set timeout to stop wait messages if nothing new arrives
                    logging.log(LoggingConfiguration.IMPORTANT, " - Waiting " + str(Configuration.AGREEMENT_TIMEOUT) +
                                                                " seconds for new messages before stop agreement ...")
                    self._messaging.set_stop_timeout(Configuration.AGREEMENT_TIMEOUT)
                else:
                    logging.info("Confirmed last agreement")

    def broadcast(self):
        """

        :return:
        """
        logging.info("Broadcasting bidding information ...")

        # build the message to broadcast
        message_to_broadcast = BiddingMessage(sender=self.sdo_name,
                                              winners=self.sdo_bidder.per_node_winners,
                                              bidding_data=self.sdo_bidder.bidding_data)

        # get the neighbors list
        neighborhood = self.neighborhood_detector.get_current_neighborhood()

        # time.sleep(0.06)

        for neighbor in neighborhood:
            logging.info("Sending message to neighbor '" + neighbor + "' ...")
            self.send_bid_message(neighbor, message_to_broadcast)
            logging.info("Message has been sent.")
            self.message_counter += 1

        # store rate for validation
        timestamp = time.time()
        sent_time = float("{0:.3f}".format(timestamp))
        if len(self.message_rates) == 0:
            self.last_time = float("{0:.3f}".format(self.begin_time))
        else:
            last_begin_time = float(next(reversed(self.message_rates)).split(":")[0])
            if last_begin_time == self.last_time:
                del self.message_rates[next(reversed(self.message_rates))]

        if sent_time - self.last_time > Configuration.SAMPLE_FREQUENCY:
            self.message_rates[str(self.last_time) + ":" + str(sent_time)] = self.message_counter - self.sent_count
            self.sent_count = self.message_counter
            self.last_time = sent_time
        else:
            self.message_rates[str(self.last_time) + ":" + str(sent_time)] = self.message_counter - self.sent_count

        logging.info("broadcast successfully completed.")

    def send_bid_message(self, dst_sdo, message):
        """

        :param dst_sdo:
        :param message:
        :type dst_sdo: str
        :type message: BiddingMessage
        :return:
        """
        self._messaging.send_message(dst_sdo, message)
