import logging
import pprint
import time
from collections import OrderedDict

from datetime import datetime
from threading import Lock, Thread, Condition

import itertools

from config.configuration import Configuration
from config.logging_configuration import LoggingConfiguration
from sdo_node.agreement.sdo_agreement import SdoAgreement
from sdo_node.bidding.sdo_bidder import SdoBidder
from sdo_node.utils.bidding_message import BiddingMessage
from sdo_node.utils.messaging import Messaging
from sdo_node.utils.neighborhood import NeighborhoodDetector


class SDONode:

    def __init__(self, sdo_name, rap, service_bundle):

        # SDO node
        self.sdo_name = sdo_name
        self.rap = rap
        self.sdo_bidder = SdoBidder(sdo_name, rap, service_bundle)
        self.sdo_agreement = SdoAgreement(sdo_name, rap, self.sdo_bidder)
        self.agree_neighbors = set()

        self.neighborhood_detector = NeighborhoodDetector(sdos=self.rap.sdos,
                                                          base_sdo=self.sdo_name,
                                                          neighbor_probability=Configuration.NEIGHBOR_PROBABILITY,
                                                          max_neighbors_ratio=Configuration.MAX_NEIGHBORS_RATIO,
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

        # validation
        self.sent_count = 0
        self.message_rates = OrderedDict()

    def start_distributed_scheduling(self):

        self.begin_time = time.time()

        # connect
        self._messaging.connect()
        self._messaging.set_stop_timeout(Configuration.WEAK_AGREEMENT_TIMEOUT, permanent=True)

        # first bidding
        self.sdo_bidder.sdo_bidding()
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
        print(self.sdo_name +
              " | strong: " + str(strong_agreement).ljust(5) +
              " | winners: " + str(sorted(self.sdo_bidder.get_winners())) +
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
            message = self.dequeue_next_message()
            if message is not None:
                self.bid_message_handler(message)
        self._messaging.disconnect_write()

    def bid_message_enqueue(self, message):
        """

        :param message:
        :return:
        """
        self.cv.acquire()
        # self.queue_locks[message.sender].aquire()
        self.message_queues[message.sender].append(message)
        # self.queue_locks[message.sender].release()
        self.cv.notify()
        self.cv.release()

    def dequeue_next_message(self):
        """

        :return:
        """
        self.cv.acquire()
        while len(list(itertools.chain(*self.message_queues.values()))) == 0 and self.end_time == 0:
            self.cv.wait()
        if self.end_time != 0:
            return None
        message = None
        for sdo in self.neighborhood:
            # self.queue_locks[sdo].aquire()
            if len(self.message_queues[sdo]) > 0:
                # message = self.message_queues[sdo][0]
                # self.message_queues[sdo] = self.message_queues[sdo][1:]
                message = self.message_queues[sdo][-1]
                self.message_queues[sdo] = list()
            # self.queue_locks[message.sender].release()
            if message is not None:
                break
        self.cv.release()
        return message

    def bid_message_handler(self, message):
        """

        :param message:
        :type message: BiddingMessage
        :return:
        """

        self.received_messages += 1
        self.last_message_time = time.time()
        self.last_seen[message.sender] = message.timestamp

        # [ agreement process for this message ]
        logging.log(LoggingConfiguration.IMPORTANT, "Handling message from '" + message.sender + "'" +
                    " - ts: " + datetime.fromtimestamp(message.timestamp).strftime('%d/%m/%Y %H:%M:%S.%f')[:-3])
        self.sdo_agreement.sdo_agreement(message.winners, message.bidding_data, message.sender)

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
