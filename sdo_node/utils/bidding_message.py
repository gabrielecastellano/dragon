import pprint

import time


class BiddingMessage:

    def __init__(self, sender=None, winners=None, bidding_data=None):
        """

        :param str sender:
        :param dict[str, set of str] winners:
        :param dict[str, dict[str, union[int, dict, float]]] bidding_data:
        """
        self.sender = sender
        self.winners = winners
        self.bidding_data = bidding_data
        self.timestamp = time.time()

    def to_dict(self):
        bidding_message_dict = dict()
        bidding_message_dict["sender"] = self.sender
        bidding_message_dict["winners"] = {node: list(self.winners[node]) for node in self.winners}
        bidding_message_dict["bidding_data"] = self.bidding_data
        bidding_message_dict["timestamp"] = self.timestamp
        return bidding_message_dict

    def parse_dict(self, bidding_message_dict):
        self.sender = bidding_message_dict["sender"]
        self.winners = {node: set(bidding_message_dict["winners"][node]) for node in bidding_message_dict["winners"]}
        self.bidding_data = bidding_message_dict["bidding_data"]
        self.timestamp = bidding_message_dict["timestamp"]
