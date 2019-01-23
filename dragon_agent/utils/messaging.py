import json
import logging
import threading

import pika

from config.logging_configuration import LoggingConfiguration
from dragon_agent.utils.bidding_message import BiddingMessage
from dragon_agent.utils.singleton import Singleton


class Messaging(object, metaclass=Singleton):
    """
    This class manage exchange of messages with the neighborhood through rabbitmq.
    """

    def __init__(self, broker=None):
        """

        :param str broker: address of the broker
        """
        self._broker_host = broker
        self._timeout_id = None
        self._permanent_timeout_id = None
        self._permanent_timeout = None
        self._connection = None
        self._channel = None
        self._write_connection = None
        self._write_channel = None
        self._message_handler = self._default_message_handler

    def connect(self):
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._connection = self._rabbitmq_connect(self._broker_host)
        self._channel = self._connection.channel()
        # self._write_connection = self._rabbitmq_connect(self._broker_host)
        # self._write_channel = self._write_connection.channel()

    def disconnect(self):
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._connection.close()
        # self._write_connection.close()

    def connect_write(self):
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._write_connection = self._rabbitmq_connect(self._broker_host)
        self._write_channel = self._write_connection.channel()

    def disconnect_write(self):
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._write_connection.close()

    @staticmethod
    def _rabbitmq_connect(broker_host):
        """

        :param broker_host:
        :return:
        """
        return pika.BlockingConnection(pika.ConnectionParameters(broker_host))

    def set_stop_timeout(self, timeout, permanent=False):
        """
        Set a timeout to stop consuming messages
        :param int timeout:
        :param bool permanent:
        :return:
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        if permanent:
            self._permanent_timeout_id = self._connection.add_timeout(timeout, self.stop_consuming)
            self._permanent_timeout = timeout
        elif self._timeout_id is None:
            self._timeout_id = self._connection.add_timeout(timeout, self.stop_consuming)

    def del_stop_timeout(self):
        """
        Delete the current timeout
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        if self._timeout_id is not None:
            self._connection.remove_timeout(self._timeout_id)
            self._timeout_id = None

    def _refresh_timeout(self, timeout_id, timeout):
        """
        Reset the permanent timeout
        :param timeout_id:
        :param timeout:
        :return: the new id
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._connection.remove_timeout(timeout_id)
        return self._connection.add_timeout(timeout, self.stop_consuming)

    def send_message(self, dst, message):
        """

        :param str dst: name of the destination is used as queue
        :param BiddingMessage message:
        :return:
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        # self._channel.queue_declare(queue=dst)
        # self._channel.basic_publish(exchange='', routing_key=dst, body=json.dumps(message.to_dict()))
        self._write_channel.queue_declare(queue=dst)
        self._write_channel.basic_publish(exchange='', routing_key=dst, body=json.dumps(message.to_dict()))

    def start_consuming(self):
        """

        :return:
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._channel.start_consuming()

    def stop_consuming(self):
        """

        :return:
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        logging.log(LoggingConfiguration.IMPORTANT, "Timeout!")
        self._channel.stop_consuming()

    def consume(self, topic):
        """

        :return:
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        message = None
        q = self._channel.queue_declare(queue=topic)
        for method, properties, body in self._channel.consume(queue=topic):
            self._channel.basic_ack(method.delivery_tag)
            if q.method.message_count == 0:
                message = BiddingMessage()
                message.parse_dict(json.loads(body.decode()))
                break
        return message

    def register_handler(self, topic, handler=None):
        """

        :param topic:
        :param handler: must be a a callable that takes one parameter of type BiddingMessage
        :return:
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._channel.queue_declare(queue=topic)
        if handler is None:
            handler = self._default_message_handler
        self._message_handler = handler
        self._channel.basic_consume(self._message_callback, queue=topic, no_ack=True)

    def _timeout_handler(self):
        """
        Stops message consuming
        """
        logging.log(LoggingConfiguration.IMPORTANT, threading.get_ident())
        self._channel.stop_consuming()

    @staticmethod
    def _message_callback(channel, method, header, body):
        """

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver method: basic_deliver method
        :param pika.Spec.BasicProperties header: properties
        :param bytes body: The message body
        :return:
        """
        logging.log(15, " [x] Received " + body.decode())
        self = Messaging()
        self._permanent_timeout_id = self._refresh_timeout(self._permanent_timeout_id, self._permanent_timeout)

        message = BiddingMessage()
        message.parse_dict(json.loads(body.decode()))
        self._message_handler(message)

    @staticmethod
    def _default_message_handler(message):
        """

        :param BiddingMessage message:
        """
        print("Received bidding_message from '" + message.sender + "': \n" + str(message))
