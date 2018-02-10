import pika

from config.configuration import Configuration


def delete_queues(queues):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    for sdo in queues:
        channel.queue_declare(queue=sdo)
        channel.queue_purge(sdo)
        channel.queue_delete(queue=sdo)
    connection.close()


if __name__ == "__main__":
    sdos = ["sdo" + str(n) for n in range(Configuration.SDO_NUMBER)]
    delete_queues(sdos)
