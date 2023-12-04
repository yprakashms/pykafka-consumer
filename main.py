import logging
import signal
import sys
import socket
import time

from confluent_kafka.cimpl import Consumer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP

logger = logging.getLogger('main')
last_assignment_time = 0
consumer_poll_timeout = 0.1


def log_assignment(consumer, partitions):
    global last_assignment_time
    if last_assignment_time < int(time.time()) - 600: # log only once in 10 minutes
        last_assignment_time = int(time.time())
        logger.info('Assignment: {}'.format(partitions))


def _create_consumer(consumer_settings, kafka_topic):
    logger.debug('Started consuming messages from {} with custom configs {}'.format(kafka_topic, consumer_settings))
    consumer = Consumer(consumer_settings)
    consumer.subscribe([kafka_topic], on_assign=log_assignment)
    return consumer


def create_consumer(offset='latest'):
    hostname = socket.gethostname()
    host_ipaddr = socket.gethostbyname(hostname)
    logger.info("Current node hostname = %s, ipaddress = %s", hostname, host_ipaddr)

    consumer_settings = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_CONSUMER_GROUP,
        'client.id': host_ipaddr,
        'enable.auto.commit': True,
        'session.timeout.ms': 10000,  # https://stackoverflow.com/a/43992308
        'heartbeat.interval.ms': 3000,  # https://stackoverflow.com/a/55499324
        'max.poll.interval.ms': 60000,  # https://stackoverflow.com/a/39759329
        'default.topic.config': {'auto.offset.reset': offset}
    }
    consumer = _create_consumer(consumer_settings, KAFKA_TOPIC)

    return consumer


def get_kafka_message(consumer):
    msg = consumer.poll(consumer_poll_timeout)
    if msg is not None:
        if not msg.error():
            ts_type, ts = msg.timestamp()
            return msg.topic(), ts, msg.value()

    return None, 0, None


def setup_logging():
    file_handler = logging.FileHandler(filename='logs/pipeline.log')
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    _handlers = [file_handler, stdout_handler]
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s %(asctime)s.%(msecs)05d %(threadName)s %(name)s:%(lineno)d %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=_handlers
    )


def start():
    setup_logging()
    consumer = create_consumer(offset='earliest')
    while True:
        _, _, msg = get_kafka_message(consumer)
        if msg:
            print('RECEIVED MESSAGE -> ', msg)


if __name__ == '__main__':
    start()
