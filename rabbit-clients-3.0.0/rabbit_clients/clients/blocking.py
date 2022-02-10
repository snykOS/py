"""
Base classes for Rabbit

"""
# pylint: disable=too-few-public-methods, too-many-arguments
from typing import Any, Dict
import json

import pika
from retry import retry

from rabbit_clients.clients.config import RABBIT_CONFIG


def _create_connection_and_channel() -> pika.BlockingConnection.channel:
    """
    Will run immediately on library import.  Requires that an environment variable
    for RABBIT_URL has been set.

    :return: RabbitMQ Channel
    :rtype: tuple

    """
    credentials = pika.PlainCredentials(RABBIT_CONFIG.RABBITMQ_USER,
                                        RABBIT_CONFIG.RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBIT_CONFIG.RABBITMQ_HOST,
                                  virtual_host=RABBIT_CONFIG.RABBITMQ_VIRTUAL_HOST,
                                  credentials=credentials))
    return connection.channel()


def send_log(channel: Any, method: str, properties: Any, body: str) -> Dict[str, Any]:
    """
    Helper function to send messages to logging queue

    :param channel: Channel from incoming message
    :param method: Method from incoming message
    :param properties: Properties from incoming message
    :param body: JSON from incoming message
    :return: Dictionary representation of message

    """
    return {
        'channel': str(channel),
        'method': str(method),
        'properties': str(properties),
        'body': body
    }


class ConsumeMessage:
    """
    Decorator class that allows users to quickly attach functioning code to a
    RabbitMQ Broker without needing to manage channels, connections, etc.

    """
    def __init__(self, queue: str, exchange: str = '', exchange_type: str = 'direct',
                 logging: bool = True, logging_queue: str = 'logging'):
        self._consume_queue = queue
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._logging = logging
        self._logging_queue = logging_queue

    def __call__(self, func, *args, **kwargs) -> Any:
        @retry(pika.exceptions.AMQPConnectionError, tries=5, delay=5, jitter=(1, 3))
        def prepare_channel():
            """
            Ensure RabbitMQ Connection is open and that you have an open
            channel.  Then provide a callback returns the target function
            but ensures that the incoming message body has been
            converted from JSON to a Python dictionary.

            :param func: The user function being decorated
            :return: An open listener utilizing the user function or
            a one time message receive in the event of parent function
            parameter of production ready being set to False

            """
            # Open RabbitMQ connection if it has closed or is not set
            _channel = _create_connection_and_channel()

            if self._exchange:
                _channel.exchange_declare(exchange=self._exchange,
                                          exchange_type=self._exchange_type)
                declared_queue = _channel.queue_declare(queue=self._consume_queue)
                _channel.queue_bind(exchange=self._exchange, queue=declared_queue.method.queue)
            else:
                _channel.queue_declare(queue=self._consume_queue)

            log_publisher = PublishMessage(queue=self._logging_queue)

            # Callback function for when a message is received
            def message_handler(channel, method, properties, body):

                # Utilize module decorator to send logging messages
                decoded_body = json.loads(body.decode('utf-8'))
                if self._logging:
                    log_publisher(send_log)(channel, method, properties, decoded_body)

                func(decoded_body)

            _channel.basic_consume(queue=self._consume_queue,
                                   on_message_callback=message_handler, auto_ack=True)

            try:
                _channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                pass
            except KeyboardInterrupt:
                _channel.stop_consuming()

        return prepare_channel


class PublishMessage:
    """
    Decorator class that assumes the decorated function will return a Python
    dict to be transmitted as JSON to the RabbitMQ Broker

    """
    def __init__(self, queue: str, exchange: str = '', exchange_type: str = 'direct'):
        self._queue = queue
        self._exchange = exchange
        self._exchange_type = exchange_type

    def __call__(self, func, *args, **kwargs) -> Any:
        @retry(pika.exceptions.AMQPConnectionError, tries=5, delay=5, jitter=(1, 3))
        def wrapper(*args, **kwargs):
            """
            Run the function as expected but the return from the function must
            be a Python dictionary as it will be converted to JSON. Then ensure
            RabbitMQ connection is open and that you have an open channel.  Then
            use a basic_publish method to send the message to the target queue.

            :param args:  Any positional arguments passed to the function
            :param kwargs: Any keyword arguments pass to the function
            :return: None

            """
            # Run the function and get dictionary as result
            result = json.dumps(func(*args, **kwargs))

            # Ensure open connection and channel
            channel = _create_connection_and_channel()

            if self._exchange:
                channel.exchange_declare(exchange=self._exchange, exchange_type=self._exchange_type)
                declared_queue = channel.queue_declare(queue=self._queue)
                channel.queue_bind(exchange=self._exchange, queue=declared_queue.method.queue)
            else:
                # Ensure queue exists
                channel.queue_declare(queue=self._queue)

            # Send message to queue
            channel.basic_publish(
                exchange=self._exchange,
                routing_key=self._queue,
                body=result
            )

        return wrapper
