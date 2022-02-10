"""
Configuration module providing config objects

"""
# pylint: disable=too-few-public-methods
import os


class RabbitConfig:
    """
    Configuration class object for RABBITMQ configurations

    """
    RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
    RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
    RABBITMQ_VIRTUAL_HOST = os.getenv('RABBITMQ_VIRTUAL_HOST', '/')
    RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')


RABBIT_CONFIG = RabbitConfig()
